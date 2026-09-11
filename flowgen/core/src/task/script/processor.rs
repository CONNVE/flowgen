//! Script-based event transformation processor.
//!
//! Executes Rhai scripts to transform, filter, or manipulate event data.
//! Scripts can return objects, arrays, or null to control event emission.

use crate::event::{Event, EventBuilder, EventData, EventExt};
use chrono::{Datelike, Timelike};
use rhai::{Dynamic, Engine, Scope};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during script execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: crate::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: crate::event::Error,
    },
    #[error("Script execution error: {source}")]
    ScriptExecution {
        #[source]
        source: Box<rhai::EvalAltResult>,
    },
    #[error("Event conversion error: {source}")]
    EventConversion {
        #[source]
        source: crate::event::Error,
    },
    #[error("Script returned invalid type: {0}")]
    InvalidReturnType(String),
    #[error("Error serializing event to JSON: {source}")]
    JsonSerialization {
        #[source]
        source: serde_json::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Error parsing RFC 2822 timestamp '{timestamp}': {source}")]
    ParseRFC2822Timestamp {
        timestamp: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Error parsing RFC 3339 timestamp '{timestamp}': {source}")]
    ParseRFC3339Timestamp {
        timestamp: String,
        #[source]
        source: chrono::ParseError,
    },
    #[error("Invalid Unix timestamp '{timestamp}': timestamp out of valid range")]
    InvalidUnixTimestamp { timestamp: i64 },
    #[error("Timestamp must be an integer, got type: {type_name}")]
    TimestampTypeError { type_name: String },
    #[error("Error loading script resource: {source}")]
    ResourceLoad {
        #[source]
        source: crate::resource::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
}

/// Handles individual script execution operations.
pub struct EventHandler {
    /// Resolved script code (inline or loaded from resource file).
    code: String,
    /// Channel sender for processed events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Rhai script engine instance.
    engine: Engine,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task context for cache access.
    task_context: Arc<crate::task::context::TaskContext>,
}

/// Wrapper for cache access from Rhai scripts.
///
/// This type is registered with the Rhai engine to enable scripts to interact
/// with the distributed cache via `ctx.cache` methods. The wrapper is necessary
/// because Rhai cannot directly call async Rust functions, so we use
/// `tokio::runtime::Handle::current().block_on()` within the registered methods
/// to bridge the sync/async boundary. This is safe because each script execution
/// runs in its own spawned tokio task.
///
/// Cache keys are automatically namespaced by flow name to prevent collisions
/// between different flows using the same cache instance.
#[derive(Clone)]
struct CacheHandle {
    /// The distributed cache backend (NATS KV, Redis, or in-memory).
    cache: Arc<dyn crate::cache::Cache>,
    /// Flow name used for automatic key namespacing.
    flow_name: String,
}

/// Wrapper for resource loading from Rhai scripts.
///
/// This type is registered with the Rhai engine to enable scripts to load
/// static resource files via `ctx.resource.get(key)`. Like `CacheHandle`,
/// it bridges Rhai's synchronous execution with async file I/O using
/// `tokio::task::block_in_place()`.
#[derive(Clone)]
struct ResourceHandle {
    /// The resource loader for resolving file paths.
    resource_loader: crate::resource::ResourceLoader,
}

impl EventHandler {
    /// Processes an event by executing the script on its data.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();
        crate::event::with_event_context(&Arc::clone(&event), async move {
            let original_event = Arc::clone(&event);

            // Convert the event to JSON string and parse with Rhai's native JSON parser.
            // This avoids serde_json internal representation leaking into Rhai.
            let value = Value::try_from(event.as_ref())
                .map_err(|source| Error::EventConversion { source })?;
            let event_obj = value["event"].to_owned();
            let event_json = serde_json::to_string(&event_obj)
                .map_err(|source| Error::JsonSerialization { source })?;

            // Parse JSON using Rhai's engine to get native Rhai types.
            let event_dynamic: Dynamic = self
                .engine
                .parse_json(event_json, true)
                .map_err(|e| Error::ScriptExecution { source: e })?
                .into();

            // Create context object for script access to runtime capabilities.
            // The ctx object exposes cache operations via ctx.cache.get/put/delete.
            // and event metadata via ctx.meta, allowing scripts to manage state.
            // and persist metadata changes through the event chain.
            let mut ctx_map = rhai::Map::new();

            // Scripts get a cache handle namespaced by the key-safe flow id.
            let cache_handle = CacheHandle {
                cache: Arc::clone(&self.task_context.cache),
                flow_name: self.task_context.flow.id(),
            };
            ctx_map.insert("cache".into(), Dynamic::from(cache_handle));

            // Add resource handle to enable loading static files from scripts.
            // Scripts can load mounted configmap files via ctx.resource.get("path/to/file.txt").
            if let Some(loader) = &self.task_context.resource_loader {
                let resource_handle = ResourceHandle {
                    resource_loader: loader.clone(),
                };
                ctx_map.insert("resource".into(), Dynamic::from(resource_handle));
            }

            // Add event metadata to allow scripts to read and modify it.
            // Scripts can modify ctx.meta and changes will be preserved in the output event.
            let meta_dynamic = if let Some(meta) = &original_event.meta {
                let meta_json = serde_json::to_string(meta)
                    .map_err(|source| Error::JsonSerialization { source })?;
                self.engine
                    .parse_json(meta_json, true)
                    .map_err(|e| Error::ScriptExecution { source: e })?
                    .into()
            } else {
                Dynamic::from(rhai::Map::new())
            };
            ctx_map.insert("meta".into(), meta_dynamic);

            // Execute the script with the parsed event and ctx in scope.
            let mut scope = Scope::new();
            scope.push("event", event_dynamic);
            scope.push("ctx", Dynamic::from(ctx_map));

            let result: Dynamic = self
                .engine
                .eval_with_scope(&mut scope, &self.code)
                .map_err(|e| Error::ScriptExecution { source: e })?;

            // Extract ctx from scope after script execution.
            let ctx_dynamic = scope.get_value::<Dynamic>("ctx");

            // Extract ctx.meta from the ctx map to capture any modifications made by the script.
            // Scripts can modify metadata (e.g., ctx.meta.processed = true) and those changes are preserved.
            // will be preserved in the output event, maintaining state through the event chain.
            let meta_from_ctx = ctx_dynamic.and_then(|ctx| {
                ctx.try_cast::<rhai::Map>()
                    .and_then(|map| map.get("meta").cloned())
            });

            // Convert the script result back to JSON.
            let result_json = dynamic_to_json(result)?;

            // Convert ctx.meta to JSON for event generation.
            let meta_json = meta_from_ctx.and_then(|m| dynamic_to_json(m).ok());

            // Process the script result based on its type.
            match result_json {
                Value::Null => {
                    // A script returning null filters out the event, so the
                    // pipeline ends here. The upstream completion channel was
                    // sized for every leaf reachable from this script's
                    // subtree, so emit one signal per leaf to release the
                    // source instead of leaving it waiting for completions
                    // that will never arrive.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        let upstream_leaf_share = self.task_context.leaf_count.max(1);
                        for _ in 0..upstream_leaf_share {
                            arc.signal_completion(None);
                        }
                    }
                    Ok(())
                }
                Value::Array(arr) => {
                    // An empty array emits no downstream events. Release the
                    // source by signalling once per leaf reachable from this
                    // script's subtree, otherwise the source waits forever
                    // for completions that will never come.
                    if arr.is_empty() {
                        if let Some(arc) = completion_tx_arc.as_ref() {
                            let upstream_leaf_share = self.task_context.leaf_count.max(1);
                            for _ in 0..upstream_leaf_share {
                                arc.signal_completion(None);
                            }
                        }
                        return Ok(());
                    }

                    // Emit multiple events, attach completion_tx to last one.
                    let arr_len = arr.len();
                    for (idx, value) in arr.into_iter().enumerate() {
                        let mut new_event =
                            self.generate_script_event(value, &original_event, meta_json.as_ref())?;

                        // Attach completion_tx to the last event only.
                        if idx == arr_len - 1 {
                            match self.tx {
                                None => {
                                    // Leaf task: signal completion with the
                                    // last element's payload.
                                    if let Some(arc) = completion_tx_arc.as_ref() {
                                        arc.signal_completion(new_event.data_as_json().ok());
                                    }
                                }
                                Some(_) => {
                                    // Pass through completion_tx to last element.
                                    new_event.completion_tx = completion_tx_arc.clone();
                                }
                            }
                        }

                        self.emit_event(new_event).await?;
                    }
                    Ok(())
                }
                value => {
                    // Emit a single event with completion_tx attached.
                    let mut new_event =
                        self.generate_script_event(value, &original_event, meta_json.as_ref())?;

                    // Signal completion or pass through to single event.
                    match self.tx {
                        None => {
                            // Leaf task: signal completion with the event payload.
                            if let Some(arc) = completion_tx_arc.as_ref() {
                                arc.signal_completion(new_event.data_as_json().ok());
                            }
                        }
                        Some(_) => {
                            // Pass through completion_tx to event.
                            new_event.completion_tx = completion_tx_arc.clone();
                        }
                    }

                    self.emit_event(new_event).await
                }
            }
        })
        .await
    }

    /// Generates a new event from the script result by comparing with the original event.
    ///
    /// Preserves the original data format (Avro, Arrow, or JSON) when the script has not
    /// modified the data content. This allows scripts to modify metadata fields like subject
    /// or id while maintaining efficient binary formats through the pipeline.
    ///
    /// The `ctx_meta` parameter contains metadata extracted from `ctx.meta` after script
    /// execution, allowing scripts to persist metadata changes through the event chain.
    fn generate_script_event(
        &self,
        result: Value,
        original_event: &Event,
        ctx_meta: Option<&Value>,
    ) -> Result<Event, Error> {
        // Convert the original event data to JSON for comparison.
        let original_data_json = original_event
            .data_as_json()
            .map_err(|source| Error::EventConversion { source })?;

        let (subject, data, id) = match result {
            Value::Object(ref obj) => {
                // Script returned an object, which may contain subject, data, or id.
                let subject = obj
                    .get("subject")
                    .and_then(|s| s.as_str())
                    .map(|s| s.to_string())
                    .unwrap_or_else(|| original_event.subject.clone());

                let data_json = obj.get("data").unwrap_or(&result);

                // Keep the original data format if the content has not changed.
                let data = if data_json == &original_data_json {
                    original_event.data.clone()
                } else if obj.contains_key("data") {
                    EventData::Json(data_json.clone())
                } else {
                    // No explicit data key, treat the whole object as data.
                    EventData::Json(result.clone())
                };

                let id = obj
                    .get("id")
                    .and_then(|v| v.as_str())
                    .map(|s| s.to_string())
                    .or_else(|| original_event.id.clone());

                (subject, data, id)
            }
            value => {
                // Script returned a non-object value, treat it as data.
                // Keep the original data format if the content has not changed.
                let data = if value == original_data_json {
                    original_event.data.clone()
                } else {
                    EventData::Json(value)
                };

                (
                    original_event.subject.clone(),
                    data,
                    original_event.id.clone(),
                )
            }
        };

        // Use ctx.meta from scope if provided, otherwise preserve original metadata.
        // This allows scripts to modify metadata that persists through the event chain.
        let meta = match ctx_meta {
            Some(Value::Object(m)) => Some(m.clone()),
            Some(Value::Null) => None,
            None => original_event.meta.clone(),
            _ => original_event.meta.clone(), // Invalid type, preserve original.
        };

        // Build the new event with the processed fields.
        let mut builder = EventBuilder::new()
            .data(data)
            .subject(subject)
            .task_id(self.task_id)
            .task_type(self.task_type);

        if let Some(id) = id {
            builder = builder.id(id);
        }

        if let Some(meta) = meta {
            builder = builder.meta(meta);
        }

        builder
            .build()
            .map_err(|source| Error::EventBuilder { source })
    }

    /// Emits a single event to the broadcast channel.
    async fn emit_event(&self, event: Event) -> Result<(), Error> {
        event
            .send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })?;
        Ok(())
    }
}

/// Converts rhai::Dynamic to serde_json::Value.
fn dynamic_to_json(dynamic: Dynamic) -> Result<Value, Error> {
    let value =
        rhai::serde::from_dynamic(&dynamic).map_err(|e| Error::InvalidReturnType(e.to_string()))?;
    Ok(value)
}

/// Script processor that executes Rhai code on events.
#[derive(Debug)]
pub struct Processor {
    /// Script task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for transformed events.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming events to transform.
    rx: Receiver<Event>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by setting up the Rhai engine and resolving script source.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        // Resolve script code from inline or resource source.
        let script_code = self
            .config
            .code
            .resolve(self.task_context.resource_loader.as_ref())
            .await
            .map_err(|source| Error::ResourceLoad { source })?;

        let mut engine = Engine::new();

        // Bound script CPU and memory so a runaway or hostile script
        // cannot stall the worker. Limits come from the task config and
        // are individually overridable in flow YAML.
        let limits = &self.config.limits;
        engine.set_max_operations(limits.max_operations);
        engine.set_max_call_levels(limits.max_call_depth);
        engine.set_max_string_size(limits.max_string_size);
        engine.set_max_array_size(limits.max_array_size);
        engine.set_max_map_size(limits.max_map_size);
        engine.set_max_expr_depths(limits.max_expr_depth, limits.max_function_expr_depth);

        // Route Rhai print() and debug() through tracing so output inherits
        // the current span context (flow, task, task_id, task_type).
        engine.on_print(|msg| {
            tracing::info!(target: "rhai::script", "{msg}");
        });
        engine.on_debug(|msg, source, pos| {
            let src = source.unwrap_or("script");
            tracing::debug!(target: "rhai::script", source = src, line = pos.line().unwrap_or(0), "{msg}");
        });

        register_helpers(&mut engine);
        // Register cache methods on CacheHandle type to enable distributed caching from Rhai scripts.
        // These methods bridge Rhai's synchronous execution model with Rust's async cache operations.
        // by using block_on within each spawned script task.
        engine.register_type_with_name::<CacheHandle>("CacheHandle");

        // Register ctx.cache.get(key) -> Dynamic.
        // Retrieves a value from the distributed cache. Returns () if the key does not exist,
        // or the string value if found. This allows scripts to check for missing keys
        // using `if ctx.cache.get(key) != ()`.
        // Keys are automatically namespaced by flow name.
        engine.register_fn(
            "get",
            |handle: &mut CacheHandle, key: &str| -> rhai::Dynamic {
                let namespaced_key = format!("{}.{}", handle.flow_name, key);
                let cache = handle.cache.clone();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        match cache.get(&namespaced_key).await {
                            Ok(Some(bytes)) => {
                                let s = String::from_utf8(bytes.to_vec()).unwrap_or_else(|e| {
                                    String::from_utf8_lossy(e.as_bytes()).to_string()
                                });
                                rhai::Dynamic::from(s)
                            }
                            _ => rhai::Dynamic::UNIT,
                        }
                    })
                })
            },
        );

        // Register ctx.cache.put(key, value, ttl_seconds) -> bool.
        // Stores a value in the distributed cache with a time-to-live in seconds.
        // Returns true if the operation succeeded, false otherwise.
        // Keys are automatically namespaced by flow name.
        engine.register_fn(
            "put",
            |handle: &mut CacheHandle, key: &str, value: i64, ttl_secs: i64| -> bool {
                let namespaced_key = format!("{}.{}", handle.flow_name, key);
                let cache = handle.cache.clone();
                let value_str = value.to_string();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let bytes = bytes::Bytes::from(value_str);
                        cache
                            .put(&namespaced_key, bytes, Some(ttl_secs as u64))
                            .await
                            .is_ok()
                    })
                })
            },
        );

        // Register ctx.cache.put(key, value, ttl_seconds) -> bool for string values.
        // Stores a string value in the distributed cache with a time-to-live in seconds.
        // Returns true if the operation succeeded, false otherwise.
        // Keys are automatically namespaced by flow name.
        engine.register_fn(
            "put",
            |handle: &mut CacheHandle, key: &str, value: &str, ttl_secs: i64| -> bool {
                let namespaced_key = format!("{}.{}", handle.flow_name, key);
                let cache = handle.cache.clone();
                let value_owned = value.to_string();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let bytes = bytes::Bytes::from(value_owned);
                        cache
                            .put(&namespaced_key, bytes, Some(ttl_secs as u64))
                            .await
                            .is_ok()
                    })
                })
            },
        );

        // Register ctx.cache.put(key, value) -> bool for integer values without TTL.
        // Stores a value in the distributed cache with no expiration.
        // Returns true if the operation succeeded, false otherwise.
        // Keys are automatically namespaced by flow name.
        engine.register_fn(
            "put",
            |handle: &mut CacheHandle, key: &str, value: i64| -> bool {
                let namespaced_key = format!("{}.{}", handle.flow_name, key);
                let cache = handle.cache.clone();
                let value_str = value.to_string();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let bytes = bytes::Bytes::from(value_str);
                        cache.put(&namespaced_key, bytes, None).await.is_ok()
                    })
                })
            },
        );

        // Register ctx.cache.put(key, value) -> bool for string values without TTL.
        // Stores a string value in the distributed cache with no expiration.
        // Returns true if the operation succeeded, false otherwise.
        // Keys are automatically namespaced by flow name.
        engine.register_fn(
            "put",
            |handle: &mut CacheHandle, key: &str, value: &str| -> bool {
                let namespaced_key = format!("{}.{}", handle.flow_name, key);
                let cache = handle.cache.clone();
                let value_owned = value.to_string();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        let bytes = bytes::Bytes::from(value_owned);
                        cache.put(&namespaced_key, bytes, None).await.is_ok()
                    })
                })
            },
        );

        // Register ctx.cache.delete(key) -> bool.
        // Removes a value from the distributed cache.
        // Returns true if the operation succeeded, false otherwise.
        // Keys are automatically namespaced by flow name.
        engine.register_fn("delete", |handle: &mut CacheHandle, key: &str| -> bool {
            let namespaced_key = format!("{}.{}", handle.flow_name, key);
            let cache = handle.cache.clone();
            tokio::task::block_in_place(move || {
                tokio::runtime::Handle::current()
                    .block_on(async move { cache.delete(&namespaced_key).await.is_ok() })
            })
        });

        // Register ctx.cache.list_keys(prefix) -> Array of strings.
        // Lists every key under the given prefix. The flow-name namespace is
        // applied to the prefix and stripped from each returned key so scripts
        // see the same key shape they used in `put`/`get`/`delete`.
        // Returns an empty array on cache errors so scripts can iterate safely.
        engine.register_fn(
            "list_keys",
            |handle: &mut CacheHandle, prefix: &str| -> rhai::Array {
                let namespace = format!("{}.", handle.flow_name);
                let namespaced_prefix = format!("{namespace}{prefix}");
                let cache = handle.cache.clone();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        match cache.list_keys(&namespaced_prefix).await {
                            Ok(keys) => keys
                                .into_iter()
                                .map(|k| {
                                    // Strip the flow-name namespace prefix so callers
                                    // see the same key they passed to put/get.
                                    let stripped =
                                        k.strip_prefix(&namespace).unwrap_or(&k).to_string();
                                    rhai::Dynamic::from(stripped)
                                })
                                .collect(),
                            Err(_) => rhai::Array::new(),
                        }
                    })
                })
            },
        );

        // Register resource methods on ResourceHandle type to enable loading static files from Rhai scripts.
        // This allows scripts to access mounted configmap files via ctx.resource.get("path/to/file.txt").
        engine.register_type_with_name::<ResourceHandle>("ResourceHandle");

        // Register ctx.resource.get(key) -> Dynamic.
        // Loads a resource file by key and returns its content as a string.
        // Returns () if the resource cannot be loaded, allowing scripts to check
        // with `if ctx.resource.get("path") != ()`.
        engine.register_fn(
            "get",
            |handle: &mut ResourceHandle, key: &str| -> rhai::Dynamic {
                let loader = handle.resource_loader.clone();
                let key = key.to_string();
                tokio::task::block_in_place(move || {
                    tokio::runtime::Handle::current().block_on(async move {
                        match loader.load(&key).await {
                            Ok(content) => rhai::Dynamic::from(content),
                            Err(_) => rhai::Dynamic::UNIT,
                        }
                    })
                })
            },
        );

        // Register render(template, data) -> String.
        // Renders a Handlebars template string against a data map.
        // Supports {{field}} and {{nested.field}} syntax.
        // Process environment variables are intentionally NOT exposed here:
        // scripts may be authored by anyone with commit access to a synced
        // repository, and surfacing pod env vars would let them exfiltrate
        // secrets. Operator-controlled config rendering retains env access.
        engine.register_fn(
            "render",
            |template: &str, data: Dynamic| -> Result<String, Box<rhai::EvalAltResult>> {
                let json_value: Value =
                    rhai::serde::from_dynamic(&data).map_err(|e| e.to_string())?;
                crate::config::render_template_no_env(template, &json_value)
                    .map_err(|e| e.to_string().into())
            },
        );

        // Override Rhai's built-in parse_json which only supports objects at the root.
        // This version uses serde to handle both objects and arrays.
        engine.register_fn(
            "parse_json",
            |json: &str| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let value: Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
                rhai::serde::to_dynamic(value).map_err(|e| e.to_string().into())
            },
        );

        // YAML parser — needed by bootstrap flows that read user flows from
        // disk/git and key them by `flow.name` rather than filename.
        engine.register_fn(
            "parse_yaml",
            |yaml: &str| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                let value: Value = serde_yaml::from_str(yaml).map_err(|e| e.to_string())?;
                rhai::serde::to_dynamic(value).map_err(|e| e.to_string().into())
            },
        );

        let event_handler = EventHandler {
            code: script_code,
            tx: self.tx.clone(),
            task_id: self.task_id,
            engine,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize script processor");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        {
            Ok(handler) => Arc::new(handler),
            Err(e) => {
                return Err(e);
            }
        };

        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                futures_util::future::join_all(handlers).await;
                return Ok(());
            }

            match self.rx.recv().await {
                Some(event) => {
                    let event_handler = Arc::clone(&event_handler);
                    let retry_strategy = retry_config.strategy();
                    let handle = tokio::spawn(
                        async move {
                            let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                match event_handler.handle(event.clone()).await {
                                    Ok(result) => Ok(result),
                                    Err(e) => {
                                        let is_retriable = !matches!(
                                            &e,
                                            Error::ScriptExecution { .. }
                                                | Error::InvalidReturnType(_)
                                                | Error::ResourceLoad { .. }
                                        );

                                        if is_retriable {
                                            Err(tokio_retry::RetryError::transient(e))
                                        } else {
                                            error!(error = %e, "Script error, skipping event");
                                            Err(tokio_retry::RetryError::permanent(e))
                                        }
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(err.to_string());
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    futures_util::future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
    /// Event sender for passing events to next task (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Event receiver for incoming events (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self.task_id,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

/// Registers flowgen's stateless helper functions on a Rhai engine.
/// Callers own the engine (and its limits); this only adds `fn`s that
/// don't depend on task context — timestamps, hashes, regex, uuid.
pub(crate) fn register_helpers(engine: &mut Engine) {
    engine.register_fn(
        "parse_rfc2822_timestamp",
        |timestamp_str: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::parse_from_rfc2822(timestamp_str)
                .map(|dt| dt.timestamp_millis())
                .map_err(|source| {
                    Error::ParseRFC2822Timestamp {
                        timestamp: timestamp_str.to_string(),
                        source,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "parse_timestamp",
        |timestamp_str: &str| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::parse_from_rfc3339(timestamp_str)
                .map(|dt| dt.timestamp_millis())
                .map_err(|source| {
                    Error::ParseRFC3339Timestamp {
                        timestamp: timestamp_str.to_string(),
                        source,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_iso",
        |timestamp_secs: i64| -> Result<String, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_iso",
        |timestamp_dynamic: Dynamic| -> Result<String, Box<rhai::EvalAltResult>> {
            let timestamp_secs: i64 = timestamp_dynamic.as_int().map_err(|_| {
                Error::TimestampTypeError {
                    type_name: timestamp_dynamic.type_name().to_string(),
                }
                .to_string()
            })?;
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.format("%Y-%m-%dT%H:%M:%SZ").to_string())
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn("uuid", || -> String { uuid::Uuid::now_v7().to_string() });

    engine.register_fn("sha256", |input: &str| -> String {
        use sha2::Digest;
        let hash = sha2::Sha256::digest(input.as_bytes());
        format!("{hash:x}")
    });

    engine.register_fn("sha512", |input: &str| -> String {
        use sha2::Digest;
        let hash = sha2::Sha512::digest(input.as_bytes());
        format!("{hash:x}")
    });

    engine.register_fn("timestamp_now", || -> i64 {
        chrono::Utc::now().timestamp()
    });

    // Returns true when `pattern` matches anywhere in `input`.
    engine.register_fn(
        "regex_match",
        |input: &str, pattern: &str| -> Result<bool, Box<rhai::EvalAltResult>> {
            regex::Regex::new(pattern)
                .map(|re| re.is_match(input))
                .map_err(|source| {
                    Box::new(rhai::EvalAltResult::ErrorRuntime(
                        format!("Invalid regex '{pattern}': {source}").into(),
                        rhai::Position::NONE,
                    ))
                })
        },
    );

    // Replaces every match of `pattern` in `input` with `replacement`.
    // Capture groups (`$1`, `$name`) are supported in the replacement.
    engine.register_fn(
        "regex_replace",
        |input: &str,
         pattern: &str,
         replacement: &str|
         -> Result<String, Box<rhai::EvalAltResult>> {
            regex::Regex::new(pattern)
                .map(|re| re.replace_all(input, replacement).into_owned())
                .map_err(|source| {
                    Box::new(rhai::EvalAltResult::ErrorRuntime(
                        format!("Invalid regex '{pattern}': {source}").into(),
                        rhai::Position::NONE,
                    ))
                })
        },
    );

    // Returns the first match of `pattern` in `input`, or `""` when no match.
    engine.register_fn(
        "regex_find",
        |input: &str, pattern: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            regex::Regex::new(pattern)
                .map(|re| {
                    re.find(input)
                        .map(|m| m.as_str().to_string())
                        .unwrap_or_default()
                })
                .map_err(|source| {
                    Box::new(rhai::EvalAltResult::ErrorRuntime(
                        format!("Invalid regex '{pattern}': {source}").into(),
                        rhai::Position::NONE,
                    ))
                })
        },
    );

    engine.register_fn(
        "timestamp_format",
        |timestamp_secs: i64, format: &str| -> Result<String, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.format(format).to_string())
                .ok_or_else(|| {
                    Box::new(rhai::EvalAltResult::ErrorRuntime(
                        format!("Invalid timestamp: {timestamp_secs}").into(),
                        rhai::Position::NONE,
                    ))
                })
        },
    );

    engine.register_fn(
        "timestamp_to_year",
        |timestamp_secs: i64| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.year() as i64)
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_month",
        |timestamp_secs: i64| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.month() as i64)
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_day",
        |timestamp_secs: i64| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.day() as i64)
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_hour",
        |timestamp_secs: i64| -> Result<i64, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| dt.hour() as i64)
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn(
        "timestamp_to_hive_path",
        |timestamp_secs: i64| -> Result<String, Box<rhai::EvalAltResult>> {
            chrono::DateTime::from_timestamp(timestamp_secs, 0)
                .map(|dt| {
                    format!(
                        "year={}/month={:02}/day={:02}/hour={:02}",
                        dt.year(),
                        dt.month(),
                        dt.day(),
                        dt.hour()
                    )
                })
                .ok_or_else(|| {
                    Error::InvalidUnixTimestamp {
                        timestamp: timestamp_secs,
                    }
                    .to_string()
                    .into()
                })
        },
    );

    engine.register_fn("timestamp_round_to_hour", |timestamp_secs: i64| -> i64 {
        (timestamp_secs / 3600) * 3600
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::runner::Runner;
    use serde_json::{json, Map, Value};
    use tokio::sync::mpsc;

    fn engine_with_helpers() -> Engine {
        let mut engine = Engine::new();
        register_helpers(&mut engine);
        engine
    }

    #[test]
    fn regex_match_returns_true_on_hit() {
        let engine = engine_with_helpers();
        let result: bool = engine
            .eval(r#"regex_match("flowgen.flows.demo/nba", "^flowgen\\.flows\\.")"#)
            .unwrap();
        assert!(result);
    }

    #[test]
    fn regex_match_returns_false_on_miss() {
        let engine = engine_with_helpers();
        let result: bool = engine
            .eval(r#"regex_match("flowgen.flows.demo", "^flowgen\\.resources\\.")"#)
            .unwrap();
        assert!(!result);
    }

    #[test]
    fn regex_replace_strips_yaml_extension() {
        let engine = engine_with_helpers();
        let result: String = engine
            .eval(r#"regex_replace("demo/nba.yaml", "\\.(yaml|yml|json)$", "")"#)
            .unwrap();
        assert_eq!(result, "demo/nba");
    }

    #[test]
    fn regex_replace_preserves_dots_in_directories() {
        let engine = engine_with_helpers();
        let result: String = engine
            .eval(r#"regex_replace("foo.bar/baz.yml", "\\.(yaml|yml|json)$", "")"#)
            .unwrap();
        assert_eq!(result, "foo.bar/baz");
    }

    #[test]
    fn regex_replace_supports_capture_groups() {
        let engine = engine_with_helpers();
        let result: String = engine
            .eval(r#"regex_replace("hello world", "(\\w+) (\\w+)", "$2 $1")"#)
            .unwrap();
        assert_eq!(result, "world hello");
    }

    #[test]
    fn regex_find_returns_first_match() {
        let engine = engine_with_helpers();
        let result: String = engine
            .eval(r#"regex_find("build 42 tests 7", "\\d+")"#)
            .unwrap();
        assert_eq!(result, "42");
    }

    #[test]
    fn regex_find_returns_empty_on_miss() {
        let engine = engine_with_helpers();
        let result: String = engine
            .eval(r#"regex_find("no digits here", "\\d+")"#)
            .unwrap();
        assert_eq!(result, "");
    }

    #[test]
    fn regex_functions_reject_invalid_pattern() {
        let engine = engine_with_helpers();
        let err = engine
            .eval::<bool>(r#"regex_match("foo", "[unclosed")"#)
            .unwrap_err();
        assert!(err.to_string().contains("Invalid regex"));
    }

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Script Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache =
            Arc::new(crate::cache::memory::MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::task::script::config::Processor {
            name: "test".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: crate::resource::Source::Inline("event".to_string()),
            sandbox: None,
            limits: crate::task::script::config::RhaiLimits::default(),
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(100);

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .receiver(rx)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(processor.is_ok());

        // Error case - missing config.
        let (tx2, rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }

    #[tokio::test]
    async fn test_script_simple_transformation() {
        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            code: r#"#{ original: event.data, transformed: true }"#.to_string(),
            tx: Some(tx.clone()),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"x": 5})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        // Drop the original tx so recv can complete
        drop(tx);

        event_handler.handle(input_event).await.unwrap();

        let output_event = rx.recv().await.unwrap();

        match output_event.data {
            EventData::Json(value) => {
                assert_eq!(value["original"]["x"], 5);
                assert_eq!(value["transformed"], true);
            }
            _ => panic!("Expected JSON output"),
        }
    }

    /// Documents how `rhai::Dynamic::Unit` (the `()` value) renders when
    /// a script forces it through string formatters. Operators have seen
    /// payloads land on Salesforce with `"None"` in Email/picklist
    /// fields and on MSSQL with `"String(None)"` in varchar columns;
    /// neither pattern is what `dynamic_to_json` produces (verified by
    /// `rhai_unit_in_map_must_serialize_as_json_null`), so they almost
    /// certainly originate from a Rhai script that coerced `()` to a
    /// string via `+ ""`, interpolation, or `to_string()`. This test
    /// captures the exact strings to point future debugging at the
    /// flow YAML rather than the runtime.
    #[test]
    fn rhai_unit_string_coercions_are_diagnostic() {
        let engine = rhai::Engine::new();

        // `() + ""` concatenation: Rhai stringifies the unit before
        // joining. Salesforce sees this as the literal "" or "None"
        // depending on Rhai's Display impl.
        let result: rhai::Dynamic = engine.eval(r#"() + """#).unwrap();
        let coerced = result.into_string().expect("expected string result");
        assert_eq!(
            coerced, "",
            "Rhai () + \"\" should produce empty string. \
             If this ever becomes \"None\", investigate the Rhai \
             upgrade — the regression would explain field-level \
             validation failures downstream."
        );

        // Debug repr of Dynamic::Unit — used by some Rust loggers and
        // matches the `String(None)` pattern reported by tenants.
        let unit = rhai::Dynamic::UNIT;
        assert_eq!(format!("{unit:?}"), "()");
        // Display impl
        assert_eq!(format!("{unit}"), "");
    }

    /// Reproduces the bug where Rhai `()` (unit) inside a returned map
    /// surfaces in JSON serialization as a string instead of `null`.
    /// Downstream consumers (Salesforce composite API, MSSQL inserts)
    /// see literal "None" / "String(None)" and either reject the record
    /// or persist the garbage string into a varchar column. `()` MUST
    /// round-trip to `serde_json::Value::Null`.
    #[test]
    fn rhai_unit_in_map_must_serialize_as_json_null() {
        let engine = rhai::Engine::new();
        let result: rhai::Dynamic = engine
            .eval(
                r#"
                #{
                    Email__c: (),
                    Name: "test",
                    Phone: ()
                }
                "#,
            )
            .unwrap();

        let value = super::dynamic_to_json(result).expect("unit in map must convert to JSON");
        let obj = value.as_object().expect("expected JSON object");
        assert_eq!(
            obj.get("Email__c"),
            Some(&Value::Null),
            "Rhai () must serialize to JSON null, not a string. got: {:?}",
            obj.get("Email__c"),
        );
        assert_eq!(
            obj.get("Phone"),
            Some(&Value::Null),
            "Rhai () must serialize to JSON null, not a string. got: {:?}",
            obj.get("Phone"),
        );
        assert_eq!(obj.get("Name"), Some(&Value::String("test".to_string())));
    }

    #[tokio::test]
    async fn test_script_filter_null() {
        let (tx, mut rx) = mpsc::channel(100);
        let tx_clone = tx.clone();

        let event_handler = EventHandler {
            code: r#"if data.age < 18 { null } else { data }"#.to_string(),
            tx: Some(tx_clone),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({"age": 15})),
            subject: "input.subject".to_string(),
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
            task_id: 0,
            id: None,
            timestamp: 123456789,
        };

        tokio::spawn(async move {
            event_handler.handle(input_event).await.unwrap();
        });

        // Should not receive any event (filtered out)
        let result = tokio::time::timeout(tokio::time::Duration::from_millis(100), rx.recv()).await;
        assert!(
            result.is_err(),
            "Expected no event to be sent (filtered out by null)"
        );
    }

    #[tokio::test]
    async fn test_rhai_trim_behavior() {
        let engine = Engine::new();

        // trim() in Rhai is in-place and returns ().
        let result: rhai::Dynamic = engine.eval(r#"let s = " hello "; s.trim(); s"#).unwrap();
        assert_eq!(result.into_string().unwrap(), "hello");

        // to_lower() returns a new string.
        let result: rhai::Dynamic = engine.eval(r#""HELLO".to_lower()"#).unwrap();
        assert_eq!(result.into_string().unwrap(), "hello");

        // Chaining to_lower().trim() — trim() returns () because it modifies in-place.
        let result: rhai::Dynamic = engine.eval(r#""HELLO ".to_lower().trim()"#).unwrap();
        // If trim() returns (), this will be unit, not a string.
        let is_unit = result.is_unit();

        // Verify: to_lower() returns new string, but trim() on it returns ().
        assert!(
            is_unit,
            "to_lower().trim() should return () because trim() is in-place"
        );

        // .map() with broken chain produces array of ().
        let result: rhai::Dynamic = engine
            .eval(r#"[#{ name: " HELLO " }].map(|row| row.name.to_lower().trim())"#)
            .unwrap();
        let arr = result.into_array().unwrap();
        assert!(
            arr[0].is_unit(),
            ".map() with to_lower().trim() produces () per element"
        );

        // Correct pattern: separate trim into its own statement.
        let result: rhai::Dynamic = engine
            .eval(
                r#"[#{ name: " HELLO " }].map(|row| { let n = row.name.to_lower(); n.trim(); n })"#,
            )
            .unwrap();
        let arr = result.into_array().unwrap();
        assert_eq!(arr[0].clone().into_string().unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_rhai_string_suffix_strip_lowercase() {
        // Pattern used by examples/salesforce/cdc/writer.yaml to derive the
        // BigQuery table name from a CDC topic. The script splits the NATS
        // subject (`pubsub.data.AccountChangeEvent`), takes the SObject
        // segment (`AccountChangeEvent`), strips the `ChangeEvent` suffix,
        // and lowercases the result (`account`). Verifies that the Rhai
        // string ops the YAML relies on (`split`, `ends_with`, `len`,
        // `sub_string`, `to_lower`) all behave as expected.
        let engine = Engine::new();

        // Builds the table-routing key for AccountChangeEvent.
        let result: rhai::Dynamic = engine
            .eval(
                r#"
                let parts = "pubsub.data.AccountChangeEvent".split(".");
                let topic = parts[2];
                let suffix = "ChangeEvent";
                if topic.ends_with(suffix) {
                    topic.sub_string(0, topic.len - suffix.len).to_lower()
                } else {
                    ""
                }
                "#,
            )
            .unwrap();
        assert_eq!(result.into_string().unwrap(), "account");

        // Same pattern with a different SObject confirms it's not hardcoded.
        let result: rhai::Dynamic = engine
            .eval(
                r#"
                let topic = "OpportunityLineItemChangeEvent";
                let suffix = "ChangeEvent";
                topic.sub_string(0, topic.len - suffix.len).to_lower()
                "#,
            )
            .unwrap();
        assert_eq!(result.into_string().unwrap(), "opportunitylineitem");

        // Topic without the expected suffix falls through the `if` branch.
        let result: rhai::Dynamic = engine
            .eval(
                r#"
                let topic = "AccountChangeFeed";
                let suffix = "ChangeEvent";
                if topic.ends_with(suffix) {
                    topic.sub_string(0, topic.len - suffix.len).to_lower()
                } else {
                    ()
                }
                "#,
            )
            .unwrap();
        assert!(result.is_unit());
    }

    #[tokio::test]
    async fn test_script_array_output() {
        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            code: r#"[#{ id: 1 }, #{ id: 2 }, #{ id: 3 }]"#.to_string(),
            tx: Some(tx),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let _ = event_handler.handle(input_event).await;
        });

        // Should receive 3 events
        let event1 = rx.recv().await.unwrap();
        let event2 = rx.recv().await.unwrap();
        let event3 = rx.recv().await.unwrap();

        match event1.data {
            EventData::Json(value) => assert_eq!(value["id"], 1),
            _ => panic!("Expected JSON output"),
        }
        match event2.data {
            EventData::Json(value) => assert_eq!(value["id"], 2),
            _ => panic!("Expected JSON output"),
        }
        match event3.data {
            EventData::Json(value) => assert_eq!(value["id"], 3),
            _ => panic!("Expected JSON output"),
        }
    }

    #[tokio::test]
    async fn test_script_map_closure_property_access() {
        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            code: r#"
                event.data.map(|row| #{
                    full_name: row.first_name + " " + row.last_name
                })
            "#
            .to_string(),
            tx: Some(tx),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let input_event = Event {
            data: EventData::Json(json!([
                {"first_name": "John", "last_name": "Doe"},
                {"first_name": "Jane", "last_name": "Smith"}
            ])),
            subject: "test".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        tokio::spawn(async move {
            let result = event_handler.handle(input_event).await;
            if let Err(e) = &result {
                eprintln!("Handle error: {e}");
            }
        });

        let event1 = rx.recv().await.unwrap();
        match event1.data {
            EventData::Json(value) => {
                assert_eq!(value["full_name"], "John Doe");
            }
            _ => panic!("Expected JSON output"),
        }

        let event2 = rx.recv().await.unwrap();
        match event2.data {
            EventData::Json(value) => {
                assert_eq!(value["full_name"], "Jane Smith");
            }
            _ => panic!("Expected JSON output"),
        }
    }

    #[tokio::test]
    async fn test_event_handler_arrow_input() {
        let (tx, mut rx) = mpsc::channel(100);

        let event_handler = EventHandler {
            code: "event".to_string(),
            tx: Some(tx),
            task_id: 1,
            engine: Engine::new(),
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        // Create an ArrowRecordBatch event
        let schema = arrow::datatypes::Schema::new(vec![arrow::datatypes::Field::new(
            "test",
            arrow::datatypes::DataType::Int32,
            false,
        )]);
        let batch = arrow::array::RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let input_event = Event {
            data: EventData::ArrowRecordBatch(batch),
            subject: "input.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        let result = event_handler.handle(input_event).await;
        assert!(result.is_ok());

        let output_event = rx.try_recv().unwrap();
        assert_eq!(output_event.subject, "input.subject");
        assert_eq!(output_event.task_id, 1);
    }

    #[tokio::test]
    async fn test_script_sha256() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_sha256".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"let id = sha256("campaign_1_ref_42"); #{ id: id }"#.to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = processor
            .init()
            .await
            .expect("Failed to initialize processor.");

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler
            .handle(input_event)
            .await
            .expect("Failed to handle event.");

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                let id = value["id"].as_str().expect("Expected string id.");
                assert_eq!(id.len(), 64);
            }
            _ => panic!("Expected JSON output."),
        }
    }

    #[tokio::test]
    async fn test_script_sha512() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_sha512".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"let id = sha512("campaign_1_ref_42"); #{ id: id }"#.to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = processor
            .init()
            .await
            .expect("Failed to initialize processor.");

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler
            .handle(input_event)
            .await
            .expect("Failed to handle event.");

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                let id = value["id"].as_str().expect("Expected string id.");
                assert_eq!(id.len(), 128);
            }
            _ => panic!("Expected JSON output."),
        }
    }

    #[tokio::test]
    async fn test_script_sha256_deterministic() {
        let task_context = create_mock_task_context();
        let (tx1, mut rx1) = mpsc::channel(100);
        let (tx2, mut rx2) = mpsc::channel(100);

        let code = r#"let id = sha256("same_input"); #{ id: id }"#;

        for (tx, _rx_ref) in [(tx1, &mut rx1), (tx2, &mut rx2)] {
            let processor = Processor {
                config: Arc::new(crate::task::script::config::Processor {
                    name: "test_deterministic".to_string(),
                    engine: crate::task::script::config::ScriptEngine::Rhai,
                    code: crate::resource::Source::Inline(code.to_string()),
                    sandbox: None,
                    limits: crate::task::script::config::RhaiLimits::default(),
                    depends_on: None,
                    retry: None,
                }),
                tx: Some(tx),
                rx: mpsc::channel(100).1,
                task_id: 1,
                task_context: task_context.clone(),
                task_type: "test",
            };

            let handler = processor
                .init()
                .await
                .expect("Failed to initialize processor.");
            let event = Event {
                data: EventData::Json(json!({})),
                subject: "test".to_string(),
                task_id: 0,
                id: None,
                timestamp: 123456789,
                task_type: "test",
                meta: None,
                error: None,
                completion_tx: None,
            };
            handler
                .handle(event)
                .await
                .expect("Failed to handle event.");
        }

        let out1 = rx1.recv().await.expect("Expected first output.");
        let out2 = rx2.recv().await.expect("Expected second output.");

        let id1 = match &out1.data {
            EventData::Json(v) => v["id"].as_str().expect("Expected string.").to_string(),
            _ => panic!("Expected JSON."),
        };
        let id2 = match &out2.data {
            EventData::Json(v) => v["id"].as_str().expect("Expected string.").to_string(),
            _ => panic!("Expected JSON."),
        };
        assert_eq!(id1, id2);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_script_cache_put_string_value() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_cache_put_string".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"
                    ctx.cache.put("lock_key", "holder_pod_1", 120);
                    let cached = ctx.cache.get("lock_key");
                    #{ cached: cached }
                    "#
                    .to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = processor
            .init()
            .await
            .expect("Failed to initialize processor.");

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler
            .handle(input_event)
            .await
            .expect("Failed to handle event.");

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                assert_eq!(
                    value["cached"].as_str().expect("Expected string."),
                    "holder_pod_1"
                );
            }
            _ => panic!("Expected JSON output."),
        }
    }

    /// Verifies the two-arg `ctx.cache.put(key, value)` overload (no TTL).
    /// Stored value must be readable on the next `get` call.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_script_cache_put_no_ttl_string() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_cache_put_no_ttl".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"
                    ctx.cache.put("persisted_key", "value_without_ttl");
                    let cached = ctx.cache.get("persisted_key");
                    #{ cached: cached }
                    "#
                    .to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = processor
            .init()
            .await
            .expect("Failed to initialize processor.");

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler
            .handle(input_event)
            .await
            .expect("Failed to handle event.");

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                assert_eq!(
                    value["cached"].as_str().expect("Expected string."),
                    "value_without_ttl"
                );
            }
            _ => panic!("Expected JSON output."),
        }
    }

    /// Verifies `ctx.cache.list_keys(prefix)` returns previously-stored keys
    /// with the flow-name namespace stripped.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_script_cache_list_keys() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_cache_list_keys".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"
                    ctx.cache.put("orders.123", "open");
                    ctx.cache.put("orders.456", "closed");
                    ctx.cache.put("users.789", "alice");
                    let order_keys = ctx.cache.list_keys("orders.");
                    #{ count: order_keys.len, first: order_keys[0] }
                    "#
                    .to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = processor
            .init()
            .await
            .expect("Failed to initialize processor.");

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler
            .handle(input_event)
            .await
            .expect("Failed to handle event.");

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                assert_eq!(value["count"].as_i64().expect("Expected int."), 2);
                let first = value["first"].as_str().expect("Expected string.");
                assert!(
                    first == "orders.123" || first == "orders.456",
                    "Expected one of the order keys, got {first}"
                );
            }
            _ => panic!("Expected JSON output."),
        }
    }

    #[tokio::test]
    async fn test_parse_json_array() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let config = Arc::new(crate::task::script::config::Processor {
            name: "test_parse_json_array".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: crate::resource::Source::Inline(
                r#"
                let arr = parse_json(`[{"id":"a1","name":"First"},{"id":"a2","name":"Second"}]`);
                event.data = #{len: arr.len(), first_name: arr[0].name, second_id: arr[1].id};
                event
                "#
                .to_string(),
            ),
            sandbox: None,
            limits: crate::task::script::config::RhaiLimits::default(),
            depends_on: None,
            retry: None,
        });

        let (_event_tx, rx_in) = mpsc::channel(100);
        let processor = ProcessorBuilder::new()
            .config(config)
            .receiver(rx_in)
            .sender(tx)
            .task_id(0)
            .task_context(task_context)
            .task_type("script")
            .build()
            .await
            .unwrap();

        let event_handler = processor.init().await.unwrap();

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 0,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler.handle(input_event).await.unwrap();

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                assert_eq!(value["len"], 2);
                assert_eq!(value["first_name"], "First");
                assert_eq!(value["second_id"], "a2");
            }
            _ => panic!("Expected JSON output."),
        }
    }

    #[tokio::test]
    async fn test_parse_json_object() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();

        let config = Arc::new(crate::task::script::config::Processor {
            name: "test_parse_json_object".to_string(),
            engine: crate::task::script::config::ScriptEngine::Rhai,
            code: crate::resource::Source::Inline(
                r#"
                let obj = parse_json(`{"key":"value","count":42}`);
                event.data = obj;
                event
                "#
                .to_string(),
            ),
            sandbox: None,
            limits: crate::task::script::config::RhaiLimits::default(),
            depends_on: None,
            retry: None,
        });

        let (_event_tx, rx_in) = mpsc::channel(100);
        let processor = ProcessorBuilder::new()
            .config(config)
            .receiver(rx_in)
            .sender(tx)
            .task_id(0)
            .task_context(task_context)
            .task_type("script")
            .build()
            .await
            .unwrap();

        let event_handler = processor.init().await.unwrap();

        let input_event = Event {
            data: EventData::Json(json!({})),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 0,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        event_handler.handle(input_event).await.unwrap();

        let output = rx.recv().await.expect("Expected output event.");
        match output.data {
            EventData::Json(value) => {
                assert_eq!(value["key"], "value");
                assert_eq!(value["count"], 42);
            }
            _ => panic!("Expected JSON output."),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cache_put_runs_exactly_once_per_handle_call_with_array_event_data() {
        let (tx, mut rx) = mpsc::channel(100);
        let task_context = create_mock_task_context();
        let cache = Arc::clone(&task_context.cache);

        let processor = Processor {
            config: Arc::new(crate::task::script::config::Processor {
                name: "test_cache_put_once".to_string(),
                engine: crate::task::script::config::ScriptEngine::Rhai,
                code: crate::resource::Source::Inline(
                    r#"
                    ctx.cache.put("calls", "1", 300);
                    ctx.meta.payload = event.data;
                    event
                    "#
                    .to_string(),
                ),
                sandbox: None,
                limits: crate::task::script::config::RhaiLimits::default(),
                depends_on: None,
                retry: None,
            }),
            tx: Some(tx),
            rx: mpsc::channel(100).1,
            task_id: 1,
            task_context,
            task_type: "test",
        };

        let event_handler = match processor.init().await {
            Ok(handler) => handler,
            Err(e) => panic!("processor.init() returned Err: {e:?}"),
        };

        let input_event = Event {
            data: EventData::Json(json!([{"tick": 2}])),
            subject: "test.subject".to_string(),
            task_id: 0,
            id: None,
            timestamp: 123456789,
            task_type: "test",
            meta: None,
            error: None,
            completion_tx: None,
        };

        match event_handler.handle(input_event).await {
            Ok(()) => {}
            Err(e) => panic!("handle() returned Err: {e:?}"),
        }

        match rx.try_recv() {
            Ok(event) => {
                assert_eq!(event.task_id, 1);
                assert_eq!(
                    event.meta,
                    Some({
                        let mut m = Map::new();
                        m.insert("payload".to_string(), json!([{"tick": 2}]));
                        m
                    }),
                    "ctx.meta.payload = event.data must persist onto the output \
                     event's meta field; got {:?}",
                    event.meta
                );
            }
            Err(e) => panic!("expected one output event on the channel, got: {e:?}"),
        }

        let keys = match cache.list_keys("").await {
            Ok(keys) => keys,
            Err(e) => panic!("cache.list_keys() returned Err: {e:?}"),
        };
        let call_key_count = keys.iter().filter(|k| k.ends_with("calls")).count();
        assert_eq!(
            call_key_count, 1,
            "a single handle() call with array event.data wrote {call_key_count} \
             cache keys ending in 'calls'; full key list: {keys:?}"
        );
    }
}
