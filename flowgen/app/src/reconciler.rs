//! Hot-reload reconciler for cache-sourced flows.
//!
//! Receives batched `WatchEvent`s from the watcher, debounces rapid sequences of
//! writes (e.g. a git push syncing many flows at once), and reconciles the running
//! flow registry: starting new flows, stopping removed ones, and swapping changed ones
//! without interrupting flows that were not affected.

use crate::app::FlowHandle;
use crate::config::{FlowConfig, FlowConfigRaw};
use flowgen_core::cache::{Cache, WatchEvent};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tokio::time::{Duration, Instant};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Coalescing window: events that arrive within this window are batched together.
const DEBOUNCE_WINDOW: Duration = Duration::from_secs(2);

/// Ceiling: a batch is always flushed after this duration regardless of ongoing events.
const DEBOUNCE_CEILING: Duration = Duration::from_secs(10);

/// Errors that can occur in the reconciler.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Failed to parse a flow configuration received from the cache.
    #[error("Failed to parse flow config for key '{key}': {source}")]
    FlowConfigParse {
        key: String,
        #[source]
        source: config::ConfigError,
    },
    /// Failed to read the flow registry.
    #[error("Failed to read flow registry")]
    RegistryReadFailed,
    /// Failed to write the flow registry.
    #[error("Failed to write flow registry")]
    RegistryWriteFailed,
    /// Failed to build a new flow during hot-reload.
    #[error("Failed to build flow '{flow}': {source}")]
    FlowBuild {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Failed to initialise a new flow during hot-reload.
    #[error("Failed to initialise flow '{flow}': {source}")]
    FlowInit {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Failed to start tasks for a new flow during hot-reload.
    #[error("Failed to start tasks for flow '{flow}': {source}")]
    FlowStartTasks {
        flow: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
}

/// All dependencies the reconciler needs to build and start replacement flows.
pub struct ReconcilerContext {
    /// Flow cache. Watched for flow YAML changes and used for stale-entry
    /// cleanup. Holds the flow definitions, which may sit in a different
    /// bucket than the coordination state in `system_cache`.
    pub cache: Arc<dyn Cache>,
    /// System cache. Holds leader-election leases and other cluster-wide
    /// coordination state; passed to rebuilt flows as their `system_cache`.
    pub system_cache: Arc<dyn Cache>,
    /// Runtime cache. Holds per-flow state (replay IDs, counters, last-run
    /// timestamps) and is exposed to user scripts via `ctx.cache`. Passed
    /// to rebuilt flows as their `cache`. May be the same `Arc` as
    /// `cache` in in-memory deployments.
    pub runtime_cache: Arc<dyn Cache>,
    pub app_config: Arc<crate::config::AppConfig>,
    pub resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    pub http_server: Option<Arc<flowgen_http::server::EndpointServer>>,
    pub mcp_server: Option<Arc<flowgen_mcp::server::McpServer>>,
    pub ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>>,
    pub filesystem_flow_paths: Arc<HashSet<String>>,
    pub flow_registry: Arc<RwLock<HashMap<String, FlowHandle>>>,
    pub client_registry: Arc<flowgen_core::client_registry::ClientRegistry>,
    /// This pod's holder identity, shared across every flow's Executor.
    pub holder_identity: String,
    /// Shared peer registry for flow distribution via consistent hashing.
    /// Same instance the app built at startup, so hot-reloaded flows see the
    /// same peer list as flows started at boot.
    pub peer_registry: Arc<flowgen_core::peer::PeerRegistry>,
}

/// Runs the reconciler loop until `shutdown` is cancelled.
///
/// Performs an initial cleanup of stale cache entries (keys whose flows are
/// already loaded from the filesystem), then drains `WatchEvent`s from the
/// watcher channel, coalesces rapid sequences into batches, and calls
/// `reconcile_batch` for each batch.
pub async fn run(
    mut rx: tokio::sync::mpsc::Receiver<WatchEvent>,
    ctx: ReconcilerContext,
    shutdown: CancellationToken,
) {
    cleanup_stale_cache_entries(&ctx).await;

    loop {
        let batch = match collect_batch(&mut rx, &shutdown).await {
            Some(b) => b,
            None => break,
        };
        reconcile_batch(batch, &ctx).await;
    }
    info!("Hot-reload reconciler stopped");
}

/// Deletes cache keys whose derived flow name matches a filesystem-managed flow.
///
/// Prevents stale cache entries (e.g. from a previous git-sync run) from
/// re-appearing after a restart when the user has switched back to file-based
/// flow management.
async fn cleanup_stale_cache_entries(ctx: &ReconcilerContext) {
    if ctx.filesystem_flow_paths.is_empty() {
        return;
    }

    let prefix = ctx
        .app_config
        .flows
        .cache
        .as_ref()
        .map(|c| c.prefix.as_str())
        .unwrap_or(crate::config::DEFAULT_FLOWS_CACHE_PREFIX);

    let keys = match ctx.cache.list_keys(prefix).await {
        Ok(k) => k,
        Err(e) => {
            warn!(error = %e, "Failed to list cache keys for stale entry cleanup");
            return;
        }
    };

    for key in keys {
        let flow_name = match derive_flow_name(&key, ctx) {
            Some(name) => name,
            None => continue,
        };

        if ctx.filesystem_flow_paths.contains(&flow_name) {
            info!(
                flow = %flow_name,
                key = %key,
                "Deleting stale cache entry superseded by filesystem flow"
            );
            if let Err(e) = ctx.cache.delete(&key).await {
                warn!(
                    key = %key,
                    error = %e,
                    "Failed to delete stale cache key"
                );
            }
        }
    }
}

/// Collects a debounced batch of watch events.
///
/// Waits for the first event, then accumulates additional events that arrive within
/// `DEBOUNCE_WINDOW`. Forces a flush after `DEBOUNCE_CEILING` regardless of ongoing
/// activity. Returns `None` on shutdown.
async fn collect_batch(
    rx: &mut tokio::sync::mpsc::Receiver<WatchEvent>,
    shutdown: &CancellationToken,
) -> Option<Vec<WatchEvent>> {
    let first = tokio::select! {
        _ = shutdown.cancelled() => return None,
        ev = rx.recv() => ev?,
    };

    let mut batch = vec![first];
    let mut window = Box::pin(tokio::time::sleep(DEBOUNCE_WINDOW));
    let mut ceiling = Box::pin(tokio::time::sleep(DEBOUNCE_CEILING));

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => return None,
            _ = &mut window => break,
            _ = &mut ceiling => break,
            ev = rx.recv() => match ev {
                None => break,
                Some(e) => {
                    batch.push(e);
                    window.as_mut().reset(Instant::now() + DEBOUNCE_WINDOW);
                }
            },
        }
    }
    Some(batch)
}

/// Applies a batch of watch events to the running flow registry.
///
/// Deduplicates events by key (last write wins within a batch), then processes
/// each key: replacing existing flows or starting new ones for `Put` events,
/// and stopping flows for `Delete` events. Filesystem flows are never overwritten.
async fn reconcile_batch(batch: Vec<WatchEvent>, ctx: &ReconcilerContext) {
    // Deduplicate: last event per key wins.
    let mut by_key: HashMap<String, WatchEvent> = HashMap::new();
    for event in batch {
        let key = match &event {
            WatchEvent::Put { key, .. } | WatchEvent::Delete { key } => key.clone(),
        };
        by_key.insert(key, event);
    }

    for (key, event) in by_key {
        match event {
            WatchEvent::Put { value, .. } => reconcile_put(&key, value, ctx).await,
            WatchEvent::Delete { .. } => reconcile_delete(&key, ctx).await,
        }
    }
}

/// Handles a `Put` event: parses the config, builds and starts the new flow,
/// then stops and deregisters the old one if it was running.
///
/// If any step of building or starting the new flow fails, the old flow is left
/// running and the error is logged. The new flow is fully ready before the old
/// one is touched.
async fn reconcile_put(key: &str, value: bytes::Bytes, ctx: &ReconcilerContext) {
    // Gate by the cache prefix so non-flow keys are ignored entirely. The
    // derived name is only used until we have the parsed yaml.
    let flow_id = match derive_flow_name(key, ctx) {
        Some(name) => name,
        None => return,
    };

    // Parse the YAML config from the cache value.
    let (raw, content) = match parse_flow_config(key, &value) {
        Ok(parsed) => parsed,
        Err(e) => {
            error!(error = %e);
            return;
        }
    };

    // Cache key suffix (path-shaped) is the identity. Wrap the raw config
    // so downstream code — task context, cache namespaces, MCP URIs — sees
    // the same identity the reconciler is keying on.
    let config = match FlowConfig::from_path(raw, flow_id.clone(), Some(content)) {
        Ok(c) => c,
        Err(reason) => {
            error!(
                key = %key,
                error = %reason,
                "Hot-reloaded flow config failed validation, skipping"
            );
            return;
        }
    };

    if ctx.filesystem_flow_paths.contains(&flow_id) {
        warn!(
            flow = %flow_id,
            key = %key,
            "Ignoring cache update: this flow is loaded from the filesystem and takes precedence"
        );
        return;
    }

    // Build and start the replacement flow before touching the running one.
    let mut new_flow = match build_flow(config, ctx) {
        Ok(f) => f,
        Err(e) => {
            error!(error = %e);
            return;
        }
    };

    if let Err(e) = new_flow.init().await {
        error!(
            error = %Error::FlowInit {
                flow: flow_id.clone(),
                source: Box::new(e),
            }
        );
        return;
    }

    let blocking_handles = match new_flow.start_tasks().await {
        Ok(h) => h,
        Err(e) => {
            error!(
                error = %Error::FlowStartTasks {
                    flow: flow_id.clone(),
                    source: Box::new(e),
                }
            );
            return;
        }
    };

    // Await blocking handles (webhook/AI gateway registration) before swapping.
    let results = futures::future::join_all(blocking_handles).await;
    for result in results {
        if let Err(e) = result {
            error!(flow = %flow_id, "Blocking task failed during hot-reload: {e}");
        }
    }

    let flow_display_name = new_flow
        .config
        .flow
        .labels
        .as_ref()
        .and_then(|labels| labels.get("display_name"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let flow_description = new_flow
        .config
        .flow
        .labels
        .as_ref()
        .and_then(|labels| labels.get("description"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let flow_tags = new_flow
        .config
        .flow
        .labels
        .as_ref()
        .and_then(|labels| labels.get("tags"))
        .and_then(|value| value.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(ToString::to_string))
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let require_leader_election = new_flow
        .config
        .flow
        .require_leader_election
        .unwrap_or(false);
    let task_count = new_flow.config.flow.tasks.len();
    let flow_yaml = match new_flow.config.raw_source.clone() {
        Some(source) => source,
        None => match serde_yaml::to_string(&*new_flow.config) {
            Ok(yaml) => yaml,
            Err(source) => {
                tracing::warn!(
                    error = %source,
                    "Failed to serialize flow config to YAML for admin API"
                );
                String::new()
            }
        },
    };

    let cancellation_token = new_flow
        .cancellation_token()
        .unwrap_or_else(tokio_util::sync::CancellationToken::new);

    let task_manager = new_flow.task_manager();

    let join_handle = new_flow.run();

    // Stop the old flow and bulk-deregister its server entries.
    stop_and_deregister(&flow_id, ctx).await;

    // Insert the new handle into the registry.
    match ctx.flow_registry.write() {
        Ok(mut registry) => {
            registry.insert(
                flow_id.clone(),
                FlowHandle {
                    identity: flow_id.clone(),
                    flow_display_name,
                    flow_description,
                    flow_tags,
                    require_leader_election,
                    task_count,
                    started_at: std::time::SystemTime::now(),
                    flow_yaml,
                    cancellation_token,
                    join_handle,
                    from_filesystem: false,
                    task_manager,
                },
            );
            info!(flow = %flow_id, key = %key, "Flow hot-reloaded");
        }
        Err(_) => {
            error!(error = %Error::RegistryWriteFailed);
        }
    }
}

/// Handles a `Delete` event: stops the flow and deregisters its routes/tools.
async fn reconcile_delete(key: &str, ctx: &ReconcilerContext) {
    let flow_name = match derive_flow_name(key, ctx) {
        Some(name) => name,
        None => return,
    };

    if ctx.filesystem_flow_paths.contains(&flow_name) {
        warn!(
            flow = %flow_name,
            key = %key,
            "Ignoring cache delete: this flow is loaded from the filesystem"
        );
        return;
    }

    stop_and_deregister(&flow_name, ctx).await;
    info!(flow = %flow_name, key = %key, "Flow removed via hot-reload");
}

/// Cancels a running flow, awaits its shutdown, and deregisters its HTTP routes
/// and MCP tools from the shared servers.
///
/// Also aborts the old flow's lease renewal background task via the
/// `TaskManager` it carries. Skipping this step would leave the renewer alive
/// in tokio after the `Flow` (and its `TaskManager`) is dropped; that orphan
/// keeps writing renewals to the lease key and races the replacement flow's
/// renewer under the same pod-level holder identity, producing endless
/// "Lost lease ownership during renewal" warnings against `current_holder=self`.
async fn stop_and_deregister(flow_name: &str, ctx: &ReconcilerContext) {
    let old_handle = match ctx.flow_registry.write() {
        Ok(mut registry) => registry.remove(flow_name),
        Err(_) => {
            error!(error = %Error::RegistryWriteFailed);
            return;
        }
    };

    let old = match old_handle {
        Some(h) => h,
        None => return,
    };

    old.cancellation_token.cancel();
    // Await with a timeout so a stuck flow does not block the reconciler indefinitely.
    let timeout = Duration::from_secs(30);
    if tokio::time::timeout(timeout, old.join_handle)
        .await
        .is_err()
    {
        warn!(
            flow = %flow_name,
            "Flow did not stop within 30 seconds, proceeding with deregistration"
        );
    }

    // Abort the lease renewer for this flow without touching the lease key in
    // the cache: the replacement flow has already taken over renewal under
    // the same pod-level holder identity, and the lease must stay claimed
    // continuously across the swap.
    if let Some(task_manager) = &old.task_manager {
        task_manager
            .unregister(&flowgen_core::identity::FlowIdentity::new(flow_name))
            .await;
    }

    // Deregister every webhook, MCP tool, and AI gateway entry owned by this
    // flow in one pass via the generic server's bulk deregister.
    if let Some(http_server) = &ctx.http_server {
        http_server.deregister_flow(flow_name);
    }
    if let Some(mcp_server) = &ctx.mcp_server {
        // Bulk-clears tools, resources, and resource templates in one pass.
        flowgen_mcp::server::deregister_flow_all(mcp_server, flow_name);
    }
    if let Some(ai_gateway_server) = &ctx.ai_gateway_server {
        ai_gateway_server.deregister_flow(flow_name);
    }
}

/// Derives a flow name from a cache key by stripping the configured flow prefix.
///
/// The remainder is returned verbatim — it is the same `flow.name` the sync
/// bootstrap appended when writing the key, so registry lookups on delete
/// events round-trip cleanly for any character `flow.name` permits.
fn derive_flow_name(key: &str, ctx: &ReconcilerContext) -> Option<String> {
    let prefix = ctx
        .app_config
        .flows
        .cache
        .as_ref()
        .map(|c| c.prefix.as_str())
        .unwrap_or(crate::config::DEFAULT_FLOWS_CACHE_PREFIX);

    key.strip_prefix(&format!("{prefix}."))
        .map(str::to_string)
        .or_else(|| {
            warn!(key = %key, prefix = %prefix, "Watch event key does not match flow prefix, ignoring");
            None
        })
}

/// Parses a raw cache value as a `FlowConfigRaw`, returning the deserialized
/// config along with the original text (so the caller can wrap it with
/// `FlowConfig::from_path` and retain the verbatim YAML for the admin API).
fn parse_flow_config(key: &str, value: &bytes::Bytes) -> Result<(FlowConfigRaw, String), Error> {
    let content = String::from_utf8_lossy(value).into_owned();
    let format = match key.ends_with(".json") {
        true => config::FileFormat::Json,
        false => config::FileFormat::Yaml,
    };

    let raw: FlowConfigRaw = config::Config::builder()
        .add_source(config::File::from_str(&content, format))
        .build()
        .and_then(|c| c.try_deserialize::<FlowConfigRaw>())
        .map_err(|source| Error::FlowConfigParse {
            key: key.to_string(),
            source,
        })?;
    Ok((raw, content))
}

/// Builds a minimal `ReconcilerContext` for tests that only exercise
/// `derive_flow_name` and `parse_flow_config`.
#[cfg(test)]
fn test_context(prefix: &str) -> ReconcilerContext {
    use flowgen_core::cache::memory::MemoryCache;

    let app_config = crate::config::AppConfig {
        cache: None,
        flows: crate::config::FlowOptions {
            path: None,
            cache: Some(crate::config::FlowCacheOptions {
                enabled: true,
                prefix: prefix.to_string(),
                db_name: "test".to_string(),
            }),
        },
        resources: None,
        http_server: None,
        mcp_server: None,
        ai_gateway: None,
        web: None,
        health: Default::default(),
        retry: None,
        event_buffer_size: None,
        telemetry: None,
    };

    let shared: Arc<dyn Cache> = Arc::new(MemoryCache::new());
    let holder_identity = "test-pod".to_string();
    ReconcilerContext {
        cache: Arc::clone(&shared),
        system_cache: Arc::clone(&shared),
        runtime_cache: Arc::clone(&shared),
        app_config: Arc::new(app_config),
        resource_loader: None,
        http_server: None,
        mcp_server: None,
        ai_gateway_server: None,
        filesystem_flow_paths: Arc::new(HashSet::new()),
        flow_registry: Arc::new(RwLock::new(HashMap::new())),
        client_registry: Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
        peer_registry: Arc::new(flowgen_core::peer::PeerRegistry::new(
            shared,
            holder_identity.clone(),
        )),
        holder_identity,
    }
}

/// Builds a `Flow` from a parsed config using the current app context.
fn build_flow(
    flow_config: FlowConfig,
    ctx: &ReconcilerContext,
) -> Result<crate::flow::Flow, Error> {
    let flow_id = flow_config.identity().to_string();
    let mut builder = crate::flow::FlowBuilder::new()
        .config(Arc::new(flow_config))
        .cache(Arc::clone(&ctx.runtime_cache))
        .system_cache(Arc::clone(&ctx.system_cache))
        .client_registry(Arc::clone(&ctx.client_registry))
        .holder_identity(ctx.holder_identity.clone())
        .peer_registry(Arc::clone(&ctx.peer_registry));

    if let Some(server) = &ctx.http_server {
        builder = builder.http_server(Arc::clone(server));
    }
    if let Some(server) = &ctx.mcp_server {
        builder = builder.mcp_server(Arc::clone(server));
    }
    if let Some(server) = &ctx.ai_gateway_server {
        builder = builder.ai_gateway_server(Arc::clone(server));
    }
    if let Some(retry) = ctx.app_config.retry.as_ref() {
        builder = builder.retry(retry.clone());
    }
    if let Some(buf) = ctx.app_config.event_buffer_size {
        builder = builder.event_buffer_size(buf);
    }
    if let Some(loader) = &ctx.resource_loader {
        builder = builder.resource_loader(loader.clone());
    }

    builder.build().map_err(|source| Error::FlowBuild {
        flow: flow_id,
        source: Box::new(source),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // -- derive_flow_name ---------------------------------------------------

    #[test]
    fn derive_flow_name_strips_prefix() {
        let ctx = test_context("flowgen.flows");
        let name = derive_flow_name("flowgen.flows.my-flow", &ctx);
        assert_eq!(name, Some("my-flow".to_string()));
    }

    #[test]
    fn derive_flow_name_preserves_dots() {
        let ctx = test_context("flowgen.flows");
        let name = derive_flow_name("flowgen.flows.my.nested.flow", &ctx);
        assert_eq!(name, Some("my.nested.flow".to_string()));
    }

    #[test]
    fn derive_flow_name_returns_none_for_non_matching_prefix() {
        let ctx = test_context("flowgen.flows");
        let name = derive_flow_name("flowgen.resources.tpl", &ctx);
        assert!(name.is_none());
    }

    #[test]
    fn derive_flow_name_uses_default_prefix_when_cache_config_is_none() {
        let app_config = crate::config::AppConfig {
            cache: None,
            flows: crate::config::FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        let shared: Arc<dyn Cache> = Arc::new(flowgen_core::cache::memory::MemoryCache::new());
        let holder_identity = "test-pod".to_string();
        let ctx = ReconcilerContext {
            cache: Arc::clone(&shared),
            system_cache: Arc::clone(&shared),
            runtime_cache: Arc::clone(&shared),
            app_config: Arc::new(app_config),
            resource_loader: None,
            http_server: None,
            mcp_server: None,
            ai_gateway_server: None,
            filesystem_flow_paths: Arc::new(HashSet::new()),
            flow_registry: Arc::new(RwLock::new(HashMap::new())),
            client_registry: Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
            peer_registry: Arc::new(flowgen_core::peer::PeerRegistry::new(
                shared,
                holder_identity.clone(),
            )),
            holder_identity,
        };

        let name = derive_flow_name("flows.default-test", &ctx);
        assert_eq!(name, Some("default-test".to_string()));
    }

    // -- parse_flow_config --------------------------------------------------

    #[test]
    fn parse_flow_config_valid_yaml() {
        let yaml = r#"
flow:
  name: test-flow
  tasks:
    - log:
        name: log-task
"#;
        let result = parse_flow_config("flows.test-flow.yaml", &bytes::Bytes::from(yaml));
        assert!(result.is_ok());
        let (raw, _content) = result.unwrap();
        assert_eq!(raw.flow.name.as_deref(), Some("test-flow"));
        assert_eq!(raw.flow.tasks.len(), 1);
    }

    #[test]
    fn parse_flow_config_invalid_yaml() {
        let bad = b"not: valid: yaml: [[[";
        let result = parse_flow_config("flows.bad.yaml", &bytes::Bytes::from(&bad[..]));
        assert!(result.is_err());
    }

    #[test]
    fn parse_flow_config_detects_json_by_key_suffix() {
        let json = r#"{
            "flow": {
                "name": "json-flow",
                "tasks": [
                    { "log": { "name": "log-task" } }
                ]
            }
        }"#;
        let result = parse_flow_config("flows.json-flow.json", &bytes::Bytes::from(json));
        assert!(result.is_ok());
        let (raw, _content) = result.unwrap();
        assert_eq!(raw.flow.name.as_deref(), Some("json-flow"));
    }

    // -- collect_batch ------------------------------------------------------

    #[tokio::test]
    async fn collect_batch_returns_none_on_shutdown() {
        let (_tx, mut rx) = tokio::sync::mpsc::channel::<WatchEvent>(16);
        let shutdown = CancellationToken::new();
        shutdown.cancel();

        let result = collect_batch(&mut rx, &shutdown).await;
        assert!(result.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn collect_batch_batches_multiple_events() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WatchEvent>(16);
        let shutdown = CancellationToken::new();

        // Send two events before calling collect_batch so they arrive
        // within the debounce window.
        tx.send(WatchEvent::Put {
            key: "flows.a".to_string(),
            value: bytes::Bytes::from("a"),
        })
        .await
        .unwrap();

        tx.send(WatchEvent::Put {
            key: "flows.b".to_string(),
            value: bytes::Bytes::from("b"),
        })
        .await
        .unwrap();

        // Drop the sender so the channel closes and the debounce loop exits
        // after draining.
        drop(tx);

        let batch = collect_batch(&mut rx, &shutdown).await;
        let batch = batch.expect("should produce a batch");
        assert_eq!(batch.len(), 2);
    }

    // -- cleanup_stale_cache_entries ------------------------------------------

    fn test_context_with_fs_flows(prefix: &str, fs_flows: HashSet<String>) -> ReconcilerContext {
        use flowgen_core::cache::memory::MemoryCache;

        let app_config = crate::config::AppConfig {
            cache: None,
            flows: crate::config::FlowOptions {
                path: None,
                cache: Some(crate::config::FlowCacheOptions {
                    enabled: true,
                    prefix: prefix.to_string(),
                    db_name: "test".to_string(),
                }),
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        let shared: Arc<dyn Cache> = Arc::new(MemoryCache::new());
        let holder_identity = "test-pod".to_string();
        ReconcilerContext {
            cache: Arc::clone(&shared),
            system_cache: Arc::clone(&shared),
            runtime_cache: Arc::clone(&shared),
            app_config: Arc::new(app_config),
            resource_loader: None,
            http_server: None,
            mcp_server: None,
            ai_gateway_server: None,
            filesystem_flow_paths: Arc::new(fs_flows),
            flow_registry: Arc::new(RwLock::new(HashMap::new())),
            client_registry: Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
            peer_registry: Arc::new(flowgen_core::peer::PeerRegistry::new(
                shared,
                holder_identity.clone(),
            )),
            holder_identity,
        }
    }

    #[tokio::test]
    async fn cleanup_stale_cache_entries_deletes_filesystem_shadowed_keys() {
        let fs_flows = HashSet::from(["my-flow".to_string(), "other-flow".to_string()]);
        let ctx = test_context_with_fs_flows("flowgen.flows", fs_flows);

        // Populate cache with entries: two match filesystem flows, one does not.
        ctx.cache
            .put("flowgen.flows.my-flow", bytes::Bytes::from("a"), None)
            .await
            .unwrap();
        ctx.cache
            .put("flowgen.flows.other-flow", bytes::Bytes::from("b"), None)
            .await
            .unwrap();
        ctx.cache
            .put(
                "flowgen.flows.cache-only-flow",
                bytes::Bytes::from("c"),
                None,
            )
            .await
            .unwrap();

        cleanup_stale_cache_entries(&ctx).await;

        // Filesystem-shadowed entries should be deleted.
        assert!(ctx
            .cache
            .get("flowgen.flows.my-flow")
            .await
            .unwrap()
            .is_none());
        assert!(ctx
            .cache
            .get("flowgen.flows.other-flow")
            .await
            .unwrap()
            .is_none());
        // Cache-only entry should remain.
        assert!(ctx
            .cache
            .get("flowgen.flows.cache-only-flow")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn cleanup_stale_cache_entries_skips_when_no_filesystem_flows() {
        let ctx = test_context_with_fs_flows("flowgen.flows", HashSet::new());

        ctx.cache
            .put("flowgen.flows.some-flow", bytes::Bytes::from("a"), None)
            .await
            .unwrap();

        cleanup_stale_cache_entries(&ctx).await;

        // Nothing should be deleted when there are no filesystem flows.
        assert!(ctx
            .cache
            .get("flowgen.flows.some-flow")
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn cleanup_stale_cache_entries_matches_dotted_flow_names() {
        // Filesystem flow named with literal dots — the derived flow name
        // must round-trip exactly so the cache entry is recognised as
        // shadowed and removed.
        let fs_flows = HashSet::from(["nested.flow.name".to_string()]);
        let ctx = test_context_with_fs_flows("flowgen.flows", fs_flows);

        ctx.cache
            .put(
                "flowgen.flows.nested.flow.name",
                bytes::Bytes::from("a"),
                None,
            )
            .await
            .unwrap();

        cleanup_stale_cache_entries(&ctx).await;

        assert!(ctx
            .cache
            .get("flowgen.flows.nested.flow.name")
            .await
            .unwrap()
            .is_none());
    }

    /// A rebuilt flow must take its lease from `system_cache`, not from the
    /// bucket the flow YAML was read from. The two are the same object in a
    /// default deployment, so this gives the reconciler three distinct caches
    /// to make a mix-up observable.
    #[tokio::test]
    async fn rebuilt_flow_leases_from_the_system_cache_not_the_flow_cache() {
        let mut ctx = test_context("flows");
        let flow_cache: Arc<dyn Cache> = Arc::new(flowgen_core::cache::memory::MemoryCache::new());
        let system_cache: Arc<dyn Cache> =
            Arc::new(flowgen_core::cache::memory::MemoryCache::new());
        let runtime_cache: Arc<dyn Cache> =
            Arc::new(flowgen_core::cache::memory::MemoryCache::new());
        ctx.cache = Arc::clone(&flow_cache);
        ctx.system_cache = Arc::clone(&system_cache);
        ctx.runtime_cache = Arc::clone(&runtime_cache);

        let yaml = r#"
flow:
  require_leader_election: true
  tasks:
    - generate:
        name: tick
    - log:
        name: out
"#;
        let (raw, _) = parse_flow_config("flows.lease-target", &bytes::Bytes::from(yaml))
            .expect("parse flow yaml");
        let config =
            FlowConfig::from_path(raw, "lease-target".to_string(), None).expect("build config");

        let flow = build_flow(config, &ctx).expect("build flow");
        let lease_cache = flow.system_cache();

        lease_cache
            .put(
                &format!("{}lease-target", flowgen_core::executor::LEASE_KEY_PREFIX),
                bytes::Bytes::from_static(b"held"),
                None,
            )
            .await
            .expect("write a lease through the flow's system cache");

        assert_eq!(
            system_cache
                .list_keys(flowgen_core::executor::LEASE_KEY_PREFIX)
                .await
                .expect("list system lease keys")
                .len(),
            1,
            "the flow's lease writes must land in the system cache"
        );
        assert!(
            flow_cache
                .list_keys(flowgen_core::executor::LEASE_KEY_PREFIX)
                .await
                .expect("list flow lease keys")
                .is_empty(),
            "no lease may land in the bucket holding flow definitions"
        );
        assert!(
            runtime_cache
                .list_keys(flowgen_core::executor::LEASE_KEY_PREFIX)
                .await
                .expect("list runtime lease keys")
                .is_empty(),
            "no lease may land in the script-reachable runtime bucket"
        );
    }
}
