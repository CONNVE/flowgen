use flowgen_core::{
    client::Client,
    config::ConfigExt,
    event::{new_completion_channel, AvroData, Event, EventBuilder, EventData, EventExt},
};
use salesforce_core::pubsubapi::{
    eventbus::v1::{ConsumerEvent, ManagedFetchRequest, SchemaInfo},
    FetchRequest, PubSubError, SchemaRequest, TopicRequest,
};
use std::sync::Arc;
use tokio::sync::{mpsc::Sender, Mutex};
use tokio_stream::StreamExt;
use tracing::{error, warn, Instrument};

const DEFAULT_NUM_REQUESTED: i32 = 200;
const DEFAULT_TOPIC_PREFIX_DATA: &str = "/data/";
const DEFAULT_TOPIC_PREFIX_EVENT: &str = "/event/";

/// Sanitizes a topic name to create a valid NATS KV cache key.
/// Removes leading/trailing slashes and replaces internal slashes with hyphens.
fn sanitize_topic_name(topic_name: &str) -> String {
    topic_name.trim_matches('/').replace('/', "-")
}

/// Errors that can occur during Salesforce Pub/Sub subscription operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Pub/Sub error: {source}")]
    PubSub {
        #[source]
        source: PubSubError,
    },
    #[error("Authentication error: {source}")]
    Auth {
        #[source]
        source: salesforce_core::client::Error,
    },
    #[error("Event error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Async task join failed: {source}")]
    TaskJoin {
        #[source]
        source: tokio::task::JoinError,
    },
    #[error("Error sending event message: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Binary encoding error: {source}")]
    Bincode {
        #[source]
        source: bincode::Error,
    },
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("JSON error: {source}")]
    Serde {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Stream ended unexpectedly, connection may have been lost")]
    StreamEnded,
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Managed subscription requires durable_consumer_options to be configured")]
    MissingManagedSubscriptionConfig,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Processes events from a single Salesforce Pub/Sub topic.
///
/// Subscribes to a topic, deserializes Avro payloads, and forwards events
/// to the event channel. Supports durable consumers with replay ID caching.
pub struct EventHandler {
    /// Salesforce Pub/Sub client context
    pubsub: Arc<Mutex<salesforce_core::pubsubapi::Client>>,
    /// Same key `init()` used to cache the Salesforce client, so `run()` can
    /// evict the exact entry that may be stale rather than recomputing it
    /// (and risking drift if `credentials_path` is templated).
    client_key: flowgen_core::client_registry::ClientKey,
    /// Key for the shared gRPC channel. Evicted alongside `client_key` on
    /// reconnect: the cached channel is the thing that just died, and
    /// leaving it in place would hand every retry the same dead connection.
    channel_key: flowgen_core::client_registry::ClientKey,
    /// Subscriber configuration
    config: Arc<super::config::Subscriber>,
    /// Channel sender for processed events
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

/// Checks if a gRPC error is due to an invalid/corrupted replay ID.
fn is_replay_id_error(error: &PubSubError) -> bool {
    if let PubSubError::Tonic(status) = error {
        if let Some(error_code) = status.metadata().get("error-code") {
            if let Ok(code_str) = error_code.to_str() {
                return code_str
                    == "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.validation.failed"
                    || code_str
                        == "sfdc.platform.eventbus.grpc.subscription.fetch.replayid.corrupted";
            }
        }
    }
    false
}

/// Checks if a gRPC error is due to invalid authentication.
fn is_auth_error(error: &PubSubError) -> bool {
    if let PubSubError::Tonic(status) = error {
        let message = status.message();
        return message.contains("does not have valid authentication credentials")
            || message.contains("authentication exception occurred");
    }
    false
}

impl EventHandler {
    /// Processes a batch of events from Salesforce Pub/Sub.
    /// Events are processed concurrently, but replay_id is only cached after ALL events succeed.
    async fn process_events(
        &self,
        events: Vec<ConsumerEvent>,
        schema_info: &SchemaInfo,
        topic_name: &str,
    ) -> Result<(), Error> {
        if events.is_empty() {
            return Ok(());
        }

        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let cache = Arc::clone(&self.task_context.cache);
        let flow_name = self.task_context.flow.identity().to_string();
        // `flow_key` keys the cache; `flow_name` stays human-readable for logs.
        let flow_key = self.task_context.flow.id();
        let mut handles = Vec::new();
        let last_replay_id = events.last().map(|ce| ce.replay_id.clone());

        for ce in events {
            if self.task_context.cancellation_token.is_cancelled() {
                break;
            }

            if let Some(event) = ce.event {
                let tx = self.tx.clone();
                let task_id = self.task_id;
                let task_type = self.task_type;
                let schema = schema_info.schema_json.clone();
                let event_id = event.id.clone();

                // Normalize topic name by removing data/ or event/ prefix.
                let subject = topic_name
                    .strip_prefix(DEFAULT_TOPIC_PREFIX_DATA)
                    .or_else(|| topic_name.strip_prefix(DEFAULT_TOPIC_PREFIX_EVENT))
                    .unwrap_or(topic_name)
                    .to_lowercase();

                let config = Arc::clone(&self.config);
                let cancellation_token = self.task_context.cancellation_token.clone();
                let leaf_count = self.task_context.leaf_count;

                // Spawn a task for each event to process them concurrently.
                let handle = tokio::spawn(
                    async move {
                        // Check cancellation before processing.
                        if cancellation_token.is_cancelled() {
                            return Ok(());
                        }

                        // Size the completion channel to the number of leaves
                        // in this flow. The Pub/Sub event is acked only after
                        // every leaf has signalled completion.
                        let (completion_state, completion_rx) = new_completion_channel(leaf_count);

                        // Setup event data payload.
                        let data = AvroData {
                            schema,
                            raw_bytes: event.payload[..].to_vec(),
                        };

                        // Build and send event.
                        let mut e = EventBuilder::new()
                            .data(EventData::Avro(data))
                            .subject(subject)
                            .id(event.id)
                            .task_id(task_id)
                            .task_type(task_type)
                            .build()
                            .map_err(|e| Error::Event { source: e })?;

                        e.completion_tx = Some(completion_state);

                        // Check cancellation before sending to prevent "receiver dropped" errors.
                        if cancellation_token.is_cancelled() {
                            return Ok(());
                        }

                        e.send_with_logging(tx.as_ref())
                            .await
                            .map_err(|source| Error::SendMessage { source })?;

                        // Wait for flow completion.
                        // Failed flows skip replay_id advancement, allowing next fetch to retry from same position.
                        let success = match config.ack_timeout {
                            Some(timeout) => matches!(
                                tokio::time::timeout(timeout, completion_rx).await,
                                Ok(Ok(Ok(_)))
                            ),
                            None => matches!(completion_rx.await, Ok(Ok(_))),
                        };

                        if !success {
                            warn!(
                                event_id = ?event_id,
                                "Flow completion failed or timed out"
                            );
                        }

                        Ok::<(), Error>(())
                    }
                    .instrument(tracing::info_span!(
                        parent: tracing::Span::current(),
                        "task.handle",
                        duration_ms = tracing::field::Empty,
                    )),
                );

                handles.push(handle);
            }
        }

        // Wait for all events in the batch to complete.
        let mut all_succeeded = true;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {} // Event succeeded.
                Ok(Err(e)) => {
                    error!("Event processing failed: {}", e);
                    all_succeeded = false;
                }
                Err(e) => {
                    error!("Event processing task panicked: {}", e);
                    all_succeeded = false;
                }
            }
        }

        if all_succeeded && !self.task_context.cancellation_token.is_cancelled() {
            if let Some(replay_id) = last_replay_id {
                if self
                    .config
                    .topic
                    .durable_consumer_options
                    .as_ref()
                    .is_some_and(|opts| opts.enabled && !opts.managed_subscription)
                {
                    let sanitized_topic = sanitize_topic_name(topic_name);
                    let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
                    if let Err(e) = cache.put(&cache_key, replay_id.into(), None).await {
                        error!(
                            "Failed to cache replay_id for flow {flow_name} topic {topic_name}: {e}"
                        );
                    }
                }
            }
        } else {
            warn!("Not all events succeeded in batch, replay_id not advanced");
        }

        Ok(())
    }

    /// Runs the topic listener to process events from Salesforce Pub/Sub.
    ///
    /// Fetches topic and schema info, establishes subscription with optional
    /// replay ID, then processes incoming events in a loop.
    async fn handle(self) -> Result<(), Error> {
        // Get cache from task context.
        let cache = &self.task_context.cache;
        let flow_name = self.task_context.flow.identity();
        // `flow_key` keys the cache; `flow_name` stays human-readable for logs.
        let flow_key = self.task_context.flow.id();
        // Get topic metadata.
        let topic_info = self
            .pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: self.config.topic.name.clone(),
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        // Get schema for message deserialization.
        let schema_info = self
            .pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        // Set batch size for event fetching.
        let num_requested = match self.config.topic.num_requested {
            Some(num_requested) => num_requested,
            None => DEFAULT_NUM_REQUESTED,
        };

        // Get topic name for later use.
        let topic_name = topic_info.topic_name.as_str();

        // Determine subscription type and create appropriate stream.
        let use_managed_subscription = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .map(|opts| opts.enabled && opts.managed_subscription)
            .unwrap_or(false);

        if use_managed_subscription {
            // Use managed subscription.
            let durable_consumer_opts = self
                .config
                .topic
                .durable_consumer_options
                .as_ref()
                .ok_or(Error::MissingManagedSubscriptionConfig)?;

            let managed_request = ManagedFetchRequest {
                developer_name: durable_consumer_opts.name.clone(),
                num_requested,
                ..Default::default()
            };

            let mut stream = self
                .pubsub
                .lock()
                .await
                .managed_subscribe(managed_request)
                .await
                .map_err(|e| Error::PubSub { source: e })?
                .into_inner();

            // Process managed subscription events.
            while let Some(event) = stream.next().await {
                if self.task_context.cancellation_token.is_cancelled() {
                    drop(stream);
                    return Ok(());
                }

                let events = match event {
                    Ok(fr) => fr.events,
                    Err(e) => {
                        return Err(Error::PubSub {
                            source: PubSubError::Tonic(Box::new(e)),
                        });
                    }
                };

                self.process_events(events, &schema_info, topic_name)
                    .await?;

                if self.task_context.cancellation_token.is_cancelled() {
                    drop(stream);
                    return Ok(());
                }
            }

            return Err(Error::StreamEnded);
        }

        // Build fetch request with optional replay_id.
        let mut fetch_request = FetchRequest {
            topic_name: topic_name.to_string(),
            num_requested,
            ..Default::default()
        };

        // Set replay ID or preset for non-managed durable consumers.
        if let Some(durable_consumer_opts) = self
            .config
            .topic
            .durable_consumer_options
            .as_ref()
            .filter(|opts| opts.enabled && !opts.managed_subscription)
        {
            // Try to load cached replay_id, or use configured preset.
            let sanitized_topic = sanitize_topic_name(topic_name);
            let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
            let cached_replay_id = cache.get(&cache_key).await.ok();

            match cached_replay_id.flatten() {
                Some(reply_id) => {
                    fetch_request.replay_id = reply_id.into();
                    fetch_request.replay_preset = 2;
                }
                None => {
                    fetch_request.replay_preset = durable_consumer_opts.replay_preset.to_i32();
                    warn!(
                        "No cached replay_id found for flow {flow_name} topic {topic_name}, starting from {:?}",
                        &durable_consumer_opts.replay_preset
                    );
                }
            }
        }

        let mut stream = match self
            .pubsub
            .lock()
            .await
            .subscribe(fetch_request.clone())
            .await
        {
            Ok(response) => response.into_inner(),
            Err(e) => {
                // Check if reconnect is needed (gRPC auth errors).
                if is_auth_error(&e) {
                    // Reconnect to get fresh OAuth token.
                    {
                        let mut pubsub = self.pubsub.lock().await;
                        pubsub
                            .reconnect()
                            .await
                            .map_err(|e| Error::PubSub { source: e })?;
                    }

                    // Retry subscribe with new credentials.
                    match self
                        .pubsub
                        .lock()
                        .await
                        .subscribe(fetch_request.clone())
                        .await
                    {
                        Ok(response) => response.into_inner(),
                        Err(e) => {
                            // If it still fails, return error to trigger full retry.
                            return Err(Error::PubSub { source: e });
                        }
                    }
                } else if !fetch_request.replay_id.is_empty() && is_replay_id_error(&e) {
                    // If subscribe fails due to invalid replay_id, clear it from cache and let retry handle it.
                    warn!("Invalid replay_id detected, clearing from cache and retrying");

                    // Clear the invalid replay_id from cache so next retry starts fresh.
                    if self
                        .config
                        .topic
                        .durable_consumer_options
                        .as_ref()
                        .is_some_and(|opts| opts.enabled && !opts.managed_subscription)
                    {
                        let sanitized_topic = sanitize_topic_name(topic_name);
                        let cache_key = format!("flow.{flow_key}.replay_id.{sanitized_topic}");
                        if let Err(e) = cache.delete(&cache_key).await {
                            error!(
                                "Failed to delete invalid replay_id from cache for flow {} topic {}: {}",
                                flow_name, topic_name, e
                            );
                        }
                    }

                    // Return error to trigger retry with fresh connection.
                    return Err(Error::PubSub { source: e });
                } else {
                    // Return error to trigger retry with fresh connection.
                    return Err(Error::PubSub { source: e });
                }
            }
        };

        while let Some(event) = stream.next().await {
            if self.task_context.cancellation_token.is_cancelled() {
                drop(stream);
                return Ok(());
            }

            let events = match event {
                Ok(fr) => fr.events,
                Err(e) => {
                    return Err(Error::PubSub {
                        source: PubSubError::Tonic(Box::new(e)),
                    });
                }
            };

            self.process_events(events, &schema_info, topic_name)
                .await?;

            if self.task_context.cancellation_token.is_cancelled() {
                drop(stream);
                return Ok(());
            }
        }

        Err(Error::StreamEnded)
    }
}

/// Manages multiple Salesforce Pub/Sub topic subscriptions.
///
/// Creates TopicListener instances for each configured topic,
/// handling authentication and connection setup.
#[derive(Debug)]
pub struct Subscriber {
    /// Configuration for topics, credentials, and consumer options
    config: Arc<super::config::Subscriber>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the subscriber by establishing connections and authentication.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Creating gRPC service connection
    /// - Authenticating with Salesforce
    /// - Building Pub/Sub context
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        // Determine Pub/Sub endpoint.
        let endpoint = match &init_config.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ),
        };

        // One gRPC channel per endpoint, shared through the client registry.
        // `Channel` is a cheap handle over a connection pool, so clones
        // multiplex onto the same HTTP/2 connection instead of each
        // subscription opening its own — several subscriptions against one
        // org otherwise hold several TLS connections open, each running its
        // own keepalive whether or not events are flowing.
        let channel_key = flowgen_core::client_registry::ClientKeyBuilder::new("salesforce_pubsub")
            .field("grpc_endpoint", &endpoint)
            .build();
        let channel = self
            .task_context
            .client_registry
            .get_or_init(channel_key.clone(), || async {
                let service = flowgen_core::service::ServiceBuilder::new()
                    .endpoint(endpoint)
                    .build()
                    .map_err(|e| Error::Service { source: e })?
                    .connect()
                    .await
                    .map_err(|e| Error::Service { source: e })?;
                service.channel.ok_or_else(|| Error::Service {
                    source: flowgen_core::service::Error::MissingEndpoint(),
                })
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;
        let channel = (*channel).clone();

        // Authenticate with Salesforce (shared via client registry).
        let credentials_path = init_config.credentials_path.clone();
        let client_key =
            flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path);
        let sfdc_client = self
            .task_context
            .client_registry
            .get_or_init(client_key.clone(), || async {
                let client = salesforce_core::client::Builder::new()
                    .credentials_path(credentials_path)
                    .build()
                    .map_err(|e| Error::Auth { source: e })?
                    .connect()
                    .await
                    .map_err(|e| Error::Auth { source: e })?;
                Ok(tokio::sync::Mutex::new(client))
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        // Create Pub/Sub context.
        let pubsub = {
            let guard = sfdc_client.lock().await;
            salesforce_core::pubsubapi::Client::new(channel, guard.clone())
                .map_err(|e| Error::PubSub { source: e })?
        };
        let pubsub = Arc::new(Mutex::new(pubsub));

        // Create event handler.
        Ok(EventHandler {
            client_key,
            channel_key,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            pubsub,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();
        let mut reconnect_backoff = retry_config.reconnect_strategy();

        let mut lost_connectivity = false;

        // Infinite retry loop: subscribers must maintain connectivity indefinitely.
        loop {
            if cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Initialize with circuit breaker to detect permanent errors (bad credentials, etc.).
            let init_future = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    match self.init().await {
                        Ok(handler) => Ok(handler),
                        Err(e) => {
                            error!(error = %e, "Subscriber initialization failed");
                            Err(tokio_retry::RetryError::transient(e))
                        }
                    }
                },
            );

            let event_handler = tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = init_future => match result {
                    Ok(handler) => {
                        reconnect_backoff = retry_config.reconnect_strategy();
                        if lost_connectivity {
                            warn!("Subscriber reconnected successfully");
                        }
                        handler
                    }
                    Err(e) => {
                        let delay = match reconnect_backoff.next() {
                            Some(d) => d,
                            None => retry_config.initial_backoff,
                        };
                        error!(error = %e, delay_ms = %delay.as_millis(), "Subscriber initialization exhausted retry attempts, will retry after backoff");
                        tokio::select! {
                            _ = tokio::time::sleep(delay) => {}
                            _ = cancellation_token.cancelled() => return Ok(()),
                        }
                        continue;
                    }
                },
            };

            // Run event loop until failure, then reinitialize.
            let client_key = event_handler.client_key.clone();
            let channel_key = event_handler.channel_key.clone();
            let handle_future = event_handler.handle();
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = handle_future => match result {
                    Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                    Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
                },
            }
            lost_connectivity = true;

            self.task_context.client_registry.remove(&client_key).await;
            self.task_context.client_registry.remove(&channel_key).await;

            let delay = match reconnect_backoff.next() {
                Some(d) => d,
                None => retry_config.initial_backoff,
            };
            warn!(delay_ms = %delay.as_millis(), "Reconnect backoff");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = cancellation_token.cancelled() => return Ok(()),
            }
        }
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Subscriber configuration
    config: Option<Arc<super::config::Subscriber>>,
    /// Event channel sender
    tx: Option<Sender<Event>>,
    /// Task identifier
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl SubscriberBuilder {
    /// Creates a new builder instance.
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    /// Sets the subscriber configuration.
    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the event channel sender.
    pub fn sender(mut self, sender: Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    /// Sets the current task ID.
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    /// Sets the task execution context.
    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    /// Sets the task type.
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    /// Builds the Subscriber instance.
    pub async fn build(self) -> Result<Subscriber, Error> {
        Ok(Subscriber {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsubapi::config;
    use serde_json::{Map, Value};
    use std::path::PathBuf;
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_sanitize_topic_name() {
        // Test with leading slash.
        assert_eq!(sanitize_topic_name("/event/Test__e"), "event-Test__e");

        // Test with leading and trailing slashes.
        assert_eq!(sanitize_topic_name("/data/MyData/"), "data-MyData");

        // Test with multiple slashes.
        assert_eq!(
            sanitize_topic_name("/event/BulkApi2JobEvent"),
            "event-BulkApi2JobEvent"
        );

        // Test without slashes.
        assert_eq!(sanitize_topic_name("simple"), "simple");

        // Test with only slashes.
        assert_eq!(sanitize_topic_name("///"), "");
    }

    #[tokio::test]
    async fn test_subscriber_builder() {
        let config = Arc::new(config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: config::Topic {
                name: "/event/Test__e".to_string(),
                durable_consumer_options: None,
                num_requested: Some(10),
            },
            endpoint: None,
            ack_timeout: None,
            depends_on: None,
            retry: None,
        });
        let (tx, _) = mpsc::channel::<Event>(10);

        // Success case.
        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel::<Event>(10);
        let result = SubscriberBuilder::new()
            .sender(tx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
