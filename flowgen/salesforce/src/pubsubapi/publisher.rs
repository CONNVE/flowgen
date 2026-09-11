use apache_avro::{types::Value as AvroValue, Schema as AvroSchema};
use chrono::Utc;
use flowgen_core::client::Client;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventData, EventExt};
use flowgen_core::task::runner::Runner;
use futures_util::future;
use salesforce_core::pubsubapi::{
    ProducerEvent, PubSubError, PublishRequest, SchemaRequest, TopicRequest,
};
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, Mutex};
use tracing::{error, warn, Instrument};

/// Checks if a gRPC error is due to invalid authentication.
fn is_auth_error(error: &PubSubError) -> bool {
    if let PubSubError::Tonic(status) = error {
        let message = status.message();
        return message.contains("does not have valid authentication credentials")
            || message.contains("authentication exception occurred");
    }
    false
}

/// Errors that can occur during Salesforce Pub/Sub publishing operations.
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
    #[error("Serialization error: {source}")]
    SerdeExt {
        #[source]
        source: flowgen_core::serde::Error,
    },
    #[error("Avro operation error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("Render error: {source}")]
    Render {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Send event message error: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error(transparent)]
    Event(#[from] flowgen_core::event::Error),
    #[error(transparent)]
    ConfigRender(#[from] flowgen_core::config::Error),
    #[error("Service error: {source}")]
    Service {
        #[source]
        source: flowgen_core::service::Error,
    },
    #[error("Missing required attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Empty object")]
    EmptyObject(),
    #[error("Error parsing Schema JSON string to Schema type")]
    SchemaParse(),
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing and publishing events to Salesforce Pub/Sub.
pub struct EventHandler {
    /// Publisher configuration.
    config: Arc<super::config::Publisher>,
    /// Pub/Sub connection context.
    pubsub: Arc<Mutex<salesforce_core::pubsubapi::Client>>,
    /// Registry keys for the cached gRPC channel and Salesforce client, so a
    /// rebuild after a failed publish evicts the entries that just went stale
    /// instead of handing the replacement the same dead connection.
    channel_key: flowgen_core::client_registry::ClientKey,
    client_key: flowgen_core::client_registry::ClientKey,
    /// Topic name for publishing.
    topic: String,
    /// Schema ID for event serialization.
    schema_id: String,
    /// Avro serializer configuration.
    schema: Arc<AvroSchema>,
    /// Current task identifier.
    task_id: usize,
    /// Channel sender for response events.
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl EventHandler {
    /// Processes an event by publishing it to Salesforce Pub/Sub.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())?;
            let config = self.config.render(&event_value)?;

            let mut publish_payload = match &config.payload {
                super::config::Payload::FromEvent { from_event } if *from_event => {
                    // Use incoming event data as the payload.
                    let event_data = event.data_as_json()?;
                    match event_data {
                        serde_json::Value::Object(map) => map,
                        _ => return Err(Error::EmptyObject()),
                    }
                }
                super::config::Payload::FromEvent { .. } => {
                    return Err(Error::EmptyObject());
                }
                super::config::Payload::Fields(fields) => fields.clone(),
            };

            let now = Utc::now().timestamp_millis();
            publish_payload.insert(
                "CreatedDate".to_string(),
                serde_json::Value::Number(serde_json::Number::from(now)),
            );

            // Convert serde_json::Map to Avro Record using From<serde_json::Value> trait.
            let json_value = serde_json::Value::Object(publish_payload);
            let record = AvroValue::from(json_value)
                .resolve(self.schema.as_ref())
                .map_err(|e| Error::Avro { source: e })?;

            // Serialize the record directly without schema wrapper (Salesforce expects just the data).
            let serialized_payload = apache_avro::to_avro_datum(self.schema.as_ref(), record)
                .map_err(|e| Error::Avro { source: e })?;

            let mut events = Vec::new();
            let pe = ProducerEvent {
                schema_id: self.schema_id.clone(),
                payload: serialized_payload,
                ..Default::default()
            };
            events.push(pe);

            let resp = self
                .pubsub
                .lock()
                .await
                .publish(PublishRequest {
                    topic_name: self.topic.clone(),
                    events,
                    ..Default::default()
                })
                .await
                .map_err(|e| Error::PubSub { source: e })?
                .into_inner();

            // Generate subject prefix from topic name.
            let subject = if let Some(stripped) = self
                .topic
                .replace('/', ".")
                .to_lowercase()
                .strip_prefix('.')
            {
                stripped.to_string()
            } else {
                self.topic.to_owned()
            };

            let resp_json =
                serde_json::to_value(&resp).map_err(|e| Error::SerdeJson { source: e })?;

            let mut e = flowgen_core::event::EventBuilder::new()
                .data(EventData::Json(resp_json))
                .subject(subject)
                .id(resp.rpc_id)
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    // Pass through completion_tx to next task.
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// Salesforce Pub/Sub publisher that receives events and publishes them to configured topics.
#[derive(Debug)]
pub struct Publisher {
    /// Publisher configuration including topic settings and credentials.
    config: Arc<super::config::Publisher>,
    /// Receiver for incoming events to publish.
    rx: Receiver<Event>,
    /// Channel sender for response events.
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

impl Publisher {
    /// Builds an `EventHandler`, retrying on failure with the init strategy.
    /// Used both for the initial connection and to rebuild it after a publish
    /// exhausts its retries.
    async fn init_retrying(
        &self,
        retry_config: &flowgen_core::retry::RetryConfig,
    ) -> Result<Arc<EventHandler>, Error> {
        tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize publisher");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        )
        .await
        .map(Arc::new)
    }
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Publisher {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the publisher by establishing connection and retrieving schema.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to Salesforce Pub/Sub service
    /// - Authenticating with credentials
    /// - Retrieving topic information and schema
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self.config.render(&serde_json::json!({}))?;

        let endpoint = match &init_config.endpoint {
            Some(endpoint) => endpoint.clone(),
            None => format!(
                "{}:{}",
                super::config::DEFAULT_PUBSUB_URL,
                super::config::DEFAULT_PUBSUB_PORT
            ),
        };

        // Shared per endpoint, so a publisher and subscriber pointed at the
        // same org multiplex over one HTTP/2 connection rather than holding
        // one each, with a keepalive on each.
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

        let pubsub = {
            let guard = sfdc_client.lock().await;
            salesforce_core::pubsubapi::Client::new(channel, guard.clone())
                .map_err(|e| Error::PubSub { source: e })?
        };

        let pubsub = Arc::new(Mutex::new(pubsub));

        let topic_info = pubsub
            .lock()
            .await
            .get_topic(TopicRequest {
                topic_name: init_config.topic.clone(),
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        let schema_info = pubsub
            .lock()
            .await
            .get_schema(SchemaRequest {
                schema_id: topic_info.schema_id,
            })
            .await
            .map_err(|e| Error::PubSub { source: e })?
            .into_inner();

        let schema = AvroSchema::parse_str(&schema_info.schema_json)
            .map_err(|e| Error::Avro { source: e })?;

        let event_handler = EventHandler {
            config: Arc::clone(&self.config),
            pubsub,
            channel_key,
            client_key,
            topic: init_config.topic.to_owned(),
            schema_id: schema_info.schema_id,
            schema: Arc::new(schema),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        // The loop holds the receiver mutably while `init_retrying` borrows
        // the rest of `self` to rebuild, so it moves out here. The channel
        // left behind is never read.
        let mut rx = std::mem::replace(&mut self.rx, tokio::sync::mpsc::channel(1).1);

        let mut event_handler = self.init_retrying(&retry_config).await?;

        // Set by a publish that exhausted its retries. The gRPC channel is
        // established once at init and never repaired in place, so a dropped
        // connection would otherwise leave this task publishing into a dead
        // transport until the pod restarts. Rebuilding before the next event
        // reconnects instead — the same "reconnect on event-loop failure"
        // the subscriber already does.
        let needs_reinit = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let mut handlers = Vec::new();

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                future::join_all(handlers).await;
                return Ok(());
            }

            match rx.recv().await {
                Some(event) => {
                    // Only events this task will actually publish are worth a
                    // rebuild; one addressed to a different task is dropped by
                    // the check below and must not trigger a reconnect.
                    let ours = Some(event.task_id) == event_handler.task_id.checked_sub(1);

                    if ours && needs_reinit.swap(false, std::sync::atomic::Ordering::AcqRel) {
                        warn!("Rebuilding publisher connection after failed publish");
                        self.task_context
                            .client_registry
                            .remove(&event_handler.channel_key)
                            .await;
                        self.task_context
                            .client_registry
                            .remove(&event_handler.client_key)
                            .await;
                        let rebuilt = tokio::select! {
                            _ = self.task_context.cancellation_token.cancelled() => {
                                future::join_all(handlers).await;
                                return Ok(());
                            }
                            result = self.init_retrying(&retry_config) => result,
                        };
                        match rebuilt {
                            Ok(handler) => event_handler = handler,
                            Err(e) => {
                                // Keep the old handler and let the next event
                                // try again: a publisher that gives up here
                                // would stay dead for the pod's lifetime,
                                // which is the failure this rebuild exists
                                // to prevent.
                                error!(error = %e, "Failed to rebuild publisher connection, will retry on next event");
                                needs_reinit.store(true, std::sync::atomic::Ordering::Release);
                            }
                        }
                    }

                    if ours {
                        let event_handler = Arc::clone(&event_handler);
                        let needs_reinit = Arc::clone(&needs_reinit);
                        let retry_strategy = retry_config.strategy();
                        let handle = tokio::spawn(
                            async move {
                                let result = tokio_retry::Retry::spawn(retry_strategy, || async {
                                    match event_handler.handle(event.clone()).await {
                                        Ok(result) => Ok(result),
                                        Err(e) => {
                                            error!(error = %e, "Failed to publish message");
                                            // Check if reconnect is needed (gRPC auth errors).
                                            if let Error::PubSub { ref source } = e {
                                                if is_auth_error(source) {
                                                    let mut pubsub =
                                                        event_handler.pubsub.lock().await;
                                                    if let Err(reconnect_err) =
                                                        pubsub.reconnect().await
                                                    {
                                                        return Err(tokio_retry::RetryError::transient(Error::PubSub {
                                                            source: reconnect_err,
                                                        }));
                                                    }
                                                }
                                            }
                                            let retryable = match &e {
                                                Error::PubSub { source } => source.is_retryable(),
                                                _ => true,
                                            };
                                            if retryable {
                                                Err(tokio_retry::RetryError::transient(e))
                                            } else {
                                                Err(tokio_retry::RetryError::permanent(e))
                                            }
                                        }
                                    }
                                })
                                .await;

                                if let Err(err) = result {
                                    error!(error = %err, "Failed to publish message after all retry attempts");
                                    needs_reinit.store(true, std::sync::atomic::Ordering::Release);
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
                }
                None => {
                    // Channel closed, wait for all spawned handlers to complete.
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Default)]
pub struct PublisherBuilder {
    config: Option<Arc<super::config::Publisher>>,
    rx: Option<Receiver<Event>>,
    tx: Option<tokio::sync::mpsc::Sender<Event>>,
    task_id: usize,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl PublisherBuilder {
    pub fn new() -> PublisherBuilder {
        PublisherBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Publisher>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
        self
    }

    pub fn sender(mut self, sender: tokio::sync::mpsc::Sender<Event>) -> Self {
        self.tx = Some(sender);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = task_id;
        self
    }

    pub fn task_context(
        mut self,
        task_context: Arc<flowgen_core::task::context::TaskContext>,
    ) -> Self {
        self.task_context = Some(task_context);
        self
    }

    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }

    pub async fn build(self) -> Result<Publisher, Error> {
        Ok(Publisher {
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

    #[tokio::test]
    async fn test_publisher_builder() {
        let config = Arc::new(config::Publisher {
            name: "test_publisher".to_string(),
            credentials_path: PathBuf::from("test_creds"),
            topic: "/event/Test__e".to_string(),
            payload: config::Payload::Fields(serde_json::Map::new()),
            endpoint: None,
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel::<Event>(10);

        // Success case.
        let publisher = PublisherBuilder::new()
            .config(config.clone())
            .receiver(rx)
            .sender(tx.clone())
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(publisher.is_ok());

        // Error case - missing config.
        let (_tx2, rx2) = mpsc::channel::<Event>(10);
        let result = PublisherBuilder::new()
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
