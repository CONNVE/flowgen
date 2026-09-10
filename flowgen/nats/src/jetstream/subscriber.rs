use super::message::NatsMessageExt;
use async_nats::jetstream::{self};
use flowgen_core::{
    client::Client,
    config::ConfigExt,
    event::{new_completion_channel, Event, EventExt},
};
use std::sync::Arc;
use tokio::pin;
use tokio::sync::mpsc::Sender;
use tokio_stream::StreamExt;
use tracing::{error, warn};

/// Errors that can occur during NATS JetStream subscription operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("NATS client error: {source}")]
    Client {
        #[source]
        source: crate::client::Error,
    },
    #[error("Message conversion error: {source}")]
    MessageConversion {
        #[source]
        source: crate::jetstream::message::Error,
    },
    #[error("Consumer creation error: {source}")]
    Consumer {
        #[source]
        source: async_nats::jetstream::stream::ConsumerError,
    },
    #[error("Consumer stream error: {source}")]
    ConsumerStream {
        #[source]
        source: async_nats::jetstream::consumer::StreamError,
    },
    #[error("Stream management error: {source}")]
    StreamManagement {
        #[source]
        source: super::stream::Error,
    },
    #[error("Error getting stream: {source}")]
    GetStream {
        #[source]
        source: async_nats::jetstream::context::GetStreamError,
    },
    #[error("Subscription error: {source}")]
    Subscribe {
        #[source]
        source: async_nats::SubscribeError,
    },
    #[error("Consumer configuration check error")]
    ConsumerInfoFailed,
    #[error("Consumer '{consumer}' exists with different filter subject '{existing}', expected '{expected}'. Please delete the existing consumer or use a different durable name")]
    ConsumerFilterMismatch {
        consumer: String,
        existing: String,
        expected: String,
    },
    #[error("Missing stream configuration")]
    MissingStreamConfig,
    #[error("Durable name is required for JetStream consumers")]
    MissingDurableName,
    #[error("Other subscriber error")]
    Other(#[source] Box<dyn std::error::Error + Send + Sync>),
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
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
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing NATS messages.
pub struct EventHandler {
    consumer: jetstream::consumer::Consumer<jetstream::consumer::pull::Config>,
    /// Checked via `connection_state()` to distinguish a dead connection
    /// from a live consumer with nothing to deliver.
    client: async_nats::Client,
    /// Same key `init()` used to cache the NATS client, so `run()` can evict
    /// the exact entry that may be stale rather than recomputing it (and
    /// risking drift if `credentials_path`/`url` are templated).
    client_key: flowgen_core::client_registry::ClientKey,
    tx: Option<Sender<Event>>,
    task_id: usize,
    config: Arc<super::config::Subscriber>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes a single message result. Wrapped in a `task.handle`
    /// span so the admin UI's activity feed reports per-message
    /// duration, matching how processors expose it.
    #[tracing::instrument(
        skip(self, message_result),
        name = "task.handle",
        fields(duration_ms = tracing::field::Empty)
    )]
    async fn process_message(
        &self,
        message_result: Result<
            async_nats::jetstream::Message,
            async_nats::error::Error<async_nats::jetstream::consumer::pull::MessagesErrorKind>,
        >,
    ) -> Result<(), Error> {
        match message_result {
            Ok(message) => {
                // Size the completion channel to the number of leaves in this
                // flow's directed acyclic graph. The NATS message is acked
                // only after every leaf has signalled completion.
                let (completion_state, completion_rx) =
                    new_completion_channel(self.task_context.leaf_count);

                let mut e = message
                    .to_event(self.task_type, self.task_id)
                    .map_err(|source| Error::MessageConversion { source })?;

                e.completion_tx = Some(completion_state);

                e.send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;

                // Wait for flow completion with configured ack_timeout.
                // Failed flows skip message acknowledgment, allowing JetStream to automatically redeliver.
                match self.config.ack_timeout {
                    Some(timeout) => match tokio::time::timeout(timeout, completion_rx).await {
                        Ok(Ok(Ok(_))) => {
                            message.ack().await.ok();
                        }
                        Ok(Ok(Err(_))) | Ok(Err(_)) | Err(_) => {
                            warn!("Flow completion failed or timed out");
                        }
                    },
                    None => match completion_rx.await {
                        Ok(Ok(_)) => {
                            message.ack().await.ok();
                        }
                        Ok(Err(_)) | Err(_) => {
                            warn!("Flow completion failed or timed out");
                        }
                    },
                }

                Ok(())
            }
            Err(err) => Err(Error::Other(Box::new(err))),
        }
    }

    /// Processes messages from the NATS JetStream consumer.
    ///
    /// Uses a continuous pull stream rather than looping over `fetch()` calls.
    /// `fetch()` returns immediately when no messages are available, which
    /// caused empty requests to spin at hundreds of round-trips per second on
    /// an idle consumer. The stream keeps a batch request outstanding with a
    /// server-side expiry, so the consumer waits on NATS instead of polling.
    async fn handle(self) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let mut messages = self
            .consumer
            .stream()
            .max_messages_per_batch(self.config.max_messages_per_batch)
            .expires(self.config.batch_expires)
            .messages()
            .await
            .map_err(|e| Error::ConsumerStream { source: e })?;

        match self.config.throttle {
            Some(throttle_duration) => {
                let throttled = messages.throttle(throttle_duration);
                pin!(throttled);
                while let Some(message_result) = throttled.next().await {
                    if self.task_context.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
                    self.process_message(message_result).await?;
                }
            }
            None => {
                while let Some(message_result) = messages.next().await {
                    if self.task_context.cancellation_token.is_cancelled() {
                        return Ok(());
                    }
                    self.process_message(message_result).await?;
                }
            }
        }

        // The continuous stream only ends on an error or a lost connection.
        if self.client.connection_state() == async_nats::connection::State::Disconnected {
            return Err(Error::StreamEnded);
        }

        Ok(())
    }
}

/// NATS JetStream subscriber that consumes messages and converts them to flowgen events.
#[derive(Debug)]
pub struct Subscriber {
    /// Subscriber configuration including stream and consumer settings.
    config: Arc<super::config::Subscriber>,
    /// Sender for forwarding converted events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tagging.
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

    /// Initializes the subscriber by establishing connection and creating consumer.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Connecting to NATS with credentials
    /// - Getting or creating JetStream stream and consumer
    /// - Validating consumer configuration
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let nats_key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("credentials_path", &init_config.credentials_path)
            .field("url", &init_config.url)
            .build();
        let jetstream_ctx = self
            .task_context
            .client_registry
            .get_or_init(nats_key.clone(), || {
                let credentials_path = init_config.credentials_path.clone();
                let url = init_config.url.clone();
                async move {
                    let mut builder = crate::client::ClientBuilder::new();
                    builder.url(url);
                    if let Some(path) = credentials_path {
                        builder.credentials_path(path);
                    }
                    let client = builder
                        .build()
                        .map_err(|source| Error::Client { source })?
                        .connect()
                        .await
                        .map_err(|source| Error::Client { source })?;
                    client.jetstream.ok_or_else(|| {
                        Error::Other("JetStream context not available".to_string().into())
                    })
                }
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        {
            let mut jetstream = (*jetstream_ctx).clone();
            let stream_opts = self
                .config
                .stream
                .as_ref()
                .ok_or_else(|| Error::MissingStreamConfig)?;

            // Set timeout on JetStream context if configured.
            if let Some(timeout) = init_config.ack_timeout {
                jetstream.set_timeout(timeout);
            }

            let jetstream = match stream_opts.create_or_update {
                true => super::stream::create_or_update_stream(jetstream, stream_opts)
                    .await
                    .map_err(|source| Error::StreamManagement { source })?,
                false => jetstream,
            };

            let stream = jetstream
                .get_stream(&stream_opts.name)
                .await
                .map_err(|source| Error::GetStream { source })?;

            let durable_name = init_config
                .durable_name
                .as_ref()
                .ok_or_else(|| Error::MissingDurableName)?;

            let mut consumer_config = jetstream::consumer::pull::Config {
                durable_name: Some(durable_name.clone()),
                filter_subject: init_config.subject.clone(),
                ..Default::default()
            };

            if let Some(ack_timeout) = init_config.ack_timeout {
                consumer_config.ack_wait = ack_timeout;
            }

            if let Some(max_ack_pending) = init_config.max_ack_pending {
                consumer_config.max_ack_pending = max_ack_pending;
            }

            if let Some(max_waiting) = init_config.max_waiting {
                consumer_config.max_waiting = max_waiting;
            }

            if let Some(max_deliver) = init_config.max_deliver {
                consumer_config.max_deliver = max_deliver;
            }

            if !init_config.backoff.is_empty() {
                consumer_config.backoff = init_config.backoff.clone();
            }

            let consumer = match stream.get_consumer(durable_name).await {
                Ok(mut existing_consumer) => {
                    let consumer_info = existing_consumer
                        .info()
                        .await
                        .map_err(|_| Error::ConsumerInfoFailed)?;
                    let current_filter = consumer_info.config.filter_subject.clone();

                    if current_filter != init_config.subject {
                        return Err(Error::ConsumerFilterMismatch {
                            consumer: durable_name.clone(),
                            existing: current_filter,
                            expected: init_config.subject.clone(),
                        });
                    } else {
                        existing_consumer
                    }
                }
                Err(_) => stream
                    .create_consumer(consumer_config)
                    .await
                    .map_err(|e| Error::Consumer { source: e })?,
            };

            Ok(EventHandler {
                client: jetstream_ctx.client(),
                client_key: nats_key,
                consumer,
                tx: self.tx.clone(),
                task_id: self.task_id,
                config: Arc::clone(&self.config),
                task_type: self.task_type,
                task_context: Arc::clone(&self.task_context),
            })
        }
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

            // Initialize with circuit breaker to detect permanent errors (bad config, filter mismatch, etc.).
            let init_future = tokio_retry::Retry::spawn(
                retry_config.init_strategy(self.task_context.startup_delay),
                || async {
                    match self.init().await {
                        Ok(handler) => Ok(handler),
                        Err(e) => {
                            let is_retriable = !matches!(&e, Error::ConsumerFilterMismatch { .. });

                            if is_retriable {
                                error!(error = %e, "Subscriber initialization failed");
                                Err(tokio_retry::RetryError::transient(e))
                            } else {
                                error!(error = %e, "Permanent initialization error");
                                Err(tokio_retry::RetryError::permanent(e))
                            }
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
            let handle_future = event_handler.handle();
            tokio::select! {
                _ = cancellation_token.cancelled() => return Ok(()),
                result = handle_future => match result {
                    Ok(()) => warn!("Subscriber lost connectivity, reinitializing"),
                    Err(e) => error!(error = %e, "Subscriber lost connectivity, reinitializing"),
                },
            }
            lost_connectivity = true;

            // Evict the cached NATS client so the next init() creates a fresh connection
            // instead of reusing the dead one from the registry.
            self.task_context.client_registry.remove(&client_key).await;

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

/// Builder for configuring and creating NATS JetStream subscribers.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Optional subscriber configuration.
    config: Option<Arc<super::config::Subscriber>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Task identifier for event processing.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization.
    task_type: Option<&'static str>,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Subscriber>) -> Self {
        self.config = Some(config);
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
    use serde_json::{Map, Value};
    use std::{path::PathBuf, time::Duration};
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
    async fn test_subscriber_builder() {
        let config = Arc::new(super::super::config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.jwt")),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages_per_batch: 100,
            delay: Some(Duration::from_secs(5)),
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        // Success case.
        let subscriber = SubscriberBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .task_id(1)
            .task_type("test_task")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(subscriber.is_ok());

        // Error case - missing config.
        let (tx2, _rx2) = mpsc::channel(100);
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

    #[tokio::test]
    async fn test_subscriber_builder_build_missing_task_context() {
        let config = Arc::new(super::super::config::Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.jwt")),
            subject: "test.subject".to_string(),
            stream: Some(super::super::config::StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(super::super::config::RetentionPolicy::Limits),
                discard: Some(super::super::config::DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages_per_batch: 50,
            delay: None,
            throttle: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

        let result = SubscriberBuilder::new()
            .config(config)
            .sender(tx)
            .task_id(1)
            .build()
            .await;

        assert!(result.is_err());
        assert!(
            matches!(result.unwrap_err(), Error::MissingBuilderAttribute(attr) if attr == "task_context")
        );
    }
}
