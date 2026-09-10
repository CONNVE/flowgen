//! Event generation subscriber for producing scheduled synthetic events.
//!
//! Implements a timer-based event generator that creates events at regular intervals
//! with optional structured data payloads and count limits for testing and simulation workflows.

use crate::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use chrono::DateTime;
use croner::Cron;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{sync::mpsc::Sender, time};
use tracing::{error, warn};

/// System information included in generated events for time-based filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemInfo {
    /// Last run time in seconds since UNIX epoch (if available from cache).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_run_time: Option<u64>,
    /// Next scheduled run time in seconds since UNIX epoch (if available).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_run_time: Option<u64>,
}

/// Errors that can occur during generate task execution.
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
    #[error("Cache error: {_0}")]
    Cache(String),
    #[error("System time error: {source}")]
    SystemTime {
        #[source]
        source: std::time::SystemTimeError,
    },
    #[error("Invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Invalid cron expression '{expression}': {source}")]
    InvalidCron {
        expression: String,
        #[source]
        source: croner::errors::CronError,
    },
    #[error("Cron schedule has no next occurrence")]
    CronNoNextOccurrence,
    #[error("Invalid timezone '{0}'")]
    InvalidTimezone(String),
    #[error("Configuration validation error: {source}")]
    ConfigValidation {
        #[source]
        source: crate::task::generate::config::ConfigError,
    },
}
/// Event handler for generating scheduled events.
pub struct EventHandler {
    config: Arc<crate::task::generate::config::Subscriber>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<crate::task::context::TaskContext>,
    task_type: &'static str,
}

impl EventHandler {
    /// Calculate next run time based on interval, cron schedule, or run-once mode.
    fn calculate_next_run(&self, now: u64, last_run: Option<u64>) -> Result<u64, Error> {
        match (&self.config.interval, &self.config.cron) {
            // Use interval if it's configured.
            (Some(interval), _) => Ok(last_run
                .map(|t| t + interval.as_secs())
                .unwrap_or_else(|| now + interval.as_secs())),
            // Use cron expression if it's configured.
            (None, Some(cron_expr)) => {
                let cron = Cron::from_str(cron_expr).map_err(|e| Error::InvalidCron {
                    expression: cron_expr.clone(),
                    source: e,
                })?;

                let utc_dt = DateTime::from_timestamp(now as i64, 0)
                    .ok_or_else(|| Error::InvalidTimestamp(now as i64))?;

                // Convert to configured timezone for cron evaluation, or use UTC.
                let next_timestamp = if let Some(ref tz_str) = self.config.timezone {
                    let tz: chrono_tz::Tz = tz_str
                        .parse()
                        .map_err(|_| Error::InvalidTimezone(tz_str.clone()))?;
                    let local_dt = utc_dt.with_timezone(&tz);
                    let next = cron.find_next_occurrence(&local_dt, false).map_err(|e| {
                        Error::InvalidCron {
                            expression: cron_expr.clone(),
                            source: e,
                        }
                    })?;
                    next.timestamp() as u64
                } else {
                    let next = cron.find_next_occurrence(&utc_dt, false).map_err(|e| {
                        Error::InvalidCron {
                            expression: cron_expr.clone(),
                            source: e,
                        }
                    })?;
                    next.timestamp() as u64
                };

                Ok(next_timestamp)
            }
            // Neither interval nor cron - run immediately (run-once mode).
            // This is allowed when count is specified.
            (None, None) => Ok(now),
        }
    }

    /// Generates events at scheduled intervals.
    async fn run_loop(&self) -> Result<(), Error> {
        // Note: This generator creates events from scratch (no incoming events),
        // so we don't use with_event_context() here. EventBuilder::new() will create fresh events.
        // create events with meta: None, which is correct for a pipeline starter.

        // Get cache from task context.
        let cache = &self.task_context.cache;
        let flow_key = self.task_context.flow.id();
        let task_name = &self.config.name;

        // Generate cache keys with flow-scoped namespace.
        let cache_key = format!("flow.{flow_key}.last_run.{task_name}");
        let counter_cache_key = format!("flow.{flow_key}.counter.{task_name}");

        // Restore counter from cache to resume after restart.
        // When allow_rerun is enabled, always start from zero.
        let mut counter: u64 = match self.config.allow_rerun {
            true => 0,
            false => cache
                .get(&counter_cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| String::from_utf8_lossy(&bytes).parse::<u64>().ok())
                .unwrap_or(0),
        };

        // If count is already reached, skip re-running.
        if let Some(count) = self.config.count {
            if counter >= count {
                return Ok(());
            }
        }

        loop {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }

            // Calculate now timestamp.
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();

            // Get last_run from cache or return none.
            let last_run = cache
                .get(&cache_key)
                .await
                .ok()
                .flatten()
                .and_then(|bytes| String::from_utf8_lossy(&bytes).parse::<u64>().ok());

            // Calculate next run time for interval or cron.
            let next_run_time = self.calculate_next_run(now, last_run)?;

            // Sleep until it's time to generate the next event.
            if next_run_time > now {
                let sleep_duration = next_run_time - now;
                tokio::select! {
                    _ = time::sleep(Duration::from_secs(sleep_duration)) => {}
                    _ = self.task_context.cancellation_token.cancelled() => return Ok(()),
                }
            }

            // Get current time after sleeping (actual execution time).
            let current_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|e| Error::SystemTime { source: e })?
                .as_secs();

            // Determine if there will be a next run.
            let next_run_time_val = match self.config.count {
                Some(count) if count == counter + 1 => None,
                _ => Some(self.calculate_next_run(current_time, Some(current_time))?),
            };

            // Create system information.
            // Use current execution time as last_run_time (this is the time of the current run).
            // Fall back to cached last_run only if current_time is unavailable.
            let system_info = SystemInfo {
                last_run_time: Some(current_time),
                next_run_time: next_run_time_val,
            };

            // Prepare event data with system information and optional user-defined payload.
            let mut data = match &self.config.payload {
                Some(user_data) => user_data.clone(),
                None => json!({}),
            };
            if let Some(obj) = data.as_object_mut() {
                obj.insert(
                    "system_info".to_string(),
                    serde_json::to_value(&system_info).unwrap_or(serde_json::Value::Null),
                );
            }

            // Create a completion channel sized to the number of leaves in
            // this flow's directed acyclic graph. The source acks the
            // generated event only after every leaf has signalled.
            let (completion_state, completion_rx) =
                new_completion_channel(self.task_context.leaf_count);

            self.handle(data, completion_state).await?;

            // Wait for flow completion before updating cache.
            let success = match self.config.ack_timeout {
                Some(timeout) => matches!(
                    tokio::time::timeout(timeout, completion_rx).await,
                    Ok(Ok(Ok(_)))
                ),
                None => matches!(completion_rx.await, Ok(Ok(_))),
            };

            // Update cache only if flow completed successfully.
            // Failed flows skip cache update, allowing next cron run to retry from same timestamp.
            if success && !self.task_context.cancellation_token.is_cancelled() {
                counter += 1;
                if let Err(cache_err) = cache
                    .put(&cache_key, current_time.to_string().into(), None)
                    .await
                {
                    warn!("Failed to update cache: {:?}", cache_err);
                }
                if let Err(cache_err) = cache
                    .put(&counter_cache_key, counter.to_string().into(), None)
                    .await
                {
                    warn!("Failed to update counter cache: {:?}", cache_err);
                }
            } else {
                warn!(
                    task = self.config.name,
                    "Flow completion failed or timed out"
                );
            }

            match self.config.count {
                Some(count) if count == counter => return Ok(()),
                Some(_) | None => {}
            }
        }
    }

    #[tracing::instrument(skip(self, data, completion_state), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(
        &self,
        data: serde_json::Value,
        completion_state: crate::event::SharedCompletionTx,
    ) -> Result<(), Error> {
        let e = EventBuilder::new()
            .data(EventData::Json(data))
            .subject(self.config.name.to_owned())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .completion_tx(completion_state)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        e.send_with_logging(self.tx.as_ref())
            .await
            .map_err(|source| Error::SendMessage { source })
    }
}

/// Event generator that produces events at scheduled intervals.
#[derive(Debug)]
pub struct Subscriber {
    /// Configuration settings for event generation.
    config: Arc<crate::task::generate::config::Subscriber>,
    /// Channel sender for broadcasting generated events.
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<crate::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl crate::task::runner::Runner for Subscriber {
    type Error = Error;
    type EventHandler = EventHandler;

    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        self.config
            .validate()
            .map_err(|source| Error::ConfigValidation { source })?;

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            tx: self.tx.clone(),
            task_id: self.task_id,
            task_context: Arc::clone(&self.task_context),
            task_type: self.task_type,
        })
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(self) -> Result<(), Error> {
        let retry_config =
            crate::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);
        let cancellation_token = self.task_context.cancellation_token.clone();

        // Initialise with retry. Cancellation during init returns early so the
        // task tears down without waiting for the retry strategy to exhaust.
        let init_future = tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize generate subscriber");
                        Err(tokio_retry::RetryError::transient(e))
                    }
                }
            },
        );

        let event_handler = tokio::select! {
            _ = cancellation_token.cancelled() => return Ok(()),
            result = init_future => match result {
                Ok(handler) => handler,
                Err(e) => return Err(e),
            },
        };

        // Drive the cron loop. Cancellation also short-circuits the long
        // inter-tick sleeps inside `run_loop()` via the same token.
        let retry_strategy = retry_config.strategy();
        let handle_future = tokio_retry::Retry::spawn(retry_strategy, || async {
            event_handler
                .run_loop()
                .await
                .map_err(tokio_retry::RetryError::transient)
        });

        tokio::select! {
            _ = cancellation_token.cancelled() => Ok(()),
            result = handle_future => {
                match result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        error!(error = %e, "Generate failed after all retry attempts");
                        Ok(())
                    }
                }
            }
        }
    }
}

/// Builder for constructing Subscriber instances.
#[derive(Default)]
pub struct SubscriberBuilder {
    /// Generate task configuration (required for build).
    config: Option<Arc<crate::task::generate::config::Subscriber>>,
    /// Event broadcast sender (required for build).
    tx: Option<Sender<Event>>,
    /// Task identifier for event tracking.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<crate::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl SubscriberBuilder {
    pub fn new() -> SubscriberBuilder {
        SubscriberBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<crate::task::generate::config::Subscriber>) -> Self {
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

    pub fn task_context(mut self, task_context: Arc<crate::task::context::TaskContext>) -> Self {
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
    use crate::cache::Cache;
    use crate::task::runner::Runner;
    use serde_json::{Map, Value};
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<crate::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
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
    async fn test_subscriber_builder() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: Some(json!({"test": "data"})),
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });
        let (tx, _rx) = mpsc::channel(100);

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
    async fn test_subscriber_run_with_count() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: Some(json!({"test": "data"})),
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(2),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event1 = rx.recv().await.unwrap();
        // Complete first flow.
        if let Some(shared_tx) = &event1.completion_tx {
            shared_tx.signal_completion(None);
        }

        let event2 = rx.recv().await.unwrap();
        // Complete second flow.
        if let Some(shared_tx) = &event2.completion_tx {
            shared_tx.signal_completion(None);
        }

        assert_eq!(event1.subject, "test");
        assert_eq!(event2.subject, "test");
        assert_eq!(event1.task_id, 1);
        assert_eq!(event2.task_id, 1);

        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }

    #[tokio::test]
    async fn test_subscriber_event_content() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: Some(json!({"custom_field": "custom_value"})),
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 0,
            task_type: "test",
            task_context: create_mock_task_context(),
        };

        tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        let event = rx.recv().await.unwrap();

        match event.data {
            EventData::Json(value) => {
                assert_eq!(value["custom_field"], "custom_value");
                assert!(value["system_info"].is_object());
                // last_run_time is always populated with current execution time.
                assert!(value["system_info"]["last_run_time"].is_number());
                // First run with count=1, so no next_run_time.
                assert!(value["system_info"]["next_run_time"].is_null());
            }
            _ => panic!("Expected JSON event data"),
        }
    }

    #[tokio::test]
    async fn test_cache_key_generation() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(1)),
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let (tx, mut rx) = mpsc::channel(100);
        let cache = Arc::new(crate::cache::memory::MemoryCache::new());

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Cache Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache.clone() as Arc<dyn crate::cache::Cache>)
                .build()
                .unwrap(),
        );

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };

        // Run subscriber in background.
        tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        // Receive event and complete the flow.
        if let Some(event) = rx.recv().await {
            if let Some(shared_tx) = event.completion_tx {
                shared_tx.signal_completion(None);
            }
        }

        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // Verify that cache key was created with flow-scoped format.
        let result = cache
            .get(&format!(
                "flow.{}.last_run.test",
                crate::identity::encode_key("test-flow")
            ))
            .await
            .unwrap();
        assert!(result.is_some());

        // Verify that counter was persisted to cache.
        let counter_result = cache
            .get(&format!(
                "flow.{}.counter.test",
                crate::identity::encode_key("test-flow")
            ))
            .await
            .unwrap();
        assert!(counter_result.is_some());
        let counter_val = String::from_utf8_lossy(&counter_result.unwrap())
            .parse::<u64>()
            .unwrap();
        assert_eq!(counter_val, 1);
    }

    #[tokio::test]
    async fn test_count_resume_after_restart() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(2),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let cache = Arc::new(crate::cache::memory::MemoryCache::new());

        // Pre-populate counter in cache to simulate a prior run that completed 1 of 2.
        cache
            .put(
                &format!(
                    "flow.{}.counter.test",
                    crate::identity::encode_key("test-flow")
                ),
                "1".to_string().into(),
                None,
            )
            .await
            .unwrap();

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Resume Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache.clone() as Arc<dyn crate::cache::Cache>)
                .build()
                .unwrap(),
        );

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        // Should only receive 1 event (not 2), since counter resumes from 1.
        let event = rx.recv().await.unwrap();
        if let Some(shared_tx) = &event.completion_tx {
            shared_tx.signal_completion(None);
        }

        let _ = handle.await;

        // Wait for spawned task to flush cache.
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

        // No more events should be generated.
        assert!(rx.try_recv().is_err());

        // Counter should now be 2.
        let counter_result = cache
            .get(&format!(
                "flow.{}.counter.test",
                crate::identity::encode_key("test-flow")
            ))
            .await
            .unwrap();
        let counter_val = String::from_utf8_lossy(&counter_result.unwrap())
            .parse::<u64>()
            .unwrap();
        assert_eq!(counter_val, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_count_one_with_long_interval_fires_once() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(60)),
            cron: None,
            count: Some(1),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let cache = Arc::new(crate::cache::memory::MemoryCache::new());

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Count One Interval Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache.clone() as Arc<dyn crate::cache::Cache>)
                .build()
                .unwrap(),
        );

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        tokio::time::advance(Duration::from_secs(61)).await;

        let event = rx.recv().await.expect("expected single event");
        if let Some(shared_tx) = &event.completion_tx {
            shared_tx.signal_completion(None);
        }

        let _ = handle.await;

        tokio::time::advance(Duration::from_secs(600)).await;

        assert!(
            rx.try_recv().is_err(),
            "generate with count=1 must not emit a second event"
        );

        let counter_result = cache
            .get(&format!(
                "flow.{}.counter.test",
                crate::identity::encode_key("test-flow")
            ))
            .await
            .unwrap();
        let counter_val = String::from_utf8_lossy(&counter_result.unwrap())
            .parse::<u64>()
            .unwrap();
        assert_eq!(counter_val, 1);
    }

    #[tokio::test]
    async fn test_count_skip_when_already_complete() {
        let config = Arc::new(crate::task::generate::config::Subscriber {
            name: "test".to_string(),
            payload: None,
            interval: Some(Duration::from_secs(0)),
            cron: None,
            count: Some(3),
            ack_timeout: None,
            retry: None,
            ..Default::default()
        });

        let cache = Arc::new(crate::cache::memory::MemoryCache::new());

        // Pre-populate counter to match count — task already fully completed.
        cache
            .put(
                &format!(
                    "flow.{}.counter.test",
                    crate::identity::encode_key("test-flow")
                ),
                "3".to_string().into(),
                None,
            )
            .await
            .unwrap();

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Skip Test".to_string()),
        );
        let task_manager = Arc::new(
            crate::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let task_context = Arc::new(
            crate::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache.clone() as Arc<dyn crate::cache::Cache>)
                .build()
                .unwrap(),
        );

        let (tx, mut rx) = mpsc::channel(100);

        let subscriber = Subscriber {
            config,
            tx: Some(tx),
            task_id: 1,
            task_type: "test",
            task_context,
        };

        let handle = tokio::spawn(async move {
            let _ = subscriber.run().await;
        });

        // Task should exit immediately without generating any events.
        let _ = handle.await;
        assert!(rx.try_recv().is_err());
    }
}
