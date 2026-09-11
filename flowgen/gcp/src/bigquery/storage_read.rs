//! BigQuery Storage Read API processor for high-throughput parallel table reads.
//!
//! Uses the BigQuery Storage Read API to efficiently read large tables in parallel
//! using multiple streams. Returns data in Arrow RecordBatch format for efficient
//! columnar processing. This API is optimized for reading large amounts of data
//! and does not support SQL queries.

use arrow::array::RecordBatch;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};

use gcloud_googleapis::cloud::bigquery::storage::v1::read_session::{
    TableModifiers, TableReadOptions,
};
use google_cloud_bigquery::client::{Client, ClientConfig, ReadTableOption};
use google_cloud_bigquery::http::table::TableReference;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during BigQuery Storage Read operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Error sending event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Error building event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery Storage Read client authentication error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client creation error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery Storage Read client connection error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("Storage Read operation error: {source}")]
    StorageRead {
        #[source]
        source: google_cloud_bigquery::storage::Error,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Invalid timestamp format in snapshot_time")]
    InvalidTimestamp,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Event handler for processing individual storage read events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::StorageRead>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Stream RecordBatches from the Storage Read API. Peek one batch ahead
            // so the final batch can carry completion_tx without buffering the
            // whole result set. If no batches are returned, emit an empty one so
            // downstream tasks still observe end-of-batch.
            let mut iterator = open_read_iterator(&self.client, &config).await?;
            let mut pending: Option<RecordBatch> = iterator
                .next()
                .await
                .map_err(|source| Error::StorageRead { source })?;

            if pending.is_none() {
                pending = Some(arrow::array::RecordBatch::new_empty(Arc::new(
                    arrow::datatypes::Schema::empty(),
                )));
            }

            loop {
                let batch = match pending.take() {
                    Some(b) => b,
                    None => break,
                };
                let next = iterator
                    .next()
                    .await
                    .map_err(|source| Error::StorageRead { source })?;
                let is_last = next.is_none();
                let num_rows = batch.num_rows();

                let mut result_event = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(batch))
                    .subject(format!("{}.{}", event.subject, config.name))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                if is_last {
                    match self.tx {
                        None => {
                            if let Some(arc) = completion_tx_arc.as_ref() {
                                arc.signal_completion(result_event.data_as_json().ok());
                            }
                        }
                        Some(_) => {
                            result_event.completion_tx = completion_tx_arc.clone();
                        }
                    }
                }

                result_event
                    .send_with_logging(self.tx.as_ref())
                    .context("num_records", num_rows)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;

                pending = next;
            }

            Ok(())
        })
        .await
    }
}

/// BigQuery Storage Read processor that reads tables using Storage Read API.
#[derive(Debug)]
pub struct Processor {
    /// Storage read configuration including credentials and table identifiers.
    config: Arc<super::config::StorageRead>,
    /// Receiver for incoming events to process.
    rx: Receiver<Event>,
    /// Channel sender for result events.
    tx: Option<Sender<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// Task type for event categorization and logging.
    task_type: &'static str,
}

#[async_trait::async_trait]
impl flowgen_core::task::runner::Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

    /// Initializes the processor by establishing BigQuery client connection.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Loading credentials from file
    /// - Creating BigQuery client with authentication
    async fn init(&self) -> Result<EventHandler, Error> {
        let init_config = self
            .config
            .render(&serde_json::json!({}))
            .map_err(|source| Error::ConfigRender { source })?;

        let credentials_path = init_config.credentials_path.clone();
        let client = self
            .task_context
            .client_registry
            .get_or_init(
                flowgen_core::client_registry::ClientKey::new(self.task_type, &credentials_path),
                || async {
                    let credentials = crate::resolve_credentials(&credentials_path)
                        .await
                        .map_err(|source| Error::ClientAuth { source })?;
                    let (client_config, _project_id) =
                        ClientConfig::new_with_credentials(credentials)
                            .await
                            .map_err(|source| Error::ClientCreation { source })?;
                    Client::new(client_config)
                        .await
                        .map_err(|source| Error::ClientConnection { source })
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let event_handler = EventHandler {
            client,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Self::Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize storage read processor");
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
                                        error!(error = %e, "Failed to read from storage");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(error = %e, "Storage read failed after all retry attempts");
                                // Emit error event downstream for error handling.
                                let mut error_event = event.clone();
                                error_event.error = Some(e.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(e.to_string());
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

/// Builder for creating BigQuery Storage Read processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::StorageRead>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Sender<Event>>,
    task_id: Option<usize>,
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> Self {
        Self {
            config: None,
            rx: None,
            tx: None,
            task_id: None,
            task_context: None,
            task_type: None,
        }
    }

    pub fn config(mut self, config: Arc<super::config::StorageRead>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(tx);
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
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

    pub async fn build(self) -> Result<Processor, Error> {
        Ok(Processor {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingBuilderAttribute("receiver".to_string()))?,
            tx: self.tx,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingBuilderAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
        })
    }
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Opens a streaming `RecordBatchIterator` against the BigQuery Storage Read API.
/// Each `next()` returns one Arrow `RecordBatch` as it arrives from the wire,
/// so callers can emit downstream events without buffering the whole result set.
async fn open_read_iterator(
    client: &Client,
    config: &super::config::StorageRead,
) -> Result<google_cloud_bigquery::storage::RecordBatchIterator, Error> {
    let table_ref = TableReference {
        project_id: config.project_id.clone(),
        dataset_id: config.dataset_id.clone(),
        table_id: config.table_id.clone(),
    };

    let mut read_options = TableReadOptions::default();

    if let Some(ref fields) = config.selected_fields {
        read_options.selected_fields = fields.clone();
    }

    if let Some(ref restriction) = config.row_restriction {
        read_options.row_restriction = restriction.clone();
    }

    if let Some(sample_pct) = config.sample_percentage {
        read_options.sample_percentage = Some(sample_pct);
    }

    read_options.response_compression_codec = Some(match config.compression_codec {
        super::config::CompressionCodec::Unspecified => 0,
        super::config::CompressionCodec::Lz4 => 2,
    });

    let table_modifiers = if let Some(ref snapshot_time) = config.snapshot_time {
        let timestamp = chrono::DateTime::parse_from_rfc3339(snapshot_time)
            .map_err(|_| Error::InvalidTimestamp)?;

        Some(TableModifiers {
            snapshot_time: Some(prost_types::Timestamp {
                seconds: timestamp.timestamp(),
                nanos: timestamp.timestamp_subsec_nanos() as i32,
            }),
        })
    } else {
        None
    };

    let mut option = ReadTableOption::default();
    option = option.with_session_read_options(read_options);

    if let Some(modifiers) = table_modifiers {
        option = option.with_session_table_modifiers(modifiers);
    }

    if let Some(max_streams) = config.max_stream_count {
        option = option.with_max_stream_count(max_streams);
    }

    option = option.with_job_project_id(config.get_job_project_id().to_string());

    client
        .read_table_record_batches(&table_ref, Some(option))
        .await
        .map_err(|source| Error::StorageRead { source })
}

#[cfg(test)]
mod tests {
    #[test]
    fn test_invalid_timestamp() {
        let result = chrono::DateTime::parse_from_rfc3339("invalid");
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_timestamp_parsing() {
        let result = chrono::DateTime::parse_from_rfc3339("2024-01-15T12:00:00Z");
        assert!(result.is_ok());
        let timestamp = result.unwrap();
        assert_eq!(timestamp.timestamp(), 1705320000);
    }

    #[test]
    fn test_compression_codec_values() {
        use super::super::config::CompressionCodec;

        let unspecified = CompressionCodec::Unspecified;
        let lz4 = CompressionCodec::Lz4;

        let unspecified_value = match unspecified {
            CompressionCodec::Unspecified => 0,
            CompressionCodec::Lz4 => 2,
        };

        let lz4_value = match lz4 {
            CompressionCodec::Unspecified => 0,
            CompressionCodec::Lz4 => 2,
        };

        assert_eq!(unspecified_value, 0);
        assert_eq!(lz4_value, 2);
    }

    #[test]
    fn test_table_reference_creation() {
        use google_cloud_bigquery::http::table::TableReference;

        let table_ref = TableReference {
            project_id: "my-project".to_string(),
            dataset_id: "my-dataset".to_string(),
            table_id: "my-table".to_string(),
        };

        assert_eq!(table_ref.project_id, "my-project");
        assert_eq!(table_ref.dataset_id, "my-dataset");
        assert_eq!(table_ref.table_id, "my-table");
    }
}
