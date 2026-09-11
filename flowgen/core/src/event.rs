//! Event system for processing and routing data through workflows.
//!
//! Provides event structures, data format handling, subject generation utilities,
//! and logging functionality for the flowgen event processing pipeline.

use crate::buffer::{ContentType, FromReader, ToWriter};
use apache_avro::{from_avro_datum, Reader as AvroReader};
use arrow::{array::RecordBatchWriter, csv::reader::Format};
use base64::Engine;
use chrono::Utc;
use serde::{Serialize, Serializer};
use serde_json::{Map, Value};
use std::cell::RefCell;
use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tracing::info;

/// One-shot channel used by a flow source to wait for end-to-end completion.
/// Carries an optional result payload for request-response flows (HTTP webhook, MCP).
/// Leaves that produce the response payload send `Ok(Some(value))`, others send `Ok(None)`.
pub type CompletionTx = tokio::sync::oneshot::Sender<
    Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
>;

/// Tracks leaf-task completion so the source acks only after all leaves in the
/// directed acyclic graph have committed.
///
/// In a linear flow there is one leaf, so completion fires as soon as that leaf
/// signals. In a fan-out flow there are multiple leaves; the source is notified
/// only when the last one signals. The payload delivered to the source is the
/// last leaf's payload — earlier leaves' payloads are dropped.
///
/// A leaf can signal success via `signal_completion` or failure via
/// `signal_completion_with_error`. The "last leaf wins" rule applies to both —
/// a failing leaf that signals last delivers the error to the source even if
/// sibling leaves succeeded, and vice versa.
#[derive(Debug)]
pub struct CompletionState {
    /// Number of leaves still expected to signal completion.
    remaining: AtomicUsize,
    /// One-shot sender to the source. `None` once completion has been delivered.
    sender: Mutex<Option<CompletionTx>>,
}

impl CompletionState {
    /// Creates a new completion state for a flow with the given number of leaves.
    ///
    /// A `leaf_count` of zero is treated as one to avoid a flow that can never
    /// signal completion. This should not happen in practice — every flow has
    /// at least one terminal task — but the guard prevents a stuck source if a
    /// future flow shape produces an empty leaf set.
    pub fn new(leaf_count: usize, sender: CompletionTx) -> Self {
        Self {
            remaining: AtomicUsize::new(leaf_count.max(1)),
            sender: Mutex::new(Some(sender)),
        }
    }

    /// Signals completion from a leaf task with its result payload.
    ///
    /// Decrements the outstanding-leaf counter; the leaf that brings it to zero
    /// delivers its payload to the source via the one-shot channel. Earlier
    /// leaves' payloads are dropped. Subsequent calls after delivery are no-ops.
    pub fn signal_completion(&self, payload: Option<serde_json::Value>) {
        // Saturating decrement protects against accidental double-signal from
        // a single leaf (which would otherwise underflow `remaining` and
        // eventually re-trigger the delivery branch).
        let previous = self.remaining.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| match current {
                0 => None,
                n => Some(n - 1),
            },
        );
        // We are the last leaf when the previous value was 1; otherwise
        // either more leaves are still outstanding or completion already
        // fired and the counter is saturated at zero.
        if !matches!(previous, Ok(1)) {
            return;
        }
        if let Ok(mut guard) = self.sender.lock() {
            if let Some(tx) = guard.take() {
                tx.send(Ok(payload)).ok();
            }
        }
    }

    /// Signals completion from a leaf task with a failure message.
    ///
    /// Same last-leaf-wins semantics as [`signal_completion`](Self::signal_completion):
    /// the leaf that brings the outstanding-leaf counter to zero delivers the
    /// error to the source. The source (HTTP webhook, MCP) surfaces it to the
    /// caller as a tool/request error instead of falling back to an opaque
    /// acknowledgement timeout.
    pub fn signal_completion_with_error(&self, error: String) {
        let previous = self.remaining.fetch_update(
            Ordering::AcqRel,
            Ordering::Acquire,
            |current| match current {
                0 => None,
                n => Some(n - 1),
            },
        );
        if !matches!(previous, Ok(1)) {
            return;
        }
        if let Ok(mut guard) = self.sender.lock() {
            if let Some(tx) = guard.take() {
                tx.send(Err(error.into())).ok();
            }
        }
    }
}

/// Shared handle to the per-flow completion state, attached to events so leaf
/// tasks can signal end-to-end acknowledgement back to the source.
pub type SharedCompletionTx = Arc<CompletionState>;

/// Receiver side of the source-to-leaves completion channel.
pub type CompletionRx = tokio::sync::oneshot::Receiver<
    Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
>;

/// Creates a completion channel for a flow with the given number of leaves.
///
/// The returned `SharedCompletionTx` is attached to the source event and travels
/// with every cloned/derived event through the directed acyclic graph. The
/// `CompletionRx` is held by the source task, which awaits it before
/// acknowledging the upstream message.
pub fn new_completion_channel(leaf_count: usize) -> (SharedCompletionTx, CompletionRx) {
    let (tx, rx) = tokio::sync::oneshot::channel();
    (Arc::new(CompletionState::new(leaf_count, tx)), rx)
}

tokio::task_local! {
    /// Task-local storage for the current event context.
    /// Used by EventBuilder::new() to automatically preserve meta fields from the incoming event.
    /// Unlike thread_local!, this stays with the tokio task even when it migrates between threads.
    static CURRENT_EVENT_META: RefCell<Option<Map<String, Value>>>;
}

/// Runs an async function with event context set for automatic meta preservation.
/// Use this to wrap handler functions that should preserve event meta in EventBuilder::new() calls.
///
/// Takes an Arc<Event> to avoid cloning the event data, only the Arc pointer is cloned inside.
///
/// # Example
/// ```ignore
/// async fn handle(&self, event: Event) -> Result<(), Error> {
///     let event = Arc::new(event);
///     with_event_context(&Arc::clone(&event), async move {
///         // EventBuilder::new() will automatically preserve event.meta
///         // Access event fields via event.data, event.subject, etc.
///         let new_event = EventBuilder::new()
///             .data(some_data)
///             .subject("example")
///             .build()?;
///         Ok(())
///     }).await
/// }
/// ```
pub async fn with_event_context<F, R>(event: &Arc<Event>, f: F) -> R
where
    F: std::future::Future<Output = R>,
{
    CURRENT_EVENT_META
        .scope(RefCell::new(event.meta.clone()), f)
        .await
}

/// Builder for sending events with structured logging context.
///
/// Per-task processing time is exposed through the OpenTelemetry span
/// `task.handle` rather than as a log field. Use this builder to attach
/// task-specific structured fields (record counts, status codes, etc.) to
/// the success log line.
pub struct EventLogger<'a> {
    event: Event,
    tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    fields: Vec<(&'static str, String)>,
}

impl<'a> EventLogger<'a> {
    /// Add a context field to the structured log output.
    ///
    /// # Example
    /// ```ignore
    /// event.send_with_logging(Some(&tx))
    ///     .context("row_count", 1000)
    ///     .context("external_id", "job-123")
    ///     .await?;
    /// ```
    pub fn context(mut self, key: &'static str, value: impl std::fmt::Display) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }
}

// Implement IntoFuture to make EventLogger awaitable.
impl<'a> std::future::IntoFuture for EventLogger<'a> {
    type Output = Result<(), Error>;
    type IntoFuture =
        std::pin::Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let event_id = match &self.event.id {
                Some(ref id) => id.to_string(),
                None => self.event.timestamp.to_string(),
            };
            let subject = self.event.subject.clone();

            if let Some(tx) = self.tx {
                tx.send(self.event).await.map_err(|_| Error::SendMessage)?;
            }

            if self.fields.is_empty() {
                info!(
                    event.subject = %subject,
                    event.id = %event_id,
                );
            } else {
                // Serialize context fields as a JSON object so downstream
                // consumers (admin UI, log shipper) can split them back
                // into individual attributes instead of parsing a joined
                // string.
                let map: std::collections::BTreeMap<&str, &str> =
                    self.fields.iter().map(|(k, v)| (*k, v.as_str())).collect();
                let context_json = serde_json::to_string(&map).unwrap_or_default();

                info!(
                    event.subject = %subject,
                    event.id = %event_id,
                    context = %context_json,
                );
            }

            Ok(())
        })
    }
}

/// Extension trait for event processing with logging.
pub trait EventExt {
    /// Logs event processing and optionally sends to the next task.
    ///
    /// This method always logs the event, then sends it to the next task if a sender is provided.
    /// Use this in task handlers to ensure visibility of event processing throughout the pipeline.
    ///
    /// Returns a builder that allows adding context fields via `.context()` calls.
    /// The builder implements `IntoFuture`, so you can await it directly.
    ///
    /// # Example
    /// ```ignore
    /// // Simple usage without context
    /// event.send_with_logging(Some(&tx)).await?;
    ///
    /// // With context fields
    /// event.send_with_logging(Some(&tx))
    ///     .context("row_count", 1000)
    ///     .context("external_id", "job-123")
    ///     .await?;
    /// ```
    fn send_with_logging<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> EventLogger<'a>;

    /// Sends the event downstream without emitting a log line.
    ///
    /// Use this only on streaming hot paths where per-event logs would
    /// drown the operator — token-by-token LLM chunks, SSE proxy frames,
    /// and similar high-cardinality intermediates. The terminal/final
    /// event of such a stream should still go through `send_with_logging`
    /// so completion stays observable in the default log stream.
    ///
    /// The `_silent` suffix is deliberate: `send_with_logging` is the
    /// default, opt-out is explicit at the call site, and a simple grep
    /// for `send_silent` lists every place we suppress event-flow visibility.
    fn send_silent<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>>;
}

impl EventExt for Event {
    fn send_with_logging<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> EventLogger<'a> {
        EventLogger {
            event: self,
            tx,
            fields: Vec::new(),
        }
    }

    fn send_silent<'a>(
        self,
        tx: Option<&'a tokio::sync::mpsc::Sender<Event>>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), Error>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(tx) = tx {
                tx.send(self).await.map_err(|_| Error::SendMessage)?;
            }
            Ok(())
        })
    }
}

impl Event {
    /// Converts event data to a serde_json::Value for processing.
    ///
    /// This is a convenience method that extracts the raw data payload from EventData
    /// without the event wrapper structure (subject, id, timestamp, etc.).
    /// Use this when you need to work with just the data portion of an event.
    ///
    /// # Example
    /// ```ignore
    /// let event_data = event.data_as_json()?;
    /// // event_data is the raw array/object/value, not wrapped in {"event": {...}}
    /// ```
    pub fn data_as_json(&self) -> Result<Value, Error> {
        Value::try_from(&self.data)
    }

    /// Returns true if this event carries an error from a failed upstream task.
    pub fn is_error(&self) -> bool {
        self.error.is_some()
    }
}

/// Errors that can occur during event processing operations.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("IO error: {source}")]
    IO {
        #[source]
        source: std::io::Error,
    },
    #[error("Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Avro error: {source}")]
    Avro {
        #[source]
        source: apache_avro::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::error::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Content type conversion not supported: {from} to {to}")]
    UnsupportedContentTypeConversion { from: String, to: String },
    #[error("Error sending event to channel (receiver dropped)")]
    SendMessage,
}

/// Core event structure containing data and metadata for workflow processing.
#[derive(Debug)]
pub struct Event {
    /// Event payload in one of the supported data formats.
    pub data: EventData,
    /// Subject identifier for event routing and filtering.
    pub subject: String,
    /// Optional unique identifier for the event.
    pub id: Option<String>,
    /// Event creation timestamp in microseconds since Unix epoch.
    pub timestamp: i64,
    /// Task identifier for tracking event flow through pipeline stages.
    pub task_id: usize,
    /// Task type for categorization and logging.
    pub task_type: &'static str,
    /// Optional metadata for passing contextual information between tasks.
    /// Metadata can be set by script tasks and accessed in templates using event.meta syntax.
    /// Useful for adding context that should travel with the event but is separate from the payload.
    pub meta: Option<Map<String, Value>>,
    /// Error message set when a task fails after exhausting all retries.
    /// When present, the event flows downstream as an error event so scripts
    /// can inspect it and route to DLQ, audit tables, or other error handlers.
    pub error: Option<String>,
    /// Completion notifier for end-to-end pipeline acknowledgement.
    /// Present only for events from replayable sources (NATS, Salesforce Pub/Sub,
    /// HTTP webhook, MCP server, scheduled generators).
    /// Intermediate tasks pass this through unchanged on each emitted event.
    /// Leaf tasks call `signal_completion` on the shared state; the source is
    /// notified once every leaf has signalled.
    pub completion_tx: Option<SharedCompletionTx>,
}

impl Clone for Event {
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            subject: self.subject.clone(),
            id: self.id.clone(),
            timestamp: self.timestamp,
            task_id: self.task_id,
            task_type: self.task_type,
            meta: self.meta.clone(),
            error: self.error.clone(),
            completion_tx: self.completion_tx.clone(),
        }
    }
}

impl TryFrom<&Event> for Value {
    type Error = Error;
    fn try_from(event: &Event) -> Result<Self, Self::Error> {
        let event_data = serde_json::Value::try_from(&event.data)?;
        Ok(serde_json::json!({
            "event": {
                "subject": event.subject,
                "data": event_data,
                "id": event.id,
                "timestamp": event.timestamp,
                "task_id": event.task_id,
                "task_type": event.task_type,
                "meta": event.meta,
                "error": event.error,
            }
        }))
    }
}

impl std::fmt::Display for Event {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let event_data = serde_json::Value::try_from(&self.data)
            .unwrap_or_else(|_| serde_json::json!(format!("{:?}", self.data)));

        // Build the pretty-print body field-by-field so optional fields
        // (`id`, `meta`, `error`) only appear when they actually carry a
        // value. Otherwise every log line gets littered with `"meta":
        // null` etc., which is noise that matches no industry convention
        // for structured logs.
        let mut obj = serde_json::Map::new();
        obj.insert(
            "subject".to_string(),
            serde_json::Value::String(self.subject.clone()),
        );
        obj.insert("data".to_string(), event_data);
        if let Some(id) = &self.id {
            obj.insert("id".to_string(), serde_json::Value::String(id.clone()));
        }
        obj.insert("timestamp".to_string(), serde_json::json!(self.timestamp));
        obj.insert("task_id".to_string(), serde_json::json!(self.task_id));
        obj.insert(
            "task_type".to_string(),
            serde_json::Value::String(self.task_type.to_string()),
        );
        if let Some(meta) = &self.meta {
            obj.insert("meta".to_string(), serde_json::Value::Object(meta.clone()));
        }
        if let Some(error) = &self.error {
            obj.insert(
                "error".to_string(),
                serde_json::Value::String(error.clone()),
            );
        }

        let formatted = serde_json::to_string_pretty(&serde_json::Value::Object(obj))
            .unwrap_or_else(|_| format!("{self:?}"));

        write!(f, "{formatted}")
    }
}

/// Event data payload supporting multiple serialization formats.
#[derive(Debug, Clone)]
pub enum EventData {
    /// Apache Arrow columnar data format for analytics workloads.
    ArrowRecordBatch(arrow::array::RecordBatch),
    /// Apache Avro binary format with embedded schema.
    Avro(AvroData),
    /// JSON format for flexible structured data.
    Json(serde_json::Value),
    /// Raw bytes for binary payloads (archive contents, non-UTF-8 blobs).
    Bytes(bytes::Bytes),
}

/// Avro data container with schema and serialized payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AvroData {
    /// Avro schema definition in JSON format.
    pub schema: String,
    /// Binary-encoded Avro data according to the schema.
    pub raw_bytes: Vec<u8>,
}

impl TryFrom<&EventData> for Value {
    type Error = Error;

    fn try_from(event_data: &EventData) -> Result<Self, Self::Error> {
        let data = match event_data {
            EventData::ArrowRecordBatch(data) => {
                let buf = Vec::new();
                let mut writer = arrow_json::ArrayWriter::new(buf);
                writer
                    .write_batches(&[data])
                    .map_err(|e| Error::Arrow { source: e })?;
                writer.finish().map_err(|e| Error::Arrow { source: e })?;
                let json_data = writer.into_inner();
                let json_rows: Vec<Map<String, Value>> =
                    serde_json::from_reader(json_data.as_slice())
                        .map_err(|e| Error::SerdeJson { source: e })?;
                json_rows.into()
            }
            EventData::Avro(data) => {
                let schema = apache_avro::Schema::parse_str(&data.schema)
                    .map_err(|e| Error::Avro { source: e })?;
                let avro_value = from_avro_datum(&schema, &mut &data.raw_bytes[..], None)
                    .map_err(|e| Error::Avro { source: e })?;
                serde_json::Value::try_from(avro_value).map_err(|e| Error::Avro { source: e })?
            }
            EventData::Json(data) => data.clone(),
            EventData::Bytes(bytes) => {
                // Base64 so Handlebars templates against `event.data` don't crash on binary payloads.
                Value::String(base64::engine::general_purpose::STANDARD.encode(bytes))
            }
        };
        Ok(data)
    }
}

impl Serialize for EventData {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let json_value = serde_json::Value::try_from(self).map_err(serde::ser::Error::custom)?;
        json_value.serialize(serializer)
    }
}

/// Builder for constructing Event instances with validation.
#[derive(Default, Debug)]
pub struct EventBuilder {
    /// Event data payload (required for build).
    pub data: Option<EventData>,
    /// Arrow record batch extensions for metadata.
    pub extensions: Option<arrow::array::RecordBatch>,
    /// Event subject for routing (required for build).
    pub subject: Option<String>,
    /// Optional unique event identifier.
    pub id: Option<String>,
    /// Event timestamp, defaults to current time.
    pub timestamp: Option<i64>,
    /// Current task identifier for pipeline tracking.
    pub task_id: Option<usize>,
    /// Task type for categorization and logging (required for build).
    pub task_type: Option<&'static str>,
    /// Optional metadata for contextual information.
    pub meta: Option<Map<String, Value>>,
    /// Completion notifier for end-to-end acknowledgment.
    pub completion_tx: Option<SharedCompletionTx>,
}

impl EventBuilder {
    /// Creates a new EventBuilder.
    /// Automatically preserves meta from the current event context (if set via with_event_context).
    /// Generates a correlation_id in meta if one is not already present.
    pub fn new() -> Self {
        let mut meta = CURRENT_EVENT_META
            .try_with(|m| m.borrow().clone())
            .ok()
            .flatten();

        // Ensure every event chain has a correlation_id for end-to-end tracing.
        // Set once at the source, then preserved through all downstream tasks.
        let has_correlation_id = match meta.as_ref() {
            Some(m) => m.contains_key("correlation_id"),
            None => false,
        };
        if !has_correlation_id {
            let correlation_id = uuid::Uuid::now_v7().to_string();
            meta.get_or_insert_with(Map::new)
                .insert("correlation_id".to_string(), Value::String(correlation_id));
        }

        EventBuilder {
            timestamp: Some(Utc::now().timestamp_micros()),
            meta,
            ..Default::default()
        }
    }

    pub fn data(mut self, data: EventData) -> Self {
        self.data = Some(data);
        self
    }
    pub fn subject(mut self, subject: String) -> Self {
        self.subject = Some(subject);
        self
    }
    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }
    pub fn id(mut self, id: String) -> Self {
        self.id = Some(id);
        self
    }
    pub fn timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }
    pub fn task_type(mut self, task_type: &'static str) -> Self {
        self.task_type = Some(task_type);
        self
    }
    pub fn meta(mut self, meta: Map<String, Value>) -> Self {
        self.meta = Some(meta);
        self
    }

    /// Merges extra metadata into the builder's existing meta. Existing
    /// keys are overwritten only when `extra` provides the same key.
    pub fn meta_merge(mut self, extra: Map<String, Value>) -> Self {
        match self.meta.take() {
            Some(mut existing) => {
                for (k, v) in extra {
                    existing.insert(k, v);
                }
                self.meta = Some(existing);
            }
            None => self.meta = Some(extra),
        }
        self
    }

    pub fn completion_tx(mut self, completion_tx: SharedCompletionTx) -> Self {
        self.completion_tx = Some(completion_tx);
        self
    }

    pub fn build(self) -> Result<Event, Error> {
        Ok(Event {
            data: self
                .data
                .ok_or_else(|| Error::MissingBuilderAttribute("data".to_string()))?,
            subject: self
                .subject
                .ok_or_else(|| Error::MissingBuilderAttribute("subject".to_string()))?,
            id: self.id,
            timestamp: self
                .timestamp
                .ok_or_else(|| Error::MissingBuilderAttribute("timestamp".to_string()))?,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingBuilderAttribute("task_id".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingBuilderAttribute("task_type".to_string()))?,
            meta: self.meta,
            error: None,
            completion_tx: self.completion_tx,
        })
    }
}

impl<R: Read + Seek + Send + 'static> FromReader<R> for EventData {
    type Error = Error;

    fn from_reader(
        mut reader: R,
        content_type: ContentType,
    ) -> Result<Box<dyn Iterator<Item = Result<Self, Self::Error>> + Send>, Self::Error> {
        match content_type {
            ContentType::Json => {
                let data: Value =
                    serde_json::from_reader(reader).map_err(|e| Error::SerdeJson { source: e })?;
                Ok(Box::new(std::iter::once(Ok(EventData::Json(data)))))
            }

            ContentType::Csv {
                batch_size,
                has_header,
                delimiter,
                infer_schema_max_records,
            } => {
                let delimiter_byte = delimiter.unwrap_or(b',');

                let (schema, _) = Format::default()
                    .with_header(has_header)
                    .with_delimiter(delimiter_byte)
                    .infer_schema(&mut reader, infer_schema_max_records)
                    .map_err(|e| Error::Arrow { source: e })?;
                reader.rewind().map_err(|e| Error::IO { source: e })?;

                let csv_reader = arrow::csv::ReaderBuilder::new(Arc::new(schema))
                    .with_header(has_header)
                    .with_delimiter(delimiter_byte)
                    .with_batch_size(batch_size)
                    .build(reader)
                    .map_err(|e| Error::Arrow { source: e })?;

                Ok(Box::new(csv_reader.map(|batch_result| {
                    let batch = batch_result.map_err(|e| Error::Arrow { source: e })?;
                    Ok(EventData::ArrowRecordBatch(batch))
                })))
            }

            ContentType::Parquet { batch_size } => {
                let mut buffer = Vec::new();
                reader
                    .read_to_end(&mut buffer)
                    .map_err(|e| Error::IO { source: e })?;

                let parquet_reader =
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(
                        bytes::Bytes::from(buffer),
                    )
                    .map_err(|e| Error::Arrow {
                        source: arrow::error::ArrowError::ExternalError(Box::new(e)),
                    })?
                    .with_batch_size(batch_size)
                    .build()
                    .map_err(|e| Error::Arrow {
                        source: arrow::error::ArrowError::ExternalError(Box::new(e)),
                    })?;

                Ok(Box::new(parquet_reader.map(|batch_result| {
                    let batch = batch_result.map_err(|e| Error::Arrow { source: e })?;
                    Ok(EventData::ArrowRecordBatch(batch))
                })))
            }

            ContentType::Avro => {
                let avro_reader = AvroReader::new(reader).map_err(|e| Error::Avro { source: e })?;
                let schema = avro_reader.writer_schema().clone();
                let schema_json = schema.canonical_form();

                Ok(Box::new(avro_reader.map(move |record| {
                    let value = record.map_err(|e| Error::Avro { source: e })?;
                    let raw_bytes = apache_avro::to_avro_datum(&schema, value)
                        .map_err(|e| Error::Avro { source: e })?;
                    Ok(EventData::Avro(AvroData {
                        schema: schema_json.clone(),
                        raw_bytes,
                    }))
                })))
            }
        }
    }
}

impl<W: Write> ToWriter<W> for EventData {
    type Error = Error;

    fn to_writer(self, writer: W) -> Result<(), Self::Error> {
        match self {
            EventData::Json(data) => {
                serde_json::to_writer(writer, &data).map_err(|e| Error::SerdeJson { source: e })?;
                Ok(())
            }
            EventData::ArrowRecordBatch(batch) => {
                let mut csv_writer = arrow::csv::WriterBuilder::new()
                    .with_header(true) // Assume has header.
                    .build(writer);
                csv_writer
                    .write(&batch)
                    .map_err(|e| Error::Arrow { source: e })?;
                csv_writer.close().map_err(|e| Error::Arrow { source: e })?;
                Ok(())
            }
            EventData::Avro(avro_data) => {
                let schema = apache_avro::Schema::parse_str(&avro_data.schema)
                    .map_err(|e| Error::Avro { source: e })?;
                let value = from_avro_datum(&schema, &mut &avro_data.raw_bytes[..], None)
                    .map_err(|e| Error::Avro { source: e })?;
                let mut avro_writer = apache_avro::Writer::new(&schema, writer);
                avro_writer
                    .append(value)
                    .map_err(|e| Error::Avro { source: e })?;
                avro_writer.flush().map_err(|e| Error::Avro { source: e })?;
                Ok(())
            }
            EventData::Bytes(bytes) => {
                let mut writer = writer;
                writer
                    .write_all(&bytes)
                    .map_err(|source| Error::IO { source })?;
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::io::Cursor;

    #[test]
    fn test_event_builder_success() {
        let event = EventBuilder::new()
            .data(EventData::Json(json!({"test": "value"})))
            .subject("test.subject".to_string())
            .id("test-id".to_string())
            .task_id(1)
            .task_type("test")
            .build()
            .unwrap();

        assert_eq!(event.subject, "test.subject");
        assert_eq!(event.id, Some("test-id".to_string()));
        assert_eq!(event.task_id, 1);
        assert!(event.timestamp > 0);

        match event.data {
            EventData::Json(value) => assert_eq!(value, json!({"test": "value"})),
            _ => panic!("Expected JSON data"),
        }
    }

    #[test]
    fn test_avro_data_serialization() {
        let avro_data = AvroData {
            schema: r#"{"type": "string"}"#.to_string(),
            raw_bytes: vec![1, 2, 3, 4],
        };

        let serialized = serde_json::to_string(&avro_data).unwrap();
        let deserialized: AvroData = serde_json::from_str(&serialized).unwrap();

        assert_eq!(avro_data.schema, deserialized.schema);
        assert_eq!(avro_data.raw_bytes, deserialized.raw_bytes);
    }

    #[test]
    fn test_event_data_json_conversion() {
        let json_data = json!({"field": "value", "number": 42});
        let event_data = EventData::Json(json_data.clone());

        let converted = Value::try_from(&event_data).unwrap();
        assert_eq!(converted, json_data);
    }

    #[test]
    fn test_event_data_json_to_writer() {
        let json_data = json!({"test": "data"});
        let event_data = EventData::Json(json_data);

        let mut buffer = Vec::new();
        event_data.to_writer(&mut buffer).unwrap();

        let result: serde_json::Value = serde_json::from_slice(&buffer).unwrap();
        assert_eq!(result, json!({"test": "data"}));
    }

    #[test]
    fn test_event_data_from_json_reader() {
        let json_content = r#"{"name": "test", "value": 123}"#;
        let cursor = Cursor::new(json_content);

        let events: Vec<EventData> = EventData::from_reader(cursor, ContentType::Json)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(events.len(), 1);

        match &events[0] {
            EventData::Json(value) => {
                assert_eq!(value["name"], "test");
                assert_eq!(value["value"], 123);
            }
            _ => panic!("Expected JSON event data"),
        }
    }

    #[test]
    fn test_event_data_parquet_roundtrip() {
        use arrow::array::{Int32Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let original_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        let props = parquet::file::properties::WriterProperties::builder().build();
        let mut parquet_writer =
            parquet::arrow::ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
        parquet_writer.write(&original_batch).unwrap();
        parquet_writer.close().unwrap();

        let cursor = Cursor::new(buffer);
        let events: Vec<EventData> =
            EventData::from_reader(cursor, ContentType::Parquet { batch_size: 1024 })
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();

        assert_eq!(events.len(), 1);
        match &events[0] {
            EventData::ArrowRecordBatch(batch) => {
                assert_eq!(batch.num_rows(), 3);
                assert_eq!(batch.num_columns(), 2);
                assert_eq!(batch.schema(), schema);
            }
            _ => panic!("Expected Arrow RecordBatch"),
        }
    }
}
