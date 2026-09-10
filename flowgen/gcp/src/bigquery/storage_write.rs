//! BigQuery Storage Write API processor for high-throughput streaming inserts.
//!
//! Provides direct streaming writes to BigQuery tables using the Storage Write API,
//! which offers lower latency and higher throughput compared to traditional load jobs.
//! Supports both default streams (for immediate availability) and committed streams
//! (for exactly-once semantics with deduplication).

use base64::Engine;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use futures_util::StreamExt;

use google_cloud_bigquery::{
    client::{Client, ClientConfig},
    http::table::{TableFieldMode, TableFieldSchema, TableFieldType},
    storage_write::{stream::default::DefaultStream, AppendRowsRequestBuilder},
};
use prost_types::{field_descriptor_proto, DescriptorProto, FieldDescriptorProto};
use serde_json::Value as JsonValue;
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, warn, Instrument};

/// Errors that can occur during BigQuery Storage Write operations.
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
    #[error("Error converting event data: {source}")]
    EventData {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("BigQuery client authentication error: {source}")]
    ClientAuth {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client creation error: {source}")]
    ClientCreation {
        #[source]
        source: gcloud_auth::error::Error,
    },
    #[error("BigQuery client connection error: {source}")]
    ClientConnection {
        #[source]
        source: gcloud_gax::conn::Error,
    },
    #[error("BigQuery storage write error: {source}")]
    StorageWrite {
        #[source]
        source: gcloud_gax::grpc::Status,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Failed to encode JSON to protobuf: {source}")]
    ProtoEncode {
        #[source]
        source: serde_json::Error,
    },
    #[error("Arrow conversion error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Table metadata retrieval error: {source}")]
    TableMetadata {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("Table schema missing")]
    MissingSchema,
    #[error("Protobuf encoding error: {0}")]
    ProtobufEncode(String),
    #[error("Field '{field}': expected {expected_type}, got {actual_value}")]
    FieldTypeMismatch {
        field: String,
        expected_type: &'static str,
        actual_value: String,
    },
    #[error("Field '{field}': invalid base64 encoding: {source}")]
    FieldBase64Decode {
        field: String,
        #[source]
        source: base64::DecodeError,
    },
    #[error("Field '{field}': expected JSON object for RECORD/STRUCT, got {actual_value}")]
    FieldExpectedObject { field: String, actual_value: String },
    #[error("Row write failed with errors")]
    RowWriteError,
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl From<flowgen_core::event::Error> for Error {
    fn from(source: flowgen_core::event::Error) -> Self {
        Error::EventData { source }
    }
}

/// Maps BigQuery table field type to protobuf field type.
fn bq_type_to_proto_type(bq_type: &TableFieldType) -> field_descriptor_proto::Type {
    match bq_type {
        TableFieldType::String
        | TableFieldType::Datetime
        | TableFieldType::Json
        | TableFieldType::Date
        | TableFieldType::Time
        | TableFieldType::Timestamp
        | TableFieldType::Numeric
        | TableFieldType::Bignumeric => field_descriptor_proto::Type::String,
        TableFieldType::Int64 | TableFieldType::Integer => field_descriptor_proto::Type::Int64,
        TableFieldType::Float64 | TableFieldType::Float => field_descriptor_proto::Type::Double,
        TableFieldType::Bool | TableFieldType::Boolean => field_descriptor_proto::Type::Bool,
        TableFieldType::Bytes => field_descriptor_proto::Type::Bytes,
        TableFieldType::Record | TableFieldType::Struct => field_descriptor_proto::Type::Message,
        _ => field_descriptor_proto::Type::String,
    }
}

/// Builds a protobuf DescriptorProto from a BigQuery table schema.
/// Recursively handles nested RECORD/STRUCT fields.
fn build_proto_descriptor(
    fields: &[TableFieldSchema],
    include_change_type: bool,
) -> DescriptorProto {
    build_proto_descriptor_inner("BqMessage", fields, include_change_type)
}

fn build_proto_descriptor_inner(
    message_name: &str,
    fields: &[TableFieldSchema],
    include_change_type: bool,
) -> DescriptorProto {
    let mut proto_fields: Vec<FieldDescriptorProto> = Vec::with_capacity(fields.len());
    let mut nested_types: Vec<DescriptorProto> = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let label = match field.mode {
            Some(TableFieldMode::Repeated) => Some(field_descriptor_proto::Label::Repeated.into()),
            _ => Some(field_descriptor_proto::Label::Optional.into()),
        };

        let is_record = matches!(
            field.data_type,
            TableFieldType::Record | TableFieldType::Struct
        );

        let (proto_type, type_name) = match is_record {
            true => {
                let nested_name = format!("{}_nested", field.name);
                let nested_fields = field.fields.as_deref().unwrap_or(&[]);
                let nested_descriptor =
                    build_proto_descriptor_inner(&nested_name, nested_fields, false);
                nested_types.push(nested_descriptor);
                (
                    Some(field_descriptor_proto::Type::Message.into()),
                    Some(nested_name),
                )
            }
            false => (Some(bq_type_to_proto_type(&field.data_type).into()), None),
        };

        proto_fields.push(FieldDescriptorProto {
            name: Some(field.name.clone()),
            number: Some((i + 1) as i32),
            label,
            r#type: proto_type,
            type_name,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });
    }

    if include_change_type {
        proto_fields.push(FieldDescriptorProto {
            name: Some("_CHANGE_TYPE".to_string()),
            number: Some((fields.len() + 1) as i32),
            label: Some(field_descriptor_proto::Label::Optional.into()),
            r#type: Some(field_descriptor_proto::Type::String.into()),
            type_name: None,
            extendee: None,
            default_value: None,
            oneof_index: None,
            json_name: None,
            options: None,
            proto3_optional: None,
        });
    }

    DescriptorProto {
        name: Some(message_name.to_string()),
        field: proto_fields,
        extension: vec![],
        nested_type: nested_types,
        enum_type: vec![],
        extension_range: vec![],
        oneof_decl: vec![],
        options: None,
        reserved_range: vec![],
        reserved_name: vec![],
    }
}

/// Encodes a single scalar JSON value as protobuf bytes for the given field tag and type.
fn encode_scalar_value(
    tag: u32,
    value: &JsonValue,
    field_name: &str,
    proto_type: field_descriptor_proto::Type,
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    match proto_type {
        field_descriptor_proto::Type::String => {
            let s = match value {
                JsonValue::String(s) => s.clone(),
                _ => value.to_string(),
            };
            prost::encoding::string::encode(tag, &s, buf);
        }
        field_descriptor_proto::Type::Int64 => match value.as_i64() {
            Some(n) => prost::encoding::int64::encode(tag, &n, buf),
            None => {
                return Err(Error::FieldTypeMismatch {
                    field: field_name.to_string(),
                    expected_type: "INT64",
                    actual_value: value.to_string(),
                });
            }
        },
        field_descriptor_proto::Type::Double => match value.as_f64() {
            Some(n) => prost::encoding::double::encode(tag, &n, buf),
            None => {
                return Err(Error::FieldTypeMismatch {
                    field: field_name.to_string(),
                    expected_type: "FLOAT64",
                    actual_value: value.to_string(),
                });
            }
        },
        field_descriptor_proto::Type::Bool => match value.as_bool() {
            Some(b) => prost::encoding::bool::encode(tag, &b, buf),
            None => {
                return Err(Error::FieldTypeMismatch {
                    field: field_name.to_string(),
                    expected_type: "BOOL",
                    actual_value: value.to_string(),
                });
            }
        },
        field_descriptor_proto::Type::Bytes => match value.as_str() {
            Some(s) => match base64::engine::general_purpose::STANDARD.decode(s) {
                Ok(decoded) => prost::encoding::bytes::encode(tag, &decoded, buf),
                Err(source) => {
                    return Err(Error::FieldBase64Decode {
                        field: field_name.to_string(),
                        source,
                    });
                }
            },
            None => {
                return Err(Error::FieldTypeMismatch {
                    field: field_name.to_string(),
                    expected_type: "base64 string (BYTES)",
                    actual_value: value.to_string(),
                });
            }
        },
        field_descriptor_proto::Type::Message => {
            return Err(Error::ProtobufEncode(format!(
                "field '{field_name}': nested MESSAGE type must be encoded via encode_nested_value"
            )));
        }
        _ => {
            let s = match value {
                JsonValue::String(s) => s.clone(),
                _ => value.to_string(),
            };
            prost::encoding::string::encode(tag, &s, buf);
        }
    }
    Ok(())
}

/// Encodes a nested RECORD/STRUCT JSON object as a length-delimited protobuf message.
fn encode_nested_value(
    tag: u32,
    value: &JsonValue,
    field_name: &str,
    nested_fields: &[TableFieldSchema],
    buf: &mut Vec<u8>,
) -> Result<(), Error> {
    match value.as_object() {
        Some(_) => {
            let nested_bytes = json_to_proto_bytes(value, nested_fields)?;
            prost::encoding::encode_key(tag, prost::encoding::WireType::LengthDelimited, buf);
            prost::encoding::encode_varint(nested_bytes.len() as u64, buf);
            buf.extend_from_slice(&nested_bytes);
            Ok(())
        }
        None => Err(Error::FieldExpectedObject {
            field: field_name.to_string(),
            actual_value: value.to_string(),
        }),
    }
}

/// Reports fields the encoder will drop: it walks the table's schema, so
/// anything else in the row goes nowhere and nothing says why.
fn report_unwritable_fields(row: &JsonValue, fields: &[TableFieldSchema], table_id: &str) {
    let Some(obj) = row.as_object() else {
        return;
    };
    let columns: HashSet<&str> = fields.iter().map(|f| f.name.as_str()).collect();
    let dropped: Vec<&str> = obj
        .keys()
        .map(String::as_str)
        .filter(|key| !columns.contains(key))
        .collect();
    if !dropped.is_empty() {
        warn!(
            table = %table_id,
            fields = %dropped.join(", "),
            "Dropping fields with no matching column"
        );
    }
}

/// Encodes a JSON value as protobuf bytes according to the table schema.
/// Handles scalar, repeated (ARRAY), and nested RECORD/STRUCT fields.
fn json_to_proto_bytes(data: &JsonValue, fields: &[TableFieldSchema]) -> Result<Vec<u8>, Error> {
    let obj = match data.as_object() {
        Some(obj) => obj,
        None => {
            return Err(Error::ProtobufEncode(
                "expected JSON object at top level".to_string(),
            ));
        }
    };

    let mut buf = Vec::new();

    for (i, field) in fields.iter().enumerate() {
        let tag = (i + 1) as u32;
        let value = match obj.get(&field.name) {
            Some(v) if !v.is_null() => v,
            _ => continue,
        };

        let proto_type = bq_type_to_proto_type(&field.data_type);
        let is_repeated = field.mode == Some(TableFieldMode::Repeated);
        let is_record = matches!(
            field.data_type,
            TableFieldType::Record | TableFieldType::Struct
        );

        match (is_repeated, is_record) {
            (true, true) => match value.as_array() {
                Some(arr) => {
                    let nested_fields = field.fields.as_deref().unwrap_or(&[]);
                    for elem in arr {
                        encode_nested_value(tag, elem, &field.name, nested_fields, &mut buf)?;
                    }
                }
                None => {
                    return Err(Error::FieldTypeMismatch {
                        field: field.name.clone(),
                        expected_type: "JSON array for repeated RECORD",
                        actual_value: value.to_string(),
                    });
                }
            },
            (true, false) => match value.as_array() {
                Some(arr) => {
                    for elem in arr {
                        encode_scalar_value(tag, elem, &field.name, proto_type, &mut buf)?;
                    }
                }
                None => {
                    return Err(Error::FieldTypeMismatch {
                        field: field.name.clone(),
                        expected_type: "JSON array for repeated field",
                        actual_value: value.to_string(),
                    });
                }
            },
            (false, true) => {
                let nested_fields = field.fields.as_deref().unwrap_or(&[]);
                encode_nested_value(tag, value, &field.name, nested_fields, &mut buf)?;
            }
            (false, false) => {
                encode_scalar_value(tag, value, &field.name, proto_type, &mut buf)?;
            }
        }
    }

    Ok(buf)
}

/// Event handler for processing individual storage write events.
pub struct EventHandler {
    stream: Arc<DefaultStream>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::StorageWrite>,
    task_type: &'static str,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    proto_descriptor: DescriptorProto,
    table_fields: Vec<TableFieldSchema>,
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
            // Render config to support templates.
            let event_value = JsonValue::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Encode event data as protobuf rows.
            // Supports both JSON (single row) and Arrow RecordBatch (multiple rows).
            let proto_rows = match &event.data {
                EventData::ArrowRecordBatch(batch) => {
                    let mut rows = Vec::with_capacity(batch.num_rows());
                    let mut json_writer = arrow_json::ArrayWriter::new(Vec::new());
                    json_writer
                        .write(batch)
                        .map_err(|source| Error::Arrow { source })?;
                    json_writer
                        .finish()
                        .map_err(|source| Error::Arrow { source })?;
                    let json_bytes = json_writer.into_inner();
                    let json_rows: Vec<JsonValue> = serde_json::from_slice(&json_bytes)
                        .map_err(|source| Error::ProtoEncode { source })?;

                    if let Some(first) = json_rows.first() {
                        report_unwritable_fields(first, &self.table_fields, &config.table_id);
                    }

                    for row in &json_rows {
                        let mut proto_bytes = json_to_proto_bytes(row, &self.table_fields)?;
                        if let Some(ref change_type) = config.change_type {
                            let tag = (self.table_fields.len() + 1) as u32;
                            prost::encoding::string::encode(
                                tag,
                                &change_type.to_string(),
                                &mut proto_bytes,
                            );
                        }
                        rows.push(proto_bytes);
                    }
                    rows
                }
                _ => {
                    let data = event.data_as_json()?;

                    // Support both single JSON objects and JSON arrays as input.
                    let json_rows: Vec<&JsonValue> = match &data {
                        JsonValue::Array(arr) => arr.iter().collect(),
                        _ => vec![&data],
                    };

                    if let Some(first) = json_rows.first() {
                        report_unwritable_fields(first, &self.table_fields, &config.table_id);
                    }

                    let mut rows = Vec::with_capacity(json_rows.len());
                    for row in json_rows {
                        let mut proto_bytes =
                            json_to_proto_bytes(row, &self.table_fields)?;
                        if let Some(ref change_type) = config.change_type {
                            let tag = (self.table_fields.len() + 1) as u32;
                            prost::encoding::string::encode(
                                tag,
                                &change_type.to_string(),
                                &mut proto_bytes,
                            );
                        }
                        rows.push(proto_bytes);
                    }
                    rows
                }
            };

            // Create append request with proper proto descriptor.
            let mut builder = AppendRowsRequestBuilder::new(
                self.proto_descriptor.clone(),
                proto_rows,
            );
            if let Some(trace_id) = config.trace_id.clone() {
                builder = builder.with_trace_id(trace_id);
            }

            // Append rows to the reusable default write stream.
            let mut responses = self
                .stream
                .append_rows(vec![builder])
                .await
                .map_err(|source| Error::StorageWrite { source })?;

            // Process append response.
            let mut rows_written = 0;
            let mut offset = None;

            while let Some(response_result) = responses.next().await {
                let response = response_result.map_err(|source| Error::StorageWrite { source })?;

                if !response.row_errors.is_empty() {
                    // Log each row error.
                    for row_error in &response.row_errors {
                        error!(
                            row_index = row_error.index,
                            error_code = ?row_error.code,
                            message = ?row_error.message,
                            "Row write failed"
                        );
                    }
                    return Err(Error::RowWriteError);
                }

                // Success - extract offset if available from the response oneof field.
                if let Some(resp) = response.response {
                    use gcloud_googleapis::cloud::bigquery::storage::v1::append_rows_response::Response;
                    match resp {
                        Response::AppendResult(append_result) => {
                            offset = append_result.offset;
                            rows_written += 1;
                        }
                        Response::Error(_) => {
                            // Error case is already captured in row_errors above
                        }
                    }
                }
            }

            // Build result event with write metadata.
            let result = serde_json::json!({
                "rows_written": rows_written,
                "offset": offset,
                "stream_name": config.stream_name.unwrap_or_else(|| format!(
                    "projects/{}/datasets/{}/tables/{}/streams/_default",
                    config.project_id,
                    config.dataset_id,
                    config.table_id
                ))
            });

            let event_builder = EventBuilder::new()
                .data(EventData::Json(result))
                .subject(format!("{}.{}", event.subject, config.name))
                .task_id(self.task_id)
                .task_type(self.task_type);

            let mut result_event = event_builder
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(result_event.data_as_json().ok());
                    }
                }
                Some(_) => {
                    // Pass through completion_tx to next task.
                    result_event.completion_tx = completion_tx_arc.clone();
                }
            }

            result_event
                .send_with_logging(self.tx.as_ref())
                .context("num_records", rows_written)
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }
}

/// BigQuery Storage Write processor that streams data directly to BigQuery tables.
#[derive(Debug)]
pub struct Processor {
    /// Storage Write configuration including credentials and table details.
    config: Arc<super::config::StorageWrite>,
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

    /// Initializes the processor by establishing BigQuery client and default write stream.
    ///
    /// This method performs all setup operations that can fail, including:
    /// - Loading credentials from file
    /// - Creating BigQuery client with authentication
    /// - Creating/getting the default write stream for the target table
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

        // Fetch table schema to build proto descriptor for Storage Write API.
        let table_info = client
            .table()
            .get(
                &init_config.project_id,
                &init_config.dataset_id,
                &init_config.table_id,
            )
            .await
            .map_err(|source| Error::TableMetadata { source })?;

        let table_schema = table_info.schema.ok_or_else(|| Error::MissingSchema)?;
        let proto_descriptor =
            build_proto_descriptor(&table_schema.fields, init_config.change_type.is_some());
        let table_fields = table_schema.fields;

        // Build full table path for default write stream.
        // Default streams provide immediate data availability (suitable for webhooks).
        let table_path = format!(
            "projects/{}/datasets/{}/tables/{}",
            init_config.project_id, init_config.dataset_id, init_config.table_id
        );

        // Get the default storage writer and create the default write stream once.
        // The default stream is reused for all events for efficiency.
        let writer = client.default_storage_writer();
        let stream = Arc::new(
            writer
                .create_write_stream(&table_path)
                .await
                .map_err(|source| Error::StorageWrite { source })?,
        );

        let event_handler = EventHandler {
            stream,
            task_id: self.task_id,
            tx: self.tx.clone(),
            config: Arc::clone(&self.config),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            proto_descriptor,
            table_fields,
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
                        error!(error = %e, "Failed to initialize storage write processor");
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
                                        error!(error = %e, "Failed to write to storage");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(error = %e, "Storage write failed after all retry attempts");
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

/// Builder for creating BigQuery Storage Write processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::StorageWrite>>,
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

    pub fn config(mut self, config: Arc<super::config::StorageWrite>) -> Self {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_error_conversion() {
        let event_err =
            flowgen_core::event::Error::MissingBuilderAttribute("test error".to_string());
        let storage_err: Error = event_err.into();
        assert!(matches!(storage_err, Error::EventData { .. }));
    }

    fn make_field(name: &str, data_type: TableFieldType) -> TableFieldSchema {
        TableFieldSchema {
            name: name.to_string(),
            data_type,
            mode: None,
            fields: None,
            description: None,
            policy_tags: None,
            max_length: None,
            precision: None,
            scale: None,
            rounding_mode: None,
            collation: None,
            default_value_expression: None,
        }
    }

    fn make_repeated_field(name: &str, data_type: TableFieldType) -> TableFieldSchema {
        let mut field = make_field(name, data_type);
        field.mode = Some(TableFieldMode::Repeated);
        field
    }

    fn make_record_field(name: &str, children: Vec<TableFieldSchema>) -> TableFieldSchema {
        let mut field = make_field(name, TableFieldType::Record);
        field.fields = Some(children);
        field
    }

    #[test]
    fn test_bq_type_to_proto_type_all_variants() {
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::String),
            field_descriptor_proto::Type::String
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Int64),
            field_descriptor_proto::Type::Int64
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Integer),
            field_descriptor_proto::Type::Int64
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Float64),
            field_descriptor_proto::Type::Double
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Bool),
            field_descriptor_proto::Type::Bool
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Bytes),
            field_descriptor_proto::Type::Bytes
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Record),
            field_descriptor_proto::Type::Message
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Struct),
            field_descriptor_proto::Type::Message
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Timestamp),
            field_descriptor_proto::Type::String
        );
        assert_eq!(
            bq_type_to_proto_type(&TableFieldType::Numeric),
            field_descriptor_proto::Type::String
        );
    }

    #[test]
    fn test_encode_scalar_string() {
        let mut buf = Vec::new();
        let value = serde_json::json!("hello");
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::String,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_scalar_int64() {
        let mut buf = Vec::new();
        let value = serde_json::json!(42);
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::Int64,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_scalar_int64_type_mismatch() {
        let mut buf = Vec::new();
        let value = serde_json::json!("not_a_number");
        let result = encode_scalar_value(
            1,
            &value,
            "age",
            field_descriptor_proto::Type::Int64,
            &mut buf,
        );
        match result {
            Err(Error::FieldTypeMismatch {
                ref field,
                expected_type,
                ..
            }) => {
                assert_eq!(field, "age");
                assert_eq!(expected_type, "INT64");
            }
            other => panic!("expected FieldTypeMismatch, got {other:?}"),
        }
    }

    #[test]
    fn test_encode_scalar_double() {
        let mut buf = Vec::new();
        let value = serde_json::json!(3.15);
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::Double,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_scalar_double_type_mismatch() {
        let mut buf = Vec::new();
        let value = serde_json::json!("not_a_float");
        let result = encode_scalar_value(
            1,
            &value,
            "price",
            field_descriptor_proto::Type::Double,
            &mut buf,
        );
        assert!(matches!(result, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_encode_scalar_bool() {
        let mut buf = Vec::new();
        let value = serde_json::json!(true);
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::Bool,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_scalar_bool_type_mismatch() {
        let mut buf = Vec::new();
        let value = serde_json::json!(42);
        let result = encode_scalar_value(
            1,
            &value,
            "is_active",
            field_descriptor_proto::Type::Bool,
            &mut buf,
        );
        assert!(matches!(result, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_encode_scalar_bytes_valid_base64() {
        let mut buf = Vec::new();
        let value = serde_json::json!("aGVsbG8=");
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::Bytes,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_encode_scalar_bytes_invalid_base64() {
        let mut buf = Vec::new();
        let value = serde_json::json!("not-valid-base64!!!");
        let result = encode_scalar_value(
            1,
            &value,
            "payload",
            field_descriptor_proto::Type::Bytes,
            &mut buf,
        );
        assert!(matches!(result, Err(Error::FieldBase64Decode { .. })));
    }

    #[test]
    fn test_encode_scalar_bytes_not_string() {
        let mut buf = Vec::new();
        let value = serde_json::json!(42);
        let result = encode_scalar_value(
            1,
            &value,
            "payload",
            field_descriptor_proto::Type::Bytes,
            &mut buf,
        );
        assert!(matches!(result, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_encode_scalar_non_string_to_string() {
        let mut buf = Vec::new();
        let value = serde_json::json!(123);
        encode_scalar_value(
            1,
            &value,
            "test_field",
            field_descriptor_proto::Type::String,
            &mut buf,
        )
        .unwrap();
        assert!(!buf.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_flat_schema() {
        let fields = vec![
            make_field("name", TableFieldType::String),
            make_field("age", TableFieldType::Int64),
            make_field("active", TableFieldType::Bool),
        ];
        let data = serde_json::json!({"name": "Alice", "age": 30, "active": true});
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_skips_null_fields() {
        let fields = vec![
            make_field("name", TableFieldType::String),
            make_field("age", TableFieldType::Int64),
        ];
        let data = serde_json::json!({"name": "Alice", "age": null});
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_skips_missing_fields() {
        let fields = vec![
            make_field("name", TableFieldType::String),
            make_field("age", TableFieldType::Int64),
        ];
        let data = serde_json::json!({"name": "Alice"});
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_type_mismatch_returns_error() {
        let fields = vec![make_field("age", TableFieldType::Int64)];
        let data = serde_json::json!({"age": "not_a_number"});
        let result = json_to_proto_bytes(&data, &fields);
        assert!(matches!(result, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_json_to_proto_bytes_not_object() {
        let fields = vec![make_field("name", TableFieldType::String)];
        let data = serde_json::json!("just a string");
        let result = json_to_proto_bytes(&data, &fields);
        assert!(matches!(result, Err(Error::ProtobufEncode(_))));
    }

    #[test]
    fn test_json_to_proto_bytes_repeated_field() {
        let fields = vec![make_repeated_field("tags", TableFieldType::String)];
        let data = serde_json::json!({"tags": ["a", "b", "c"]});
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_repeated_not_array() {
        let fields = vec![make_repeated_field("tags", TableFieldType::String)];
        let data = serde_json::json!({"tags": "not_an_array"});
        let result = json_to_proto_bytes(&data, &fields);
        assert!(matches!(result, Err(Error::FieldTypeMismatch { .. })));
    }

    #[test]
    fn test_json_to_proto_bytes_nested_record() {
        let fields = vec![
            make_field("id", TableFieldType::Int64),
            make_record_field(
                "address",
                vec![
                    make_field("street", TableFieldType::String),
                    make_field("city", TableFieldType::String),
                ],
            ),
        ];
        let data = serde_json::json!({
            "id": 1,
            "address": {"street": "123 Main St", "city": "Springfield"}
        });
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_nested_record_not_object() {
        let fields = vec![make_record_field(
            "address",
            vec![make_field("street", TableFieldType::String)],
        )];
        let data = serde_json::json!({"address": "not_an_object"});
        let result = json_to_proto_bytes(&data, &fields);
        assert!(matches!(result, Err(Error::FieldExpectedObject { .. })));
    }

    #[test]
    fn test_json_to_proto_bytes_repeated_record() {
        let mut field = make_record_field(
            "items",
            vec![
                make_field("name", TableFieldType::String),
                make_field("qty", TableFieldType::Int64),
            ],
        );
        field.mode = Some(TableFieldMode::Repeated);
        let fields = vec![field];
        let data = serde_json::json!({
            "items": [
                {"name": "widget", "qty": 5},
                {"name": "gadget", "qty": 3}
            ]
        });
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_json_to_proto_bytes_deeply_nested_record() {
        let fields = vec![make_record_field(
            "outer",
            vec![make_record_field(
                "inner",
                vec![make_field("value", TableFieldType::String)],
            )],
        )];
        let data = serde_json::json!({
            "outer": {"inner": {"value": "deep"}}
        });
        let bytes = json_to_proto_bytes(&data, &fields).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_build_proto_descriptor_flat() {
        let fields = vec![
            make_field("name", TableFieldType::String),
            make_field("age", TableFieldType::Int64),
        ];
        let descriptor = build_proto_descriptor(&fields, false);
        assert_eq!(descriptor.name, Some("BqMessage".to_string()));
        assert_eq!(descriptor.field.len(), 2);
        assert!(descriptor.nested_type.is_empty());
    }

    #[test]
    fn test_build_proto_descriptor_with_change_type() {
        let fields = vec![make_field("name", TableFieldType::String)];
        let descriptor = build_proto_descriptor(&fields, true);
        assert_eq!(descriptor.field.len(), 2);
        assert_eq!(descriptor.field[1].name, Some("_CHANGE_TYPE".to_string()));
    }

    #[test]
    fn test_build_proto_descriptor_with_nested_record() {
        let fields = vec![
            make_field("id", TableFieldType::Int64),
            make_record_field(
                "address",
                vec![
                    make_field("street", TableFieldType::String),
                    make_field("city", TableFieldType::String),
                ],
            ),
        ];
        let descriptor = build_proto_descriptor(&fields, false);
        assert_eq!(descriptor.field.len(), 2);
        assert_eq!(descriptor.nested_type.len(), 1);
        assert_eq!(
            descriptor.nested_type[0].name,
            Some("address_nested".to_string())
        );
        assert_eq!(descriptor.nested_type[0].field.len(), 2);
    }

    #[test]
    fn test_build_proto_descriptor_repeated_field_label() {
        let fields = vec![make_repeated_field("tags", TableFieldType::String)];
        let descriptor = build_proto_descriptor(&fields, false);
        assert_eq!(
            descriptor.field[0].label,
            Some(field_descriptor_proto::Label::Repeated.into())
        );
    }

    #[test]
    fn test_field_type_mismatch_error_display() {
        let err = Error::FieldTypeMismatch {
            field: "age".to_string(),
            expected_type: "INT64",
            actual_value: "\"hello\"".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Field 'age': expected INT64, got \"hello\""
        );
    }

    #[test]
    fn test_field_expected_object_error_display() {
        let err = Error::FieldExpectedObject {
            field: "address".to_string(),
            actual_value: "42".to_string(),
        };
        assert_eq!(
            err.to_string(),
            "Field 'address': expected JSON object for RECORD/STRUCT, got 42"
        );
    }
}
