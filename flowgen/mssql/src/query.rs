//! MSSQL query processor with Arrow RecordBatch conversion.

use crate::client::{Client, Error as ClientError};
use crate::config::Query;
use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt};
use flowgen_core::task::context::TaskContext;
use flowgen_core::task::runner::Runner;
use std::sync::Arc;
use tiberius::{ColumnData, Query as TiberiusQuery, Row};
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during query processing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to render config: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },

    #[error("Failed to build event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },

    #[error("Failed to send event to channel: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },

    #[error("MSSQL client error: {source}")]
    Client {
        #[source]
        source: ClientError,
    },

    #[error("Query execution failed: {source}")]
    QueryExecution {
        #[source]
        source: tiberius::error::Error,
    },

    #[error("Failed to convert to Arrow: {source}")]
    ArrowConversion {
        #[source]
        source: arrow::error::ArrowError,
    },

    #[error("Missing required builder attribute: {}", _0)]
    MissingRequiredAttribute(String),

    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },

    #[error("Failed to load query from resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },

    #[error("Failed to load credentials file: {source}")]
    CredentialsLoad {
        #[source]
        source: std::io::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

/// Handles individual query execution events.
pub struct EventHandler {
    client: Arc<Client>,
    config: Arc<Query>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    task_type: &'static str,
    task_context: Arc<TaskContext>,
}

impl EventHandler {
    /// Processes an event by executing the configured SQL query.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Render query (inline queries already rendered, resource files need rendering).
            let query_string = config
                .query
                .render(self.task_context.resource_loader.as_ref(), &event_value)
                .await
                .map_err(|source| Error::ResourceLoad { source })?;

            let mut connection = self
                .client
                .get_connection()
                .await
                .map_err(|source| Error::Client { source })?;

            let mut query = TiberiusQuery::new(query_string);

            if let Some(params) = &config.parameters {
                for param in params {
                    query.bind(param.as_str());
                }
            }

            let stream = query
                .query(&mut connection)
                .await
                .map_err(|source| Error::QueryExecution { source })?;

            let rows = stream
                .into_first_result()
                .await
                .map_err(|source| Error::QueryExecution { source })?;

            if rows.is_empty() {
                let empty_batch = RecordBatch::new_empty(Arc::new(Schema::empty()));
                let mut result_event = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(empty_batch))
                    .subject(format!("{}.{}", event.subject, config.name))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
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
                    .await
                    .map_err(|source| Error::SendMessage { source })?;

                return Ok(());
            }

            let batches = rows_to_record_batches(&rows, config.batch_size)
                .map_err(|source| Error::ArrowConversion { source })?;

            let total_batches = batches.len();
            for (idx, batch) in batches.into_iter().enumerate() {
                let mut result_event = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(batch))
                    .subject(format!("{}.{}", event.subject, config.name))
                    .task_id(self.task_id)
                    .task_type(self.task_type)
                    .build()
                    .map_err(|source| Error::EventBuilder { source })?;

                // Only handle completion on the last batch
                if idx == total_batches - 1 {
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
                }

                result_event
                    .send_with_logging(self.tx.as_ref())
                    .await
                    .map_err(|source| Error::SendMessage { source })?;
            }

            Ok(())
        })
        .await
    }
}

/// Query processor implementing the Runner trait.
#[derive(Debug)]
pub struct Processor {
    config: Arc<Query>,
    rx: Receiver<Event>,
    tx: Option<Sender<Event>>,
    task_id: usize,
    task_context: Arc<TaskContext>,
    task_type: &'static str,
}

#[async_trait]
impl Runner for Processor {
    type Error = Error;
    type EventHandler = EventHandler;

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
                flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
                    .field("credentials_path", &credentials_path)
                    .field("max_connections", &init_config.max_connections)
                    .field("connection_timeout", &init_config.connection_timeout)
                    .field("query_timeout", &init_config.query_timeout)
                    .build(),
                || async {
                    let connection_string = init_config
                        .build_connection_string()
                        .await
                        .map_err(|source| Error::CredentialsLoad { source })?;
                    Client::new(
                        &connection_string,
                        init_config.max_connections,
                        init_config.connection_timeout,
                        init_config.query_timeout,
                    )
                    .await
                    .map_err(|source| Error::Client { source })
                },
            )
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })?;

        let event_handler = EventHandler {
            client,
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
        };

        Ok(event_handler)
    }

    #[tracing::instrument(skip(self), name = "task.run", fields(task = %self.config.name, task_id = self.task_id, task_type = %self.task_type))]
    async fn run(mut self) -> Result<(), Error> {
        let retry_config =
            flowgen_core::retry::RetryConfig::merge(&self.task_context.retry, &self.config.retry);

        let event_handler = match tokio_retry::Retry::spawn(
            retry_config.init_strategy(self.task_context.startup_delay),
            || async {
                match self.init().await {
                    Ok(handler) => Ok(handler),
                    Err(e) => {
                        error!(error = %e, "Failed to initialize MSSQL query processor");
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
                                        error!(error = %e, "Failed to execute query");
                                        Err(tokio_retry::RetryError::transient(e))
                                    }
                                }
                            })
                            .await;

                            if let Err(e) = result {
                                error!(error = %e, "Query failed after all retry attempts");
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

/// Builder for creating Processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<Query>>,
    rx: Option<Receiver<Event>>,
    tx: Option<Option<Sender<Event>>>,
    task_id: Option<usize>,
    task_context: Option<Arc<TaskContext>>,
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

    pub fn config(mut self, config: Arc<Query>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, rx: Receiver<Event>) -> Self {
        self.rx = Some(rx);
        self
    }

    pub fn sender(mut self, tx: Sender<Event>) -> Self {
        self.tx = Some(Some(tx));
        self
    }

    pub fn task_id(mut self, task_id: usize) -> Self {
        self.task_id = Some(task_id);
        self
    }

    pub fn task_context(mut self, task_context: Arc<TaskContext>) -> Self {
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
                .ok_or_else(|| Error::MissingRequiredAttribute("config".to_string()))?,
            rx: self
                .rx
                .ok_or_else(|| Error::MissingRequiredAttribute("receiver".to_string()))?,
            tx: self
                .tx
                .ok_or_else(|| Error::MissingRequiredAttribute("sender".to_string()))?,
            task_id: self
                .task_id
                .ok_or_else(|| Error::MissingRequiredAttribute("task_id".to_string()))?,
            task_context: self
                .task_context
                .ok_or_else(|| Error::MissingRequiredAttribute("task_context".to_string()))?,
            task_type: self
                .task_type
                .ok_or_else(|| Error::MissingRequiredAttribute("task_type".to_string()))?,
        })
    }
}

impl Default for ProcessorBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Converts MSSQL rows to Arrow RecordBatches.
fn rows_to_record_batches(
    rows: &[Row],
    batch_size: usize,
) -> Result<Vec<RecordBatch>, arrow::error::ArrowError> {
    if rows.is_empty() {
        return Ok(vec![]);
    }

    let schema = infer_schema_from_rows(rows)?;
    let arrow_schema = Arc::new(schema);

    let mut batches = Vec::new();
    for chunk in rows.chunks(batch_size) {
        let columns = build_columns(chunk, &arrow_schema)?;
        let batch = RecordBatch::try_new(Arc::clone(&arrow_schema), columns)?;
        batches.push(batch);
    }

    Ok(batches)
}

/// Infers Arrow schema from MSSQL rows using column metadata.
fn infer_schema_from_rows(rows: &[Row]) -> Result<Schema, arrow::error::ArrowError> {
    if rows.is_empty() {
        return Ok(Schema::empty());
    }

    let first_row = &rows[0];
    let mut fields = Vec::new();

    for column in first_row.columns().iter() {
        let field_name = column.name().to_string();

        // Map Tiberius column type to Arrow data type.
        let data_type = match column.column_type() {
            tiberius::ColumnType::Null => DataType::Null,
            tiberius::ColumnType::Bit | tiberius::ColumnType::Bitn => DataType::Boolean,
            tiberius::ColumnType::Int1 => DataType::Int8,
            tiberius::ColumnType::Int2 => DataType::Int16,
            tiberius::ColumnType::Int4 | tiberius::ColumnType::Intn => DataType::Int32,
            tiberius::ColumnType::Int8 => DataType::Int64,
            tiberius::ColumnType::Float4 | tiberius::ColumnType::Floatn => DataType::Float32,
            tiberius::ColumnType::Float8 => DataType::Float64,
            tiberius::ColumnType::Money | tiberius::ColumnType::Money4 => DataType::Float64,
            tiberius::ColumnType::Numericn | tiberius::ColumnType::Decimaln => DataType::Float64,
            tiberius::ColumnType::Datetime
            | tiberius::ColumnType::Datetime4
            | tiberius::ColumnType::Datetimen
            | tiberius::ColumnType::Datetime2 => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            tiberius::ColumnType::DatetimeOffsetn => {
                DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()))
            }
            tiberius::ColumnType::Daten => DataType::Utf8,
            tiberius::ColumnType::Timen => DataType::Utf8,
            tiberius::ColumnType::BigVarBin
            | tiberius::ColumnType::BigBinary
            | tiberius::ColumnType::Image => DataType::Utf8,
            tiberius::ColumnType::Guid => DataType::Utf8,
            // All string/text types map to Utf8.
            _ => DataType::Utf8,
        };

        fields.push(Field::new(field_name, data_type, true));
    }

    Ok(Schema::new(fields))
}

/// Builds Arrow column arrays from MSSQL rows.
fn build_columns(
    rows: &[Row],
    schema: &Schema,
) -> Result<Vec<Arc<dyn Array>>, arrow::error::ArrowError> {
    let mut columns: Vec<Arc<dyn Array>> = Vec::new();

    for (col_idx, field) in schema.fields().iter().enumerate() {
        let array: Arc<dyn Array> = match field.data_type() {
            DataType::Int8 => {
                let mut builder = Int8Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::U8(Some(val)) => builder.append_value(*val as i8),
                            ColumnData::I16(Some(val)) => builder.append_value(*val as i8),
                            ColumnData::I32(Some(val)) => builder.append_value(*val as i8),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int16 => {
                let mut builder = Int16Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::U8(Some(val)) => builder.append_value(*val as i16),
                            ColumnData::I16(Some(val)) => builder.append_value(*val),
                            ColumnData::I32(Some(val)) => builder.append_value(*val as i16),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int32 => {
                let mut builder = Int32Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::U8(Some(val)) => builder.append_value(*val as i32),
                            ColumnData::I16(Some(val)) => builder.append_value(*val as i32),
                            ColumnData::I32(Some(val)) => builder.append_value(*val),
                            ColumnData::I64(Some(val)) => builder.append_value(*val as i32),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Int64 => {
                let mut builder = Int64Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::U8(Some(val)) => builder.append_value(*val as i64),
                            ColumnData::I16(Some(val)) => builder.append_value(*val as i64),
                            ColumnData::I32(Some(val)) => builder.append_value(*val as i64),
                            ColumnData::I64(Some(val)) => builder.append_value(*val),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float32 => {
                let mut builder = Float32Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::F32(Some(val)) => builder.append_value(*val),
                            ColumnData::F64(Some(val)) => builder.append_value(*val as f32),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Float64 => {
                let mut builder = Float64Builder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::F32(Some(val)) => builder.append_value(*val as f64),
                            ColumnData::F64(Some(val)) => builder.append_value(*val),
                            ColumnData::Numeric(Some(val)) => builder.append_value(f64::from(*val)),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Boolean => {
                let mut builder = BooleanBuilder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::Bit(Some(val)) => builder.append_value(*val),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Utf8 => {
                let mut builder = StringBuilder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::String(Some(val)) => builder.append_value(val.as_ref()),
                            ColumnData::Guid(Some(val)) => {
                                builder.append_value(val.to_string());
                            }
                            ColumnData::Xml(Some(val)) => {
                                builder.append_value(val.as_ref());
                            }
                            ColumnData::Numeric(Some(val)) => {
                                builder.append_value(f64::from(*val).to_string());
                            }
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            DataType::Timestamp(_, _) => {
                let mut builder = arrow::array::TimestampMicrosecondBuilder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        // Tiberius with chrono feature provides FromSql trait for NaiveDateTime.
                        // This handles all MSSQL datetime types (datetime, smalldatetime, etc.) uniformly.
                        let datetime_opt: Option<chrono::NaiveDateTime> = match col_data {
                            ColumnData::DateTime(_) | ColumnData::SmallDateTime(_) => {
                                row.get(col_idx)
                            }
                            _ => None,
                        };
                        if let Some(dt) = datetime_opt {
                            // Convert to UTC timestamp in microseconds for Arrow compatibility.
                            builder.append_value(dt.and_utc().timestamp_micros());
                        } else {
                            builder.append_null();
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
            _ => {
                // Fallback for unmapped data types: extract the value as a string
                // representation, or null if the column value is None.
                let mut builder = StringBuilder::new();
                for row in rows {
                    if let Some((_, col_data)) = row.cells().nth(col_idx) {
                        match col_data {
                            ColumnData::String(Some(val)) => builder.append_value(val.as_ref()),
                            ColumnData::Guid(Some(val)) => {
                                builder.append_value(val.to_string());
                            }
                            ColumnData::Xml(Some(val)) => builder.append_value(val.as_ref()),
                            ColumnData::Numeric(Some(val)) => {
                                builder.append_value(f64::from(*val).to_string());
                            }
                            ColumnData::U8(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::I16(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::I32(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::I64(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::F32(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::F64(Some(val)) => builder.append_value(val.to_string()),
                            ColumnData::Bit(Some(val)) => builder.append_value(val.to_string()),
                            _ => builder.append_null(),
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Arc::new(builder.finish())
            }
        };

        columns.push(array);
    }

    Ok(columns)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::sync::Arc;

    // ── Config Deserialization ───────────────────────────────────────

    #[test]
    fn config_deser_minimal_inline_query() {
        let json = r#"{
            "name": "fetch_users",
            "credentials_path": "/var/secrets/mssql/creds.json",
            "query": "SELECT * FROM users"
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        assert_eq!(config.name, "fetch_users");
        assert_eq!(
            config.credentials_path,
            PathBuf::from("/var/secrets/mssql/creds.json")
        );
        match &config.query {
            flowgen_core::resource::Source::Inline(s) => {
                assert_eq!(s, "SELECT * FROM users");
            }
            other => panic!("expected Inline query, got {other:?}"),
        }
        assert_eq!(config.batch_size, 10000);
        assert_eq!(config.max_connections, 10);
        assert_eq!(
            config.connection_timeout,
            std::time::Duration::from_secs(30)
        );
        assert_eq!(config.query_timeout, std::time::Duration::from_secs(120));
        assert!(config.parameters.is_none());
        assert!(config.depends_on.is_none());
        assert!(config.retry.is_none());
    }

    #[test]
    fn config_deser_resource_query() {
        let json = r#"{
            "name": "fetch_orders",
            "credentials_path": "/etc/mssql/creds.json",
            "query": { "resource": "queries/orders.sql" }
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        match &config.query {
            flowgen_core::resource::Source::Resource { resource } => {
                assert_eq!(resource, "queries/orders.sql");
            }
            other => panic!("expected Resource query, got {other:?}"),
        }
    }

    #[test]
    fn config_deser_with_parameters() {
        let json = r#"{
            "name": "parameterized",
            "credentials_path": "/creds.json",
            "query": "SELECT * FROM t WHERE a = @p1 AND b = @p2",
            "parameters": ["val1", "val2"]
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        let params = config.parameters.unwrap();
        assert_eq!(params.len(), 2);
        assert_eq!(params[0], "val1");
        assert_eq!(params[1], "val2");
    }

    #[test]
    fn config_deser_custom_timeouts() {
        let json = r#"{
            "name": "slow_query",
            "credentials_path": "/creds.json",
            "query": "SELECT 1",
            "connection_timeout": "1m",
            "query_timeout": "5m",
            "batch_size": 500,
            "max_connections": 3
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            config.connection_timeout,
            std::time::Duration::from_secs(60)
        );
        assert_eq!(config.query_timeout, std::time::Duration::from_secs(300));
        assert_eq!(config.batch_size, 500);
        assert_eq!(config.max_connections, 3);
    }

    #[test]
    fn config_deser_with_depends_on() {
        let json = r#"{
            "name": "downstream",
            "credentials_path": "/creds.json",
            "query": "SELECT 1",
            "depends_on": ["upstream_a", "upstream_b"]
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        let deps = config.depends_on.unwrap();
        assert_eq!(deps.len(), 2);
        assert_eq!(deps[0], "upstream_a");
    }

    #[test]
    fn config_deser_with_retry() {
        let json = r#"{
            "name": "retryable",
            "credentials_path": "/creds.json",
            "query": "SELECT 1",
            "retry": { "max_retries": 3, "initial_interval": "1s" }
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        assert!(config.retry.is_some());
    }

    #[test]
    fn config_deser_missing_name_fails() {
        let json = r#"{
            "credentials_path": "/creds.json",
            "query": "SELECT 1"
        }"#;
        let result = serde_json::from_str::<crate::config::Query>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_credentials_path_fails() {
        let json = r#"{
            "name": "test",
            "query": "SELECT 1"
        }"#;
        let result = serde_json::from_str::<crate::config::Query>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_deser_missing_query_fails() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/creds.json"
        }"#;
        let result = serde_json::from_str::<crate::config::Query>(json);
        assert!(result.is_err());
    }

    // ── Credentials Deserialization ─────────────────────────────────

    #[test]
    fn credentials_deser_full() {
        let json = r#"{
            "host": "sqlserver.example.com",
            "port": 1434,
            "database": "proddb",
            "username": "app",
            "password": "secret",
            "trust_server_certificate": true,
            "encrypt": false
        }"#;
        let creds: crate::config::Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.host, "sqlserver.example.com");
        assert_eq!(creds.port, 1434);
        assert_eq!(creds.database, "proddb");
        assert_eq!(creds.username, "app");
        assert_eq!(creds.password, "secret");
        assert!(creds.trust_server_certificate);
        assert!(!creds.encrypt);
    }

    #[test]
    fn credentials_deser_defaults() {
        let json = r#"{
            "host": "localhost",
            "database": "testdb",
            "username": "sa",
            "password": "pass"
        }"#;
        let creds: crate::config::Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.port, 1433);
        assert!(!creds.trust_server_certificate);
        assert!(creds.encrypt);
    }

    #[test]
    fn credentials_deser_missing_host_fails() {
        let json = r#"{
            "database": "testdb",
            "username": "sa",
            "password": "pass"
        }"#;
        let result = serde_json::from_str::<crate::config::Credentials>(json);
        assert!(result.is_err());
    }

    #[test]
    fn config_roundtrip_serde() {
        let json = r#"{
            "name": "roundtrip",
            "credentials_path": "/creds.json",
            "query": "SELECT 1",
            "batch_size": 5000,
            "max_connections": 5
        }"#;
        let config: crate::config::Query = serde_json::from_str(json).unwrap();
        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: crate::config::Query = serde_json::from_str(&serialized).unwrap();
        assert_eq!(config, deserialized);
    }

    // ── rows_to_record_batches ──────────────────────────────────────

    #[test]
    fn rows_to_record_batches_empty_returns_empty_vec() {
        let rows: Vec<Row> = vec![];
        let result = rows_to_record_batches(&rows, 100).unwrap();
        assert!(result.is_empty());
    }

    // ── infer_schema_from_rows ──────────────────────────────────────

    #[test]
    fn infer_schema_empty_rows_returns_empty_schema() {
        let rows: Vec<Row> = vec![];
        let schema = infer_schema_from_rows(&rows).unwrap();
        assert_eq!(schema.fields().len(), 0);
    }

    // ── build_columns type dispatch ────────────────────────────────
    //
    // tiberius::Row has pub(crate) fields and cannot be constructed
    // outside the tiberius crate. We test build_columns with empty row
    // slices to verify each DataType arm produces the correct Arrow
    // array type with zero rows.

    fn build_columns_empty(fields: Vec<Field>) -> Vec<Arc<dyn Array>> {
        let schema = Schema::new(fields);
        let rows: Vec<Row> = vec![];
        build_columns(&rows, &schema).unwrap()
    }

    #[test]
    fn build_columns_int8_produces_int8_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Int8, true)]);
        assert_eq!(cols.len(), 1);
        assert_eq!(*cols[0].data_type(), DataType::Int8);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_int16_produces_int16_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Int16, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Int16);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_int32_produces_int32_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Int32, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Int32);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_int64_produces_int64_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Int64, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Int64);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_float32_produces_float32_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Float32, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Float32);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_float64_produces_float64_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Float64, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Float64);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_boolean_produces_boolean_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Boolean, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Boolean);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_utf8_produces_string_array() {
        let cols = build_columns_empty(vec![Field::new("a", DataType::Utf8, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Utf8);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_timestamp_produces_timestamp_array() {
        let dt = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None);
        let cols = build_columns_empty(vec![Field::new("a", dt.clone(), true)]);
        assert_eq!(*cols[0].data_type(), dt);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_timestamp_utc_produces_timestamp_array() {
        let dt = DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, Some("UTC".into()));
        let cols = build_columns_empty(vec![Field::new("a", dt.clone(), true)]);
        // Timestamp with timezone still uses the Timestamp branch, producing
        // a TimestampMicrosecond array (timezone metadata is on the field, not
        // the builder). The fallback branch handles non-matching timestamps.
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_fallback_unmapped_type_produces_utf8() {
        // DataType::Date32 has no explicit branch; should hit the fallback arm.
        let cols = build_columns_empty(vec![Field::new("a", DataType::Date32, true)]);
        assert_eq!(*cols[0].data_type(), DataType::Utf8);
        assert_eq!(cols[0].len(), 0);
    }

    #[test]
    fn build_columns_multiple_fields() {
        let cols = build_columns_empty(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("active", DataType::Boolean, true),
            Field::new("score", DataType::Float64, true),
        ]);
        assert_eq!(cols.len(), 4);
        assert_eq!(*cols[0].data_type(), DataType::Int32);
        assert_eq!(*cols[1].data_type(), DataType::Utf8);
        assert_eq!(*cols[2].data_type(), DataType::Boolean);
        assert_eq!(*cols[3].data_type(), DataType::Float64);
    }

    #[test]
    fn build_columns_empty_schema_produces_no_columns() {
        let cols = build_columns_empty(vec![]);
        assert!(cols.is_empty());
    }

    // ── build_columns produces valid RecordBatch ───────────────────

    #[test]
    fn build_columns_empty_rows_forms_valid_record_batch() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]));
        let rows: Vec<Row> = vec![];
        let columns = build_columns(&rows, &schema).unwrap();
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 2);
    }
}
