//! BigQuery query processor for executing SQL queries.
//!
//! Handles BigQuery query execution with parameterized queries for SQL injection
//! protection. Processes events by executing queries and publishing results as
//! new events in Arrow RecordBatch format for efficient columnar processing.

use arrow::array::*;
use arrow::datatypes::*;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};

use google_cloud_bigquery::client::{Client, ClientConfig};
use google_cloud_bigquery::http::job::get_query_results::{
    GetQueryResultsRequest, GetQueryResultsResponse,
};
use google_cloud_bigquery::http::job::query::{QueryRequest, QueryResponse};
use google_cloud_bigquery::http::table::TableFieldSchema;
use google_cloud_bigquery::http::tabledata::list::{Tuple, Value};
use google_cloud_bigquery::http::types::{QueryParameter, QueryParameterType, QueryParameterValue};
use serde_json::Value as JsonValue;
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, Instrument};

/// Errors that can occur during BigQuery query operations.
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
    #[error("BigQuery query error: {source}")]
    QueryExecution {
        #[source]
        source: google_cloud_bigquery::http::error::Error,
    },
    #[error("BigQuery Storage Read query error: {source}")]
    StorageQueryExecution {
        #[source]
        source: google_cloud_bigquery::client::QueryError,
    },
    #[error("BigQuery Storage Read stream error: {source}")]
    StorageRead {
        #[source]
        source: google_cloud_bigquery::storage::Error,
    },
    #[error("BigQuery response missing schema")]
    MissingSchema,
    #[error("Arrow error: {source}")]
    Arrow {
        #[source]
        source: arrow::error::ArrowError,
    },
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Error parsing BigQuery date value: {source}")]
    DateParse {
        #[source]
        source: chrono::ParseError,
    },
    #[error("Error parsing BigQuery time value: {source}")]
    TimeParse {
        #[source]
        source: chrono::ParseError,
    },
    #[error("Invalid date/time value")]
    InvalidDateTime,
    #[error("Error loading resource: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error(
        "Client registry type mismatch — same credentials used with incompatible client types"
    )]
    ClientRegistryMismatch,
}

impl Error {
    /// True for errors that retrying will not fix: bad SQL, a missing
    /// table, or a permission error all come back as a BigQuery REST 4xx
    /// (429 excluded — that is rate limiting, which is transient).
    fn is_permanent(&self) -> bool {
        match self {
            Error::QueryExecution { source } => is_permanent_http_error(source),
            Error::StorageQueryExecution {
                source: google_cloud_bigquery::client::QueryError::JobHttp(source),
            } => is_permanent_http_error(source),
            _ => false,
        }
    }
}

fn is_permanent_http_error(source: &google_cloud_bigquery::http::error::Error) -> bool {
    match source {
        google_cloud_bigquery::http::error::Error::Response(response) => {
            (400..500).contains(&response.code) && response.code != 429
        }
        _ => false,
    }
}

/// Event handler for processing individual query events.
pub struct EventHandler {
    client: Arc<Client>,
    task_id: usize,
    tx: Option<Sender<Event>>,
    config: Arc<super::config::Query>,
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

            // Stream query result pages instead of buffering the whole table.
            // Peek one page ahead so the final page can carry completion_tx.
            let mut pages = QueryStream::start(
                &self.client,
                &config,
                self.task_context.resource_loader.as_ref(),
                &event_value,
            )
            .await?;

            let mut pending = pages.next_batch().await?;
            // Empty result set still owes downstream one event so the
            // buffer/diff/leaf pipeline observes end-of-batch. Storage Read
            // returns `None` on the very first call for an empty table;
            // REST always returns a first (possibly empty) page.
            if pending.is_none() {
                pending = Some(arrow::array::RecordBatch::new_empty(Arc::new(
                    arrow::datatypes::Schema::empty(),
                )));
            }
            let mut is_first = true;
            loop {
                let batch = match pending.take() {
                    Some(b) => b,
                    None => break,
                };
                let next = pages.next_batch().await?;
                let is_last = next.is_none();
                let num_rows = batch.num_rows();

                let mut event_builder = EventBuilder::new()
                    .data(EventData::ArrowRecordBatch(batch))
                    .subject(format!("{}.{}", event.subject, config.name))
                    .task_id(self.task_id)
                    .task_type(self.task_type);

                // Only stamp the job_id onto the first emitted event.
                if is_first {
                    if let Some(id) = pages.job_id() {
                        event_builder = event_builder.id(id.to_string());
                    }
                    is_first = false;
                }

                let mut result_event = event_builder
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
                    .context("num_rows", num_rows)
                    .await
                    .map_err(|source| Error::SendMessage { source })?;

                pending = next;
            }

            Ok(())
        })
        .await
    }
}

/// BigQuery query processor that executes SQL queries and publishes results as Arrow RecordBatch.
#[derive(Debug)]
pub struct Processor {
    /// Query configuration including credentials and SQL query.
    config: Arc<super::config::Query>,
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
                        error!(error = %e, "Failed to initialize query processor");
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
                                    Err(e) if e.is_permanent() => {
                                        error!(error = %e, "Query failed with a permanent error, skipping retries");
                                        Err(tokio_retry::RetryError::permanent(e))
                                    }
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

/// Builder for creating BigQuery query processor instances.
pub struct ProcessorBuilder {
    config: Option<Arc<super::config::Query>>,
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

    pub fn config(mut self, config: Arc<super::config::Query>) -> Self {
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

/// Stream of `RecordBatch`es from a BigQuery query, backed by either the
/// REST `getQueryResults` pagination path or the Storage Read API. Both
/// variants emit one Arrow batch per iteration so the handler can stream
/// events without buffering the whole result set.
enum QueryResultStream<'a> {
    /// Paginated REST results. One event per `getQueryResults` page.
    Rest {
        client: &'a Client,
        job_ref: google_cloud_bigquery::http::job::JobReference,
        schema: Option<google_cloud_bigquery::http::table::TableSchema>,
        next_rows: Option<Vec<Tuple>>,
        page_token: Option<String>,
    },
    /// BigQuery Storage Read backend. One event per Arrow batch
    /// returned by the wire. Boxed because `RecordBatchIterator`
    /// carries a full gRPC stream handle (~840 B); keeping it inline
    /// would bloat the REST variant too (`clippy::large_enum_variant`).
    Storage {
        iter: Box<google_cloud_bigquery::storage::RecordBatchIterator>,
    },
}

/// Wraps a `QueryResultStream` with the job_id extracted at start time so
/// the handler can stamp it on the first emitted event.
struct QueryStream<'a> {
    inner: QueryResultStream<'a>,
    job_id: Option<String>,
}

impl<'a> QueryStream<'a> {
    async fn start(
        client: &'a Client,
        config: &super::config::Query,
        resource_loader: Option<&flowgen_core::resource::ResourceLoader>,
        event_value: &serde_json::Value,
    ) -> Result<QueryStream<'a>, Error> {
        let query_string = config
            .query
            .render(resource_loader, event_value)
            .await
            .map_err(|source| Error::ResourceLoad { source })?;

        let mut query_request = QueryRequest {
            query: query_string,
            use_legacy_sql: config.use_legacy_sql,
            use_query_cache: Some(config.use_query_cache),
            ..Default::default()
        };

        if let Some(ref params) = config.parameters {
            let mut built = Vec::with_capacity(params.len());
            for (name, source) in params {
                let value = resolve_parameter_value(source, resource_loader, event_value).await?;
                built.push(build_query_parameter(name, &value)?);
            }
            query_request.query_parameters = built;
        }

        if let Some(timeout) = config.timeout {
            query_request.timeout_ms = Some(timeout.as_millis() as i64);
        }
        if let Some(max) = config.max_results {
            query_request.max_results = Some(max as i64);
        }
        if let Some(ref location) = config.location {
            query_request.location = location.clone();
        }
        if let Some(create_session) = config.create_session {
            query_request.create_session = Some(create_session);
        }
        if let Some(ref labels) = config.labels {
            query_request.labels = Some(labels.clone());
        }

        if config.use_storage_read {
            return Self::start_storage(client, config, query_request).await;
        }

        Self::start_rest(client, config, query_request).await
    }

    async fn start_rest(
        client: &'a Client,
        config: &super::config::Query,
        query_request: QueryRequest,
    ) -> Result<QueryStream<'a>, Error> {
        let response: QueryResponse = client
            .job()
            .query(config.get_job_project_id(), &query_request)
            .await
            .map_err(|source| Error::QueryExecution { source })?;

        let (schema, mut job_ref, first_rows, page_token) = if !response.job_complete {
            let result = poll_query_results(client, &response).await?;
            (
                result.schema,
                result.job_reference,
                result.rows.unwrap_or_default(),
                result.page_token,
            )
        } else {
            (
                response.schema,
                response.job_reference,
                response.rows.unwrap_or_default(),
                response.page_token,
            )
        };

        let job_id = if !job_ref.job_id.is_empty() {
            Some(std::mem::take(&mut job_ref.job_id))
        } else {
            None
        };
        if let Some(ref id) = job_id {
            job_ref.job_id = id.clone();
        }

        Ok(QueryStream {
            inner: QueryResultStream::Rest {
                client,
                job_ref,
                schema,
                next_rows: Some(first_rows),
                page_token,
            },
            job_id,
        })
    }

    async fn start_storage(
        client: &'a Client,
        config: &super::config::Query,
        query_request: QueryRequest,
    ) -> Result<QueryStream<'a>, Error> {
        use google_cloud_bigquery::query::QueryOption;

        let iter = client
            .query_record_batches(
                config.get_job_project_id(),
                query_request,
                QueryOption::default(),
            )
            .await
            .map_err(|source| Error::StorageQueryExecution { source })?;

        Ok(QueryStream {
            inner: QueryResultStream::Storage {
                iter: Box::new(iter),
            },
            // Storage Read path does not surface the parent job_id to the
            // handler; downstream callers that key on it must use REST.
            job_id: None,
        })
    }

    fn job_id(&self) -> Option<&str> {
        self.job_id.as_deref()
    }

    /// Returns the next `RecordBatch`, or `None` when the stream is drained.
    /// For the REST backend, the first call emits the initial rows even if
    /// they're empty so downstream still sees an end-of-batch event on empty
    /// result sets.
    async fn next_batch(&mut self) -> Result<Option<arrow::array::RecordBatch>, Error> {
        match &mut self.inner {
            QueryResultStream::Rest {
                client,
                job_ref,
                schema,
                next_rows,
                page_token,
            } => {
                let rows = match next_rows.take() {
                    Some(r) => r,
                    None => return Ok(None),
                };

                if let Some(token) = page_token.take() {
                    let page = get_query_results_page(client, job_ref, &token).await?;
                    *next_rows = Some(page.rows.unwrap_or_default());
                    *page_token = page.page_token;
                }

                let batch = response_to_record_batch(schema.clone(), rows)?;
                Ok(Some(batch))
            }
            QueryResultStream::Storage { iter } => iter
                .next()
                .await
                .map_err(|source| Error::StorageRead { source }),
        }
    }
}

/// Poll for query completion using getQueryResults API.
///
/// This function polls BigQuery to check if an asynchronous query job has completed.
/// Each call to getQueryResults has a 10-second server-side timeout, meaning BigQuery
/// waits up to 10 seconds for the job to complete before returning. If the job is not
/// complete, we wait 1 second before the next poll attempt.
///
/// Polling continues indefinitely until either the job completes or BigQuery returns
/// an error. Any errors from the BigQuery API will propagate up and be handled by the
/// retry system at the handler level.
async fn poll_query_results(
    client: &Client,
    initial_response: &QueryResponse,
) -> Result<GetQueryResultsResponse, Error> {
    let job_ref = &initial_response.job_reference;
    let poll_interval = std::time::Duration::from_secs(1);

    let request = GetQueryResultsRequest {
        start_index: 0,
        page_token: None,
        max_results: None,
        timeout_ms: Some(10000), // Server-side timeout: wait up to 10s for job completion.
        location: job_ref.location.clone(),
        format_options: None,
    };

    loop {
        let result_response = client
            .job()
            .get_query_results(&job_ref.project_id, &job_ref.job_id, &request)
            .await
            .map_err(|source| Error::QueryExecution { source })?;

        if result_response.job_complete {
            return Ok(result_response);
        }

        // Wait before next poll attempt.
        tokio::time::sleep(poll_interval).await;
    }
}

/// Fetch a single page of query results using pageToken.
async fn get_query_results_page(
    client: &Client,
    job_ref: &google_cloud_bigquery::http::job::JobReference,
    page_token: &str,
) -> Result<GetQueryResultsResponse, Error> {
    let request = GetQueryResultsRequest {
        start_index: 0,
        page_token: Some(page_token.to_string()),
        max_results: None,
        timeout_ms: None,
        location: job_ref.location.clone(),
        format_options: None,
    };

    client
        .job()
        .get_query_results(&job_ref.project_id, &job_ref.job_id, &request)
        .await
        .map_err(|source| Error::QueryExecution { source })
}

/// Convert BigQuery schema and rows to Arrow RecordBatch.
fn response_to_record_batch(
    schema: Option<google_cloud_bigquery::http::table::TableSchema>,
    rows: Vec<Tuple>,
) -> Result<arrow::array::RecordBatch, Error> {
    // Data-definition and data-manipulation statements such as
    // `CREATE TABLE` or `INSERT` do not return a schema. Emit an empty
    // `RecordBatch` in that case so downstream still observes an event.
    let bq_schema = match schema {
        Some(s) => s,
        None => {
            return Ok(arrow::array::RecordBatch::new_empty(Arc::new(
                Schema::empty(),
            )));
        }
    };

    // Convert BigQuery schema to Arrow schema.
    let arrow_fields: Vec<Field> = bq_schema.fields.iter().map(field_to_arrow).collect();
    let arrow_schema = Arc::new(Schema::new(arrow_fields));

    if rows.is_empty() {
        // Return empty batch with correct schema.
        return Ok(arrow::array::RecordBatch::new_empty(arrow_schema));
    }

    // Build columns.
    let columns: Vec<ArrayRef> = (0..bq_schema.fields.len())
        .map(|col_idx| {
            let field = &bq_schema.fields[col_idx];
            build_column(&rows, col_idx, field)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Create RecordBatch from query results.
    arrow::array::RecordBatch::try_new(arrow_schema, columns)
        .map_err(|source| Error::Arrow { source })
}

/// Convert BigQuery field to Arrow field.
fn field_to_arrow(field: &TableFieldSchema) -> Field {
    use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};

    let dtype = match &field.data_type {
        TableFieldType::String => DataType::Utf8,
        TableFieldType::Int64 | TableFieldType::Integer => DataType::Int64,
        TableFieldType::Float64 | TableFieldType::Float => DataType::Float64,
        TableFieldType::Bool | TableFieldType::Boolean => DataType::Boolean,
        TableFieldType::Timestamp => DataType::Timestamp(TimeUnit::Microsecond, None),
        TableFieldType::Date => DataType::Date32,
        TableFieldType::Time => DataType::Time64(TimeUnit::Microsecond),
        TableFieldType::Bytes => DataType::Binary,
        TableFieldType::Numeric => DataType::Decimal128(38, 9),
        TableFieldType::Bignumeric => DataType::Decimal256(76, 38),
        TableFieldType::Datetime => DataType::Utf8,
        TableFieldType::Json => DataType::Utf8,
        _ => DataType::Utf8,
    };

    let nullable = field.mode != Some(TableFieldMode::Required);
    Field::new(&field.name, dtype, nullable)
}

/// Build a single Arrow column from BigQuery rows.
fn build_column(
    rows: &[Tuple],
    col_idx: usize,
    field: &TableFieldSchema,
) -> Result<ArrayRef, Error> {
    use google_cloud_bigquery::http::table::TableFieldType;

    Ok(match &field.data_type {
        TableFieldType::String => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Int64 | TableFieldType::Integer => {
            let mut builder = Int64Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<i64>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Float64 | TableFieldType::Float => {
            let mut builder = Float64Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<f64>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bool | TableFieldType::Boolean => {
            let mut builder = BooleanBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<bool>() {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Timestamp => {
            let mut builder = TimestampMicrosecondBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match s.parse::<f64>() {
                        Ok(seconds) => {
                            let micros = (seconds * 1_000_000.0) as i64;
                            builder.append_value(micros);
                        }
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Date => {
            let mut builder = Date32Builder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        // BigQuery returns dates as "YYYY-MM-DD".
                        // Date32 is days since Unix epoch (1970-01-01).
                        match parse_date_to_days(s) {
                            Ok(days) => builder.append_value(days),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Time => {
            let mut builder = Time64MicrosecondBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        // BigQuery returns time as "HH:MM:SS[.ffffff]".
                        // Time64 is microseconds since midnight.
                        match parse_time_to_micros(s) {
                            Ok(micros) => builder.append_value(micros),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bytes => {
            let mut builder = BinaryBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => {
                        match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, s)
                        {
                            Ok(bytes) => builder.append_value(&bytes),
                            Err(_) => builder.append_null(),
                        }
                    }
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Numeric => {
            let mut builder = Decimal128Builder::new()
                .with_precision_and_scale(38, 9)
                .map_err(|e| Error::Arrow { source: e })?;
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match parse_decimal_128(s, 9) {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Bignumeric => {
            let mut builder = Decimal256Builder::new()
                .with_precision_and_scale(76, 38)
                .map_err(|e| Error::Arrow { source: e })?;
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => match parse_decimal_256(s, 38) {
                        Ok(val) => builder.append_value(val),
                        Err(_) => builder.append_null(),
                    },
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        TableFieldType::Datetime | TableFieldType::Json => {
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
        _ => {
            // Fallback to string for unknown types.
            let mut builder = StringBuilder::new();
            for row in rows {
                match &row.f[col_idx].v {
                    Value::Null => builder.append_null(),
                    Value::String(s) => builder.append_value(s),
                    _ => builder.append_null(),
                }
            }
            Arc::new(builder.finish()) as ArrayRef
        }
    })
}

/// Parse BigQuery NUMERIC string to Decimal128.
fn parse_decimal_128(decimal_str: &str, scale: i8) -> Result<i128, Box<dyn std::error::Error>> {
    let value: f64 = decimal_str.parse()?;
    let multiplier = 10_f64.powi(scale as i32);
    Ok((value * multiplier).round() as i128)
}

/// Parse BigQuery BIGNUMERIC string to Decimal256.
fn parse_decimal_256(
    decimal_str: &str,
    scale: i8,
) -> Result<arrow::datatypes::i256, Box<dyn std::error::Error>> {
    use arrow::datatypes::i256;
    let value: f64 = decimal_str.parse()?;
    let multiplier = 10_f64.powi(scale as i32);
    let scaled = (value * multiplier).round() as i128;
    Ok(i256::from_i128(scaled))
}

/// Parse BigQuery date string (YYYY-MM-DD) to days since Unix epoch.
fn parse_date_to_days(date_str: &str) -> Result<i32, Error> {
    use chrono::NaiveDate;
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|source| Error::DateParse { source })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).ok_or(Error::InvalidDateTime)?;
    Ok((date - epoch).num_days() as i32)
}

/// Parse BigQuery time string (HH:MM:SS[.ffffff]) to microseconds since midnight.
fn parse_time_to_micros(time_str: &str) -> Result<i64, Error> {
    use chrono::NaiveTime;
    let time = NaiveTime::parse_from_str(time_str, "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(time_str, "%H:%M:%S"))
        .map_err(|source| Error::TimeParse { source })?;

    let midnight = NaiveTime::from_hms_opt(0, 0, 0).ok_or(Error::InvalidDateTime)?;
    let duration = time - midnight;
    duration.num_microseconds().ok_or(Error::InvalidDateTime)
}

/// Build a query parameter from name and JSON value.
/// Resolves a parameter to its value, loading and parsing the file when the
/// parameter names a resource. A resource that does not parse as JSON is
/// passed through as a string, so a plain text file still works.
async fn resolve_parameter_value(
    source: &super::config::QueryParameterSource,
    resource_loader: Option<&flowgen_core::resource::ResourceLoader>,
    event_value: &JsonValue,
) -> Result<JsonValue, Error> {
    match source {
        super::config::QueryParameterSource::Literal(value) => Ok(value.clone()),
        super::config::QueryParameterSource::Resource(source) => {
            let content = flowgen_core::resource::Source::Resource {
                resource: source.resource.clone(),
            }
            .render(resource_loader, event_value)
            .await
            .map_err(|source| Error::ResourceLoad { source })?;
            Ok(serde_json::from_str(&content).unwrap_or(JsonValue::String(content)))
        }
    }
}

fn build_query_parameter(name: &str, value: &JsonValue) -> Result<QueryParameter, Error> {
    use super::config::{
        PARAM_TYPE_BOOL, PARAM_TYPE_FLOAT64, PARAM_TYPE_INT64, PARAM_TYPE_JSON, PARAM_TYPE_STRING,
    };

    let (parameter_type, parameter_value) = match value {
        JsonValue::String(s) => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_STRING.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(s.clone()),
                ..Default::default()
            },
        ),
        JsonValue::Number(n) => {
            if n.is_i64() || n.is_u64() {
                (
                    QueryParameterType {
                        parameter_type: PARAM_TYPE_INT64.to_string(),
                        ..Default::default()
                    },
                    QueryParameterValue {
                        value: Some(n.to_string()),
                        ..Default::default()
                    },
                )
            } else {
                (
                    QueryParameterType {
                        parameter_type: PARAM_TYPE_FLOAT64.to_string(),
                        ..Default::default()
                    },
                    QueryParameterValue {
                        value: Some(n.to_string()),
                        ..Default::default()
                    },
                )
            }
        }
        JsonValue::Bool(b) => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_BOOL.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(b.to_string()),
                ..Default::default()
            },
        ),
        JsonValue::Null => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_STRING.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: None,
                ..Default::default()
            },
        ),
        // Arrays and Objects are passed as JSON type to avoid PARSE_JSON() in SQL.
        JsonValue::Array(_) | JsonValue::Object(_) => (
            QueryParameterType {
                parameter_type: PARAM_TYPE_JSON.to_string(),
                ..Default::default()
            },
            QueryParameterValue {
                value: Some(
                    serde_json::to_string(value).map_err(|source| Error::SerdeJson { source })?,
                ),
                ..Default::default()
            },
        ),
    };

    Ok(QueryParameter {
        name: Some(name.to_string()),
        parameter_type,
        parameter_value,
    })
}

#[cfg(test)]
mod tests {
    use super::super::config::{
        PARAM_TYPE_BOOL, PARAM_TYPE_FLOAT64, PARAM_TYPE_INT64, PARAM_TYPE_STRING,
    };
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn parameter_from_resource_is_loaded_and_parsed() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("account_schema.json"),
            r#"{"fields":[{"name":"Id","type":"STRING"},{"name":"Amount","type":"NUMERIC"}]}"#,
        )
        .expect("write schema");
        let loader = flowgen_core::resource::ResourceLoader::new(Some(dir.path().to_path_buf()));

        let source = super::super::config::QueryParameterSource::Resource(
            super::super::config::QueryParameterResource {
                resource: "account_schema.json".to_string(),
            },
        );
        let value = resolve_parameter_value(&source, Some(&loader), &json!({}))
            .await
            .expect("resource parameter resolves");

        let fields = value
            .get("fields")
            .and_then(|f| f.as_array())
            .expect("parsed as JSON, not passed through as a string");
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].get("name").unwrap(), "Id");

        let param = build_query_parameter("schema", &value).expect("builds parameter");
        assert_eq!(
            param.parameter_type.parameter_type,
            super::super::config::PARAM_TYPE_JSON,
            "bound as JSON so SQL can walk it with JSON_QUERY_ARRAY"
        );
    }

    #[tokio::test]
    async fn literal_parameter_is_passed_through_unchanged() {
        let source = super::super::config::QueryParameterSource::Literal(json!("CUST-1"));
        let value = resolve_parameter_value(&source, None, &json!({}))
            .await
            .expect("literal resolves without a loader");
        assert_eq!(value, json!("CUST-1"));
    }

    #[test]
    fn test_parse_date_to_days() {
        assert_eq!(parse_date_to_days("1970-01-01").unwrap(), 0);
        assert_eq!(parse_date_to_days("1970-01-02").unwrap(), 1);
        assert_eq!(parse_date_to_days("2024-01-01").unwrap(), 19723);
        assert!(parse_date_to_days("invalid").is_err());
        assert!(parse_date_to_days("2024-13-01").is_err());
    }

    #[test]
    fn test_parse_time_to_micros() {
        assert_eq!(parse_time_to_micros("00:00:00").unwrap(), 0);
        assert_eq!(parse_time_to_micros("00:00:01").unwrap(), 1_000_000);
        assert_eq!(parse_time_to_micros("01:00:00").unwrap(), 3_600_000_000);
        assert_eq!(parse_time_to_micros("23:59:59").unwrap(), 86_399_000_000);
        assert_eq!(
            parse_time_to_micros("12:30:45.123456").unwrap(),
            45_045_123_456
        );
        assert!(parse_time_to_micros("invalid").is_err());
        assert!(parse_time_to_micros("25:00:00").is_err());
    }

    #[test]
    fn test_parse_decimal_128() {
        assert_eq!(parse_decimal_128("123.45", 2).unwrap(), 12345);
        assert_eq!(parse_decimal_128("0.001", 3).unwrap(), 1);
        assert_eq!(parse_decimal_128("1000.5", 1).unwrap(), 10005);
        assert!(parse_decimal_128("invalid", 2).is_err());
    }

    #[test]
    fn test_parse_decimal_256() {
        let result = parse_decimal_256("123.45", 2).unwrap();
        assert_eq!(result, arrow::datatypes::i256::from_i128(12345));
        assert!(parse_decimal_256("invalid", 2).is_err());
    }

    #[test]
    fn test_build_query_parameter_string() {
        let param = build_query_parameter("name", &json!("test")).unwrap();
        assert_eq!(param.name, Some("name".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_STRING);
        assert_eq!(param.parameter_value.value, Some("test".to_string()));
    }

    #[test]
    fn test_build_query_parameter_int64() {
        let param = build_query_parameter("count", &json!(42)).unwrap();
        assert_eq!(param.name, Some("count".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_INT64);
        assert_eq!(param.parameter_value.value, Some("42".to_string()));
    }

    #[test]
    fn test_build_query_parameter_float64() {
        let param = build_query_parameter("price", &json!(99.99)).unwrap();
        assert_eq!(param.name, Some("price".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_FLOAT64);
        assert_eq!(param.parameter_value.value, Some("99.99".to_string()));
    }

    #[test]
    fn test_build_query_parameter_bool() {
        let param = build_query_parameter("active", &json!(true)).unwrap();
        assert_eq!(param.name, Some("active".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_BOOL);
        assert_eq!(param.parameter_value.value, Some("true".to_string()));
    }

    #[test]
    fn test_build_query_parameter_null() {
        let param = build_query_parameter("optional", &json!(null)).unwrap();
        assert_eq!(param.name, Some("optional".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_STRING);
        assert_eq!(param.parameter_value.value, None);
    }

    #[test]
    fn test_build_query_parameter_array() {
        use super::super::config::PARAM_TYPE_JSON;
        let param = build_query_parameter("items", &json!([1, 2, 3])).unwrap();
        assert_eq!(param.name, Some("items".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_JSON);
        assert_eq!(param.parameter_value.value, Some("[1,2,3]".to_string()));
    }

    #[test]
    fn test_build_query_parameter_object() {
        use super::super::config::PARAM_TYPE_JSON;
        let param = build_query_parameter("data", &json!({"key": "value"})).unwrap();
        assert_eq!(param.name, Some("data".to_string()));
        assert_eq!(param.parameter_type.parameter_type, PARAM_TYPE_JSON);
        assert_eq!(
            param.parameter_value.value,
            Some("{\"key\":\"value\"}".to_string())
        );
    }

    #[test]
    fn test_field_to_arrow_string() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "test".to_string(),
            data_type: TableFieldType::String,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "test");
        assert_eq!(arrow_field.data_type(), &DataType::Utf8);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_int64() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "count".to_string(),
            data_type: TableFieldType::Int64,
            mode: Some(TableFieldMode::Required),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "count");
        assert_eq!(arrow_field.data_type(), &DataType::Int64);
        assert!(!arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_timestamp() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "created_at".to_string(),
            data_type: TableFieldType::Timestamp,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "created_at");
        assert_eq!(
            arrow_field.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_date() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "birth_date".to_string(),
            data_type: TableFieldType::Date,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "birth_date");
        assert_eq!(arrow_field.data_type(), &DataType::Date32);
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_field_to_arrow_numeric() {
        use google_cloud_bigquery::http::table::{TableFieldMode, TableFieldType};
        let field = TableFieldSchema {
            name: "amount".to_string(),
            data_type: TableFieldType::Numeric,
            mode: Some(TableFieldMode::Nullable),
            ..Default::default()
        };
        let arrow_field = field_to_arrow(&field);
        assert_eq!(arrow_field.name(), "amount");
        assert_eq!(arrow_field.data_type(), &DataType::Decimal128(38, 9));
        assert!(arrow_field.is_nullable());
    }

    #[test]
    fn test_build_column_string() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("test1".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("test2".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "name".to_string(),
            data_type: TableFieldType::String,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let string_array = result.as_any().downcast_ref::<StringArray>().unwrap();

        assert_eq!(string_array.len(), 3);
        assert_eq!(string_array.value(0), "test1");
        assert_eq!(string_array.value(1), "test2");
        assert!(string_array.is_null(2));
    }

    #[test]
    fn test_build_column_int64() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("42".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("100".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "count".to_string(),
            data_type: TableFieldType::Int64,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let int_array = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(int_array.len(), 4);
        assert_eq!(int_array.value(0), 42);
        assert_eq!(int_array.value(1), 100);
        assert!(int_array.is_null(2));
        assert!(int_array.is_null(3));
    }

    #[test]
    fn test_build_column_float64() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("99.99".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("123.45".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "price".to_string(),
            data_type: TableFieldType::Float64,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let float_array = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(float_array.len(), 3);
        assert!((float_array.value(0) - 99.99).abs() < f64::EPSILON);
        assert!((float_array.value(1) - 123.45).abs() < f64::EPSILON);
        assert!(float_array.is_null(2));
    }

    #[test]
    fn test_build_column_bool() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("true".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("false".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "active".to_string(),
            data_type: TableFieldType::Bool,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let bool_array = result.as_any().downcast_ref::<BooleanArray>().unwrap();

        assert_eq!(bool_array.len(), 3);
        assert!(bool_array.value(0));
        assert!(!bool_array.value(1));
        assert!(bool_array.is_null(2));
    }

    #[test]
    fn test_build_column_date() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("2024-01-15".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("1970-01-01".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "date".to_string(),
            data_type: TableFieldType::Date,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let date_array = result.as_any().downcast_ref::<Date32Array>().unwrap();

        assert_eq!(date_array.len(), 4);
        assert_eq!(date_array.value(0), 19737);
        assert_eq!(date_array.value(1), 0);
        assert!(date_array.is_null(2));
        assert!(date_array.is_null(3));
    }

    #[test]
    fn test_build_column_time() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("12:30:45".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("00:00:00".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
        ];

        let field = TableFieldSchema {
            name: "time".to_string(),
            data_type: TableFieldType::Time,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let time_array = result
            .as_any()
            .downcast_ref::<Time64MicrosecondArray>()
            .unwrap();

        assert_eq!(time_array.len(), 3);
        assert_eq!(time_array.value(0), 45_045_000_000);
        assert_eq!(time_array.value(1), 0);
        assert!(time_array.is_null(2));
    }

    #[test]
    fn test_build_column_bytes() {
        use google_cloud_bigquery::http::table::TableFieldType;
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let rows = vec![
            Tuple {
                f: vec![Cell {
                    v: Value::String("aGVsbG8=".to_string()),
                }],
            },
            Tuple {
                f: vec![Cell { v: Value::Null }],
            },
            Tuple {
                f: vec![Cell {
                    v: Value::String("invalid base64!".to_string()),
                }],
            },
        ];

        let field = TableFieldSchema {
            name: "data".to_string(),
            data_type: TableFieldType::Bytes,
            ..Default::default()
        };

        let result = build_column(&rows, 0, &field).unwrap();
        let binary_array = result.as_any().downcast_ref::<BinaryArray>().unwrap();

        assert_eq!(binary_array.len(), 3);
        assert_eq!(binary_array.value(0), b"hello");
        assert!(binary_array.is_null(1));
        assert!(binary_array.is_null(2));
    }

    #[test]
    fn test_response_to_record_batch_empty() {
        use google_cloud_bigquery::http::table::{TableFieldType, TableSchema};

        let schema = Some(TableSchema {
            fields: vec![TableFieldSchema {
                name: "test".to_string(),
                data_type: TableFieldType::String,
                ..Default::default()
            }],
        });

        let result = response_to_record_batch(schema, vec![]).unwrap();
        assert_eq!(result.num_rows(), 0);
    }

    #[test]
    fn test_response_to_record_batch_with_data() {
        use google_cloud_bigquery::http::table::{TableFieldType, TableSchema};
        use google_cloud_bigquery::http::tabledata::list::{Cell, Tuple, Value};

        let schema = Some(TableSchema {
            fields: vec![
                TableFieldSchema {
                    name: "name".to_string(),
                    data_type: TableFieldType::String,
                    ..Default::default()
                },
                TableFieldSchema {
                    name: "count".to_string(),
                    data_type: TableFieldType::Int64,
                    ..Default::default()
                },
            ],
        });

        let rows = vec![
            Tuple {
                f: vec![
                    Cell {
                        v: Value::String("Alice".to_string()),
                    },
                    Cell {
                        v: Value::String("10".to_string()),
                    },
                ],
            },
            Tuple {
                f: vec![
                    Cell {
                        v: Value::String("Bob".to_string()),
                    },
                    Cell {
                        v: Value::String("20".to_string()),
                    },
                ],
            },
        ];

        let result = response_to_record_batch(schema, rows).unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.num_columns(), 2);

        let name_col = result
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(name_col.value(0), "Alice");
        assert_eq!(name_col.value(1), "Bob");

        let count_col = result
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(count_col.value(0), 10);
        assert_eq!(count_col.value(1), 20);
    }

    #[test]
    fn test_response_to_record_batch_no_schema_returns_empty_batch() {
        let result = response_to_record_batch(None, vec![]).unwrap();
        assert_eq!(result.num_rows(), 0);
        assert_eq!(result.num_columns(), 0);
    }

    fn http_error_response(code: u16) -> google_cloud_bigquery::http::error::Error {
        google_cloud_bigquery::http::error::Error::Response(
            google_cloud_bigquery::http::error::ErrorResponse {
                code,
                errors: None,
                message: "test".to_string(),
            },
        )
    }

    #[test]
    fn test_client_error_is_permanent() {
        let e = Error::QueryExecution {
            source: http_error_response(400),
        };
        assert!(e.is_permanent(), "bad SQL / bad request must not retry");

        let e = Error::QueryExecution {
            source: http_error_response(404),
        };
        assert!(e.is_permanent(), "missing table must not retry");

        let e = Error::QueryExecution {
            source: http_error_response(403),
        };
        assert!(e.is_permanent(), "permission denied must not retry");
    }

    #[test]
    fn test_rate_limit_is_not_permanent() {
        let e = Error::QueryExecution {
            source: http_error_response(429),
        };
        assert!(!e.is_permanent(), "429 is rate limiting, must retry");
    }

    #[test]
    fn test_server_error_is_not_permanent() {
        let e = Error::QueryExecution {
            source: http_error_response(500),
        };
        assert!(!e.is_permanent(), "5xx must still retry");
    }

    #[test]
    fn test_storage_query_client_error_is_permanent() {
        let e = Error::StorageQueryExecution {
            source: google_cloud_bigquery::client::QueryError::JobHttp(http_error_response(400)),
        };
        assert!(e.is_permanent());
    }
}
