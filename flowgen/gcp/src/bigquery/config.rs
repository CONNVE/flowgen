//! Configuration structures for BigQuery operations.
//!
//! This module provides configuration for BigQuery query execution with support for
//! parameterized queries to prevent SQL injection attacks. All query parameters are
//! strongly typed and validated before execution.

use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;

/// BigQuery parameter type constants.
pub const PARAM_TYPE_STRING: &str = "STRING";
pub const PARAM_TYPE_INT64: &str = "INT64";
pub const PARAM_TYPE_FLOAT64: &str = "FLOAT64";
pub const PARAM_TYPE_NUMERIC: &str = "NUMERIC";
pub const PARAM_TYPE_BIGNUMERIC: &str = "BIGNUMERIC";
pub const PARAM_TYPE_BOOL: &str = "BOOL";
pub const PARAM_TYPE_TIMESTAMP: &str = "TIMESTAMP";
pub const PARAM_TYPE_DATETIME: &str = "DATETIME";
pub const PARAM_TYPE_DATE: &str = "DATE";
pub const PARAM_TYPE_TIME: &str = "TIME";
pub const PARAM_TYPE_BYTES: &str = "BYTES";
pub const PARAM_TYPE_GEOGRAPHY: &str = "GEOGRAPHY";
pub const PARAM_TYPE_JSON: &str = "JSON";

/// Default timeout for BigQuery queries (10 seconds per BigQuery API defaults).
fn default_timeout() -> Option<Duration> {
    Some(Duration::from_secs(10))
}

/// Default value for use_query_cache.
fn default_use_query_cache() -> bool {
    true
}

/// Default value for use_legacy_sql.
fn default_use_legacy_sql() -> bool {
    false
}

/// The file-reference form of a query parameter.
///
/// `deny_unknown_fields` holds this to a lone `resource` key. Without it the
/// untagged enum below also matches an inline object that merely carries a
/// `resource` field, silently dropping that object's other fields and binding
/// a file's contents where the flow asked for its own data.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct QueryParameterResource {
    /// Resource path, loaded and parsed as JSON.
    pub resource: String,
}

/// A query parameter value: either given inline, or loaded from a resource
/// file. `Resource` is tried first so `{ resource: "..." }` is read as a file
/// reference rather than as a one-key JSON object.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum QueryParameterSource {
    /// Loads the file and parses it as JSON. A non-JSON file is passed
    /// through as a string.
    Resource(QueryParameterResource),
    /// Used as-is.
    Literal(Value),
}

/// Configuration structure for BigQuery query operations.
///
/// This structure defines all parameters needed to execute queries against BigQuery,
/// including authentication credentials, project configuration, and SQL query with
/// optional parameterization for safe query execution.
///
/// # SQL Injection Protection
///
/// To prevent SQL injection attacks, use parameterized queries with the `parameters`
/// field instead of string interpolation. Parameters are type-safe and properly
/// escaped by the BigQuery client library.
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located (data project).
/// - `job_project_id`: Optional GCP project ID for running the query job (billing project). If not specified, uses `project_id`.
/// - `query`: SQL query source (inline string or resource file reference).
/// - `parameters`: Optional query parameters for safe SQL injection prevention.
/// - `location`: Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
/// - `max_results`: Optional maximum number of rows to return per page (default: all rows).
/// - `timeout`: Optional query timeout (e.g., "30s", "5m", "1h"). Default: 10 seconds.
/// - `use_query_cache`: Whether to use query cache (default: true).
/// - `use_legacy_sql`: Whether to use legacy SQL syntax (default: false for Standard SQL).
/// - `create_session`: Whether to create a new session for this query.
/// - `labels`: Optional labels for the query job (key-value pairs).
/// - `default_dataset`: Optional default dataset for unqualified table names.
///
/// # Examples
///
/// Basic query with inline SQL:
/// ```yaml
/// bigquery_query:
///   name: fetch_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   query:
///     inline: "SELECT order_id, customer_id, amount FROM `project.dataset.orders` WHERE order_date >= '2024-01-01' LIMIT 100"
/// ```
///
/// Query using external resource file:
/// ```yaml
/// bigquery_query:
///   name: fetch_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   query:
///     resource: "queries/get_recent_orders.sql"
/// ```
///
/// Parameterized query for SQL injection protection:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "fetch_orders_by_status",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "query": "SELECT order_id, customer_id, amount FROM `project.dataset.orders` WHERE status = @status AND order_date >= @start_date",
///         "parameters": {
///             "status": "completed",
///             "start_date": "2024-01-01"
///         }
///     }
/// }
/// ```
///
/// Query with location and result limits:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "analyze_eu_customers",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "location": "EU",
///         "query": "SELECT customer_id, COUNT(*) as order_count FROM `project.dataset.orders` GROUP BY customer_id ORDER BY order_count DESC",
///         "max_results": 1000,
///         "timeout": "5m"
///     }
/// }
/// ```
///
/// Query with dynamic parameters from upstream events:
/// ```json
/// {
///     "bigquery_query": {
///         "name": "fetch_customer_orders",
///         "credentials_path": "/etc/gcp/credentials.json",
///         "project_id": "my-project-id",
///         "query": "SELECT * FROM `project.dataset.orders` WHERE customer_id = @customer_id",
///         "parameters": {
///             "customer_id": "{{event.customer_id}}"
///         }
///     }
/// }
/// ```
///
/// Cross-project query (data in one project, billing in another):
/// ```yaml
/// gcp_bigquery_query:
///   name: cross_project_query
///   credentials_path: /etc/gcp/credentials.json
///   project_id: data-project-id
///   job_project_id: billing-project-id
///   query:
///     inline: "SELECT * FROM `data-project-id.dataset.table` LIMIT 100"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Query {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located (data project).
    pub project_id: String,
    /// Optional GCP project ID for running the query job (billing project).
    /// If not specified, uses `project_id`.
    #[serde(default)]
    pub job_project_id: Option<String>,
    /// SQL query source (inline or resource).
    pub query: flowgen_core::resource::Source,
    /// Optional query parameters for SQL injection protection.
    /// Keys are parameter names (without @ prefix), values are parameter values.
    ///
    /// A value may also be `{ resource: "path/to/file.json" }`, which loads
    /// the file and passes its parsed contents as the parameter. Use it to
    /// drive a query from a schema definition the flow already keeps on disk,
    /// rather than restating that list inside the SQL:
    ///
    /// ```yaml
    /// parameters:
    ///   table: "Account"
    ///   schema: { resource: schemas/account_schema.json }
    /// ```
    #[serde(default)]
    pub parameters: Option<HashMap<String, QueryParameterSource>>,
    /// Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
    pub location: Option<String>,
    /// Optional maximum number of rows to return per page.
    pub max_results: Option<u64>,
    /// Optional query timeout (e.g., "30s", "5m", "1h"). Default: 10 seconds.
    #[serde(default = "default_timeout", with = "humantime_serde")]
    pub timeout: Option<Duration>,
    /// Whether to use query cache. Default: true.
    #[serde(default = "default_use_query_cache")]
    pub use_query_cache: bool,
    /// Whether to use legacy SQL syntax. Default: false (uses Standard SQL).
    #[serde(default = "default_use_legacy_sql")]
    pub use_legacy_sql: bool,
    /// Whether to create a new session for this query.
    #[serde(default)]
    pub create_session: Option<bool>,
    /// Optional labels for the query job (key-value pairs for organization).
    #[serde(default)]
    pub labels: Option<HashMap<String, String>>,
    /// Optional default dataset for unqualified table names in the query.
    /// Format: "project_id.dataset_id" or "dataset_id" (uses query's project_id).
    #[serde(default)]
    pub default_dataset: Option<String>,
    /// Route the result through the BigQuery Storage Read API instead
    /// of paginated REST results. Emits Arrow `RecordBatch` directly
    /// per wire batch, without the per-row `Tuple` conversion the REST
    /// path pays. Recommended for large result sets (over one million
    /// rows or one hundred megabytes); adds temporary-table overhead
    /// for smaller queries. Not compatible with data-definition or
    /// data-manipulation statements such as `INSERT` or `CREATE TABLE`,
    /// because those produce no destination table and will error.
    /// Default: false.
    #[serde(default)]
    pub use_storage_read: bool,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Query {}

impl Query {
    /// Resolves the query string from inline or resource source.
    ///
    /// # Arguments
    /// * `resource_loader` - ResourceLoader instance for loading external files.
    ///
    /// # Returns
    /// The resolved SQL query string.
    pub async fn resolve_query(
        &self,
        resource_loader: &flowgen_core::resource::ResourceLoader,
    ) -> Result<String, ResolveError> {
        match &self.query {
            flowgen_core::resource::Source::Inline(sql) => Ok(sql.clone()),
            flowgen_core::resource::Source::Resource { resource } => resource_loader
                .load(resource)
                .await
                .map_err(|source| ResolveError::Resource { source }),
        }
    }

    /// Returns the project ID to use for running the query job.
    /// Uses `job_project_id` if specified, otherwise falls back to `project_id`.
    pub fn get_job_project_id(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }
}

/// Errors that can occur when resolving query configuration.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ResolveError {
    #[error("Error loading resource: {source}")]
    Resource {
        #[source]
        source: flowgen_core::resource::Error,
    },
}

/// Job operation type for unified job processor.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobOperation {
    /// Create a new job (load, query, extract, copy).
    Create,
    /// Get job status and poll until completion.
    Get,
    /// Cancel a running job.
    Cancel,
    /// Delete job metadata.
    Delete,
}

/// Unified configuration for BigQuery job operations.
///
/// This single config handles all job operations: create, get, cancel, and delete.
/// The operation type determines which fields are required and what action is performed.
///
/// # Examples
///
/// Create a load job:
/// ```yaml
/// bigquery_job:
///   name: import_events
///   operation: create
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   source_uris:
///     - "gs://my-bucket/data/*.parquet"
///   destination_table:
///     project_id: my-project-id
///     dataset_id: analytics
///     table_id: events
///   source_format: parquet
///   write_disposition: write_append
///   autodetect: true
/// ```
///
/// Poll job status:
/// ```yaml
/// bigquery_job:
///   name: check_load_status
///   operation: get
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
///   poll_interval: 5s
///   max_poll_duration: 30m
/// ```
///
/// Cancel a job:
/// ```yaml
/// bigquery_job:
///   name: stop_job
///   operation: cancel
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
/// ```
///
/// Delete job metadata:
/// ```yaml
/// bigquery_job:
///   name: cleanup_job
///   operation: delete
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project-id
///   job_id: "{{event.data.job_id}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Job {
    /// Unique task identifier.
    pub name: String,
    /// Job operation type (create, get, cancel, delete).
    pub operation: JobOperation,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// Optional GCP project ID for running the job (billing project).
    #[serde(default)]
    pub job_project_id: Option<String>,
    /// Optional BigQuery dataset location (e.g., "US", "EU", "us-central1").
    #[serde(default)]
    pub location: Option<String>,
    // Fields for create operation.
    /// Job type for create operation (load, query, extract, copy). Default: load.
    #[serde(default = "default_job_type")]
    pub job_type: String,
    /// GCS URIs of source files to load (create operation only).
    /// Supports both literal arrays and template expressions that resolve to arrays at runtime.
    #[serde(default)]
    pub source_uris: Option<serde_json::Value>,
    /// Destination BigQuery table (create operation only).
    #[serde(default)]
    pub destination_table: Option<TableReference>,
    /// Source file format (create operation only).
    #[serde(default)]
    pub source_format: Option<SourceFormat>,
    /// How to handle existing data (create operation only).
    #[serde(default)]
    pub write_disposition: Option<WriteDisposition>,
    /// Whether to create table if it doesn't exist (create operation only).
    #[serde(default)]
    pub create_disposition: Option<CreateDisposition>,
    /// Whether to automatically infer schema (create operation only).
    #[serde(default)]
    pub autodetect: Option<bool>,
    /// Explicit table schema (create operation only).
    /// When provided, overrides autodetect for precise schema control.
    #[serde(default)]
    pub schema: Option<Vec<TableField>>,
    /// Optional maximum number of bad records (create operation only).
    #[serde(default)]
    pub max_bad_records: Option<i32>,
    /// Optional labels for the job.
    #[serde(default)]
    pub labels: Option<HashMap<String, String>>,

    // Fields for get/cancel/delete operations.
    /// Job ID (get, cancel, delete operations).
    #[serde(default)]
    pub job_id: Option<String>,

    // Fields for get operation.
    /// Polling interval between status checks (get operation only).
    #[serde(default = "default_poll_interval", with = "humantime_serde")]
    pub poll_interval: Duration,
    /// Maximum duration to poll before giving up (get operation only).
    #[serde(default = "default_max_poll_duration", with = "humantime_serde")]
    pub max_poll_duration: Duration,

    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration.
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for Job {}

impl Job {
    /// Returns the project ID to use for running the job.
    pub fn get_job_project_id(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }
}

/// BigQuery table reference.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct TableReference {
    /// GCP project ID containing the table.
    pub project_id: String,
    /// BigQuery dataset ID.
    pub dataset_id: String,
    /// BigQuery table ID.
    pub table_id: String,
}

/// Source file formats for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum SourceFormat {
    Parquet,
    Csv,
    #[serde(rename = "newline_delimited_json")]
    #[default]
    NewlineDelimitedJson,
    Avro,
}

impl SourceFormat {
    pub fn as_str(&self) -> &str {
        match self {
            SourceFormat::Parquet => "PARQUET",
            SourceFormat::Csv => "CSV",
            SourceFormat::NewlineDelimitedJson => "NEWLINE_DELIMITED_JSON",
            SourceFormat::Avro => "AVRO",
        }
    }
}

/// BigQuery field type for schema definitions.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldType {
    String,
    Bytes,
    Integer,
    Int64,
    Float,
    Float64,
    Numeric,
    Bignumeric,
    Boolean,
    Bool,
    Timestamp,
    Date,
    Time,
    Datetime,
    Geography,
    Json,
    Record,
}

impl From<FieldType> for google_cloud_bigquery::http::table::TableFieldType {
    fn from(ft: FieldType) -> Self {
        match ft {
            FieldType::String => Self::String,
            FieldType::Bytes => Self::Bytes,
            FieldType::Integer | FieldType::Int64 => Self::Integer,
            FieldType::Float | FieldType::Float64 => Self::Float,
            FieldType::Numeric | FieldType::Bignumeric => Self::Numeric,
            FieldType::Boolean | FieldType::Bool => Self::Boolean,
            FieldType::Timestamp => Self::Timestamp,
            FieldType::Date => Self::Date,
            FieldType::Time => Self::Time,
            FieldType::Datetime => Self::Datetime,
            FieldType::Geography => Self::Json,
            FieldType::Json => Self::Json,
            FieldType::Record => Self::Record,
        }
    }
}

/// BigQuery field mode for schema definitions.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum FieldMode {
    #[default]
    Nullable,
    Required,
    Repeated,
}

impl From<FieldMode> for google_cloud_bigquery::http::table::TableFieldMode {
    fn from(fm: FieldMode) -> Self {
        match fm {
            FieldMode::Nullable => Self::Nullable,
            FieldMode::Required => Self::Required,
            FieldMode::Repeated => Self::Repeated,
        }
    }
}

/// Table field schema definition.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct TableField {
    /// Field name.
    pub name: String,
    /// Field data type.
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Field mode (nullable, required, repeated).
    #[serde(default)]
    pub mode: FieldMode,
    /// Optional field description.
    #[serde(default)]
    pub description: Option<String>,
}

impl From<TableField> for google_cloud_bigquery::http::table::TableFieldSchema {
    fn from(tf: TableField) -> Self {
        Self {
            name: tf.name,
            data_type: tf.field_type.into(),
            mode: Some(tf.mode.into()),
            description: tf.description,
            fields: None,
            policy_tags: None,
            max_length: None,
            precision: None,
            scale: None,
            collation: None,
            default_value_expression: None,
            rounding_mode: None,
        }
    }
}

/// Write disposition for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum WriteDisposition {
    /// Append data to existing table. Create table if it doesn't exist.
    #[serde(rename = "write_append")]
    #[default]
    WriteAppend,
    /// Truncate table before loading data. Create table if it doesn't exist.
    #[serde(rename = "write_truncate")]
    WriteTruncate,
    /// Only load if table is empty. Fail if table exists and has data.
    #[serde(rename = "write_empty")]
    WriteEmpty,
}

impl WriteDisposition {
    pub fn as_str(&self) -> &str {
        match self {
            WriteDisposition::WriteAppend => "WRITE_APPEND",
            WriteDisposition::WriteTruncate => "WRITE_TRUNCATE",
            WriteDisposition::WriteEmpty => "WRITE_EMPTY",
        }
    }
}

/// Create disposition for BigQuery load jobs.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CreateDisposition {
    /// Create table if it doesn't exist (default).
    #[serde(rename = "create_if_needed")]
    #[default]
    CreateIfNeeded,
    /// Never create table. Fail if table doesn't exist.
    #[serde(rename = "create_never")]
    CreateNever,
}

impl CreateDisposition {
    pub fn as_str(&self) -> &str {
        match self {
            CreateDisposition::CreateIfNeeded => "CREATE_IF_NEEDED",
            CreateDisposition::CreateNever => "CREATE_NEVER",
        }
    }
}

/// Default poll interval for job status checks (5 seconds).
fn default_poll_interval() -> Duration {
    Duration::from_secs(5)
}

/// Default maximum poll duration (30 minutes).
fn default_max_poll_duration() -> Duration {
    Duration::from_secs(1800)
}

/// Default job type for create operation.
fn default_job_type() -> String {
    "load".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_query_config_deserialization_with_inline_query() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/test/creds.json",
            "project_id": "test-project",
            "query": "SELECT 1"
        }"#;
        let query: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            query.query,
            flowgen_core::resource::Source::Inline("SELECT 1".to_string())
        );
        assert_eq!(query.timeout, Some(Duration::from_secs(10)));
        assert!(query.use_query_cache);
        assert!(!query.use_legacy_sql);
    }

    #[test]
    fn test_query_config_deserialization_with_resource_query() {
        let json = r#"{
            "name": "test",
            "credentials_path": "/test/creds.json",
            "project_id": "test-project",
            "query": {
                "resource": "queries/get_data.sql"
            }
        }"#;
        let query: Query = serde_json::from_str(json).unwrap();
        assert_eq!(
            query.query,
            flowgen_core::resource::Source::Resource {
                resource: "queries/get_data.sql".to_string()
            }
        );
    }

    #[test]
    fn test_query_config_creation() {
        let mut parameters = HashMap::new();
        parameters.insert(
            "status".to_string(),
            QueryParameterSource::Literal(json!("completed")),
        );
        parameters.insert(
            "start_date".to_string(),
            QueryParameterSource::Literal(json!("2024-01-01")),
        );

        let query = Query {
            name: "test_query".to_string(),
            credentials_path: Some(PathBuf::from("/etc/gcp/credentials.json")),
            project_id: "my-project-id".to_string(),
            job_project_id: None,
            query: flowgen_core::resource::Source::Inline(
                "SELECT * FROM `dataset.table` WHERE status = @status AND date >= @start_date"
                    .to_string(),
            ),
            parameters: Some(parameters.clone()),
            location: Some("US".to_string()),
            max_results: Some(1000),
            timeout: Some(Duration::from_secs(300)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        assert_eq!(query.name, "test_query");
        assert_eq!(query.project_id, "my-project-id");
        assert_eq!(query.parameters, Some(parameters));
        assert_eq!(query.location, Some("US".to_string()));
        assert_eq!(query.max_results, Some(1000));
        assert_eq!(query.timeout, Some(Duration::from_secs(300)));
    }

    #[test]
    fn test_query_config_serialization() {
        let query = Query {
            name: "serialize_test".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.json")),
            project_id: "test-project".to_string(),
            job_project_id: None,
            query: flowgen_core::resource::Source::Inline("SELECT 1".to_string()),
            parameters: None,
            location: None,
            max_results: None,
            timeout: Some(Duration::from_secs(10)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        let json = serde_json::to_string(&query).unwrap();
        let deserialized: Query = serde_json::from_str(&json).unwrap();
        assert_eq!(query, deserialized);
    }

    /// The untagged enum decides between a file reference and an inline
    /// value purely on shape, so both spellings are pinned here: a bare
    /// scalar must stay a literal, and only `{ resource: ... }` may become a
    /// resource. An inline object that merely *contains* a `resource` key
    /// alongside others must stay a literal too.
    #[test]
    fn parameters_deserialize_resource_and_literal_forms() {
        let raw = r#"{
            "name": "params",
            "project_id": "proj",
            "query": "SELECT 1",
            "parameters": {
                "table": "Account",
                "count": 5,
                "schema": { "resource": "schemas/account_schema.json" },
                "inline_obj": { "resource": "a", "other": "b" }
            }
        }"#;
        let query: Query = serde_json::from_str(raw).expect("parses");
        let params = query.parameters.expect("parameters present");

        assert_eq!(
            params.get("table"),
            Some(&QueryParameterSource::Literal(json!("Account"))),
            "a bare string must not be read as a resource"
        );
        assert_eq!(
            params.get("count"),
            Some(&QueryParameterSource::Literal(json!(5)))
        );
        assert_eq!(
            params.get("schema"),
            Some(&QueryParameterSource::Resource(QueryParameterResource {
                resource: "schemas/account_schema.json".to_string()
            })),
            "a lone `resource` key is the file-reference form"
        );
        assert_eq!(
            params.get("inline_obj"),
            Some(&QueryParameterSource::Literal(
                json!({"resource": "a", "other": "b"})
            )),
            "an object with extra keys is data, not a file reference"
        );
    }

    #[test]
    fn test_query_config_with_parameters() {
        let mut parameters = HashMap::new();
        parameters.insert(
            "customer_id".to_string(),
            QueryParameterSource::Literal(json!("CUST-12345")),
        );
        parameters.insert(
            "min_amount".to_string(),
            QueryParameterSource::Literal(json!(100.50)),
        );
        parameters.insert(
            "active".to_string(),
            QueryParameterSource::Literal(json!(true)),
        );

        let query = Query {
            name: "parameterized_query".to_string(),
            credentials_path: Some(PathBuf::from("/etc/gcp/creds.json")),
            project_id: "analytics-prod".to_string(),
            job_project_id: None,
            query: flowgen_core::resource::Source::Inline("SELECT * FROM orders WHERE customer_id = @customer_id AND amount >= @min_amount AND active = @active".to_string()),
            parameters: Some(parameters),
            location: Some("EU".to_string()),
            max_results: Some(500),
            timeout: Some(Duration::from_secs(60)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        assert!(query.parameters.is_some());
        let params = query.parameters.unwrap();
        assert_eq!(
            params.get("customer_id"),
            Some(&QueryParameterSource::Literal(json!("CUST-12345")))
        );
        assert_eq!(
            params.get("min_amount"),
            Some(&QueryParameterSource::Literal(json!(100.50)))
        );
        assert_eq!(
            params.get("active"),
            Some(&QueryParameterSource::Literal(json!(true)))
        );
    }

    #[test]
    fn test_query_config_clone() {
        let query = Query {
            name: "clone_test".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "test-project".to_string(),
            job_project_id: None,
            query: flowgen_core::resource::Source::Inline(
                "SELECT COUNT(*) FROM dataset.table".to_string(),
            ),
            location: Some("us-central1".to_string()),
            max_results: Some(100),
            timeout: Some(Duration::from_secs(30)),
            parameters: None,
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        let cloned = query.clone();
        assert_eq!(query, cloned);
    }

    #[test]
    fn test_get_job_project_id_with_override() {
        let query = Query {
            name: "cross_project_query".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "data-project".to_string(),
            job_project_id: Some("billing-project".to_string()),
            query: flowgen_core::resource::Source::Inline("SELECT 1".to_string()),
            parameters: None,
            location: None,
            max_results: None,
            timeout: Some(Duration::from_secs(10)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        assert_eq!(query.get_job_project_id(), "billing-project");
    }

    #[test]
    fn test_get_job_project_id_default() {
        let query = Query {
            name: "single_project_query".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "my-project".to_string(),
            job_project_id: None,
            query: flowgen_core::resource::Source::Inline("SELECT 1".to_string()),
            parameters: None,
            location: None,
            max_results: None,
            timeout: Some(Duration::from_secs(10)),
            use_query_cache: true,
            use_legacy_sql: false,
            create_session: None,
            labels: None,
            default_dataset: None,
            use_storage_read: false,
            depends_on: None,
            retry: None,
        };

        assert_eq!(query.get_job_project_id(), "my-project");
    }

    #[test]
    fn test_storage_read_get_job_project_id_with_override() {
        let storage_read = StorageRead {
            name: "cross_project_read".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "data-project".to_string(),
            job_project_id: Some("billing-project".to_string()),
            dataset_id: "warehouse".to_string(),
            table_id: "large_table".to_string(),
            ..Default::default()
        };

        assert_eq!(storage_read.get_job_project_id(), "billing-project");
    }

    #[test]
    fn test_storage_read_get_job_project_id_default() {
        let storage_read = StorageRead {
            name: "single_project_read".to_string(),
            credentials_path: Some(PathBuf::from("/creds.json")),
            project_id: "my-project".to_string(),
            job_project_id: None,
            dataset_id: "analytics".to_string(),
            table_id: "events".to_string(),
            ..Default::default()
        };

        assert_eq!(storage_read.get_job_project_id(), "my-project");
    }
}

/// Data format for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DataFormat {
    /// Arrow format (default, preferred for columnar processing).
    #[default]
    Arrow,
    /// Avro format (row-based).
    Avro,
}

/// Response compression codec for BigQuery Storage Read API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum CompressionCodec {
    /// No compression (default).
    #[default]
    Unspecified,
    /// LZ4 compression.
    Lz4,
}

/// Configuration structure for BigQuery Storage Read API operations.
///
/// The Storage Read API provides high-throughput, parallel access to BigQuery tables
/// without running SQL queries. It is optimized for reading large tables with optional
/// column selection and row filtering.
///
/// # Key Benefits
/// - High-throughput parallel reads using multiple streams
/// - Column selection to reduce data transfer
/// - Server-side filtering with row_restriction
/// - Arrow format for efficient columnar processing
/// - Time-travel queries with snapshot_time
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located (data project).
/// - `job_project_id`: Optional GCP project ID for creating the read session (billing project). If not specified, uses `project_id`.
/// - `dataset_id`: BigQuery dataset ID containing the table.
/// - `table_id`: BigQuery table ID to read from.
/// - `selected_fields`: Optional list of column names to read (reads all if not specified).
/// - `row_restriction`: Optional SQL WHERE clause (without WHERE keyword) for server-side filtering.
/// - `sample_percentage`: Optional random sampling percentage (0.0 to 100.0).
/// - `compression_codec`: Optional response compression. Default: unspecified.
/// - `snapshot_time`: Optional timestamp for time-travel queries (RFC3339 format).
/// - `max_stream_count`: Optional maximum number of parallel read streams.
/// - `preferred_min_stream_count`: Optional minimum streams hint for optimization.
/// - `data_format`: Data format for results. Default: arrow.
/// - `retry`: Optional retry configuration (overrides app-level retry config).
///
/// # Examples
///
/// Basic table read with all columns:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
/// ```
///
/// Read with column selection and filtering:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: read_recent_orders
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: orders
///   selected_fields:
///     - order_id
///     - customer_id
///     - amount
///   row_restriction: "order_date >= '2024-01-01' AND status = 'completed'"
/// ```
///
/// Parallel read with compression:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: fast_table_scan
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: warehouse
///   table_id: large_table
///   max_stream_count: 10
///   compression_codec: lz4
/// ```
///
/// Time-travel query with sampling:
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: historical_sample
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: events
///   snapshot_time: "2024-01-15T00:00:00Z"
///   sample_percentage: 10.0
/// ```
///
/// Cross-project read (data in one project, billing in another):
/// ```yaml
/// gcp_bigquery_storage_read:
///   name: cross_project_read
///   credentials_path: /etc/gcp/credentials.json
///   project_id: data-project-id
///   job_project_id: billing-project-id
///   dataset_id: warehouse
///   table_id: large_table
/// ```
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct StorageRead {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located (data project).
    pub project_id: String,
    /// Optional GCP project ID for creating the read session (billing project).
    /// If not specified, uses `project_id`.
    #[serde(default)]
    pub job_project_id: Option<String>,
    /// BigQuery dataset ID containing the table.
    pub dataset_id: String,
    /// BigQuery table ID to read from.
    pub table_id: String,
    /// Optional list of column names to read. If not specified, reads all columns.
    /// Supports nested field selection using dot notation (e.g., "struct_field.nested_field").
    #[serde(default)]
    pub selected_fields: Option<Vec<String>>,
    /// Optional SQL WHERE clause for server-side row filtering (without WHERE keyword).
    /// Example: "order_date >= '2024-01-01' AND status = 'completed'".
    #[serde(default)]
    pub row_restriction: Option<String>,
    /// Optional random sampling percentage (0.0 to 100.0).
    /// Samples rows randomly from the table before applying filters.
    #[serde(default)]
    pub sample_percentage: Option<f64>,
    /// Optional response compression codec. Default: unspecified (no compression).
    #[serde(default)]
    pub compression_codec: CompressionCodec,
    /// Optional timestamp for time-travel queries (RFC3339 format).
    /// Example: "2024-01-15T00:00:00Z".
    #[serde(default)]
    pub snapshot_time: Option<String>,
    /// Optional maximum number of parallel read streams.
    /// Higher values can improve throughput for large tables.
    #[serde(default)]
    pub max_stream_count: Option<i32>,
    /// Optional preferred minimum number of streams (optimization hint).
    #[serde(default)]
    pub preferred_min_stream_count: Option<i32>,
    /// Data format for results. Default: arrow (preferred for columnar processing).
    #[serde(default)]
    pub data_format: DataFormat,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl ConfigExt for StorageRead {}

impl StorageRead {
    /// Returns the project ID to use for creating the read session.
    /// Uses `job_project_id` if specified, otherwise falls back to `project_id`.
    pub fn get_job_project_id(&self) -> &str {
        self.job_project_id.as_deref().unwrap_or(&self.project_id)
    }
}

/// Configuration structure for BigQuery Storage Write API operations.
///
/// The Storage Write API provides high-throughput, low-latency streaming writes to BigQuery
/// tables. It uses protocol buffers for efficient data transfer and supports both default
/// streams (immediate availability) and committed streams (exactly-once semantics).
///
/// # Key Benefits
/// - Low latency writes (sub-second vs minutes for LOAD jobs)
/// - High throughput streaming (100k+ rows/sec)
/// - Exactly-once semantics with committed streams
/// - No intermediate storage required
/// - Automatic schema evolution
///
/// # Stream Types
/// - **Default Stream**: Immediate data availability, suitable for real-time dashboards and webhooks
/// - **Committed Stream**: Exactly-once guarantees with deduplication (not yet implemented)
/// - **Pending Stream**: Batch commits for transactional writes (not yet implemented)
///
/// # Fields
/// - `name`: Unique name / identifier of the task.
/// - `credentials_path`: Path to GCP service account credentials JSON file.
/// - `project_id`: GCP project ID where BigQuery resources are located.
/// - `dataset_id`: BigQuery dataset ID containing the table.
/// - `table_id`: BigQuery table ID to write to.
/// - `stream_name`: Optional custom stream name (uses default stream if not specified).
/// - `trace_id`: Optional trace ID for debugging and monitoring.
/// - `retry`: Optional retry configuration (overrides app-level retry config).
///
/// # Examples
///
/// Basic write to table:
/// ```yaml
/// bigquery_storage_write:
///   name: write_events
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: analytics
///   table_id: events
/// ```
///
/// Write with retry configuration:
/// ```yaml
/// bigquery_storage_write:
///   name: write_with_retry
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: landing
///   table_id: webhook_events
///   retry:
///     max_attempts: 5
///     initial_interval: 2s
///     max_interval: 30s
/// ```
///
/// Write to custom stream:
/// ```yaml
/// bigquery_storage_write:
///   name: write_to_custom_stream
///   credentials_path: /etc/gcp/credentials.json
///   project_id: my-project
///   dataset_id: realtime
///   table_id: metrics
///   stream_name: "projects/my-project/datasets/realtime/tables/metrics/streams/stream1"
///   trace_id: "webhook-{{event.id}}"
/// ```
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct StorageWrite {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Path to GCP service account credentials JSON file.
    /// When omitted, falls back to Application Default Credentials (ADC).
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// GCP project ID where BigQuery resources are located.
    pub project_id: String,
    /// BigQuery dataset ID containing the table.
    pub dataset_id: String,
    /// BigQuery table ID to write to.
    pub table_id: String,
    /// Optional custom stream name. If not specified, uses the default stream.
    /// Format: "projects/{project}/datasets/{dataset}/tables/{table}/streams/{stream}".
    #[serde(default)]
    pub stream_name: Option<String>,
    /// Optional trace ID for debugging and request tracking.
    #[serde(default)]
    pub trace_id: Option<String>,
    /// Optional CDC change type for BigQuery Change Data Capture.
    /// Requires the target table to have a primary key defined.
    #[serde(default)]
    pub change_type: Option<ChangeType>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

/// BigQuery CDC change type for Storage Write API.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ChangeType {
    /// Insert or update row by primary key.
    Upsert,
    /// Delete row by primary key.
    Delete,
}

impl std::fmt::Display for ChangeType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChangeType::Upsert => write!(f, "UPSERT"),
            ChangeType::Delete => write!(f, "DELETE"),
        }
    }
}

impl ConfigExt for StorageWrite {}
