//! Configuration structures for flowgen application and flows.
//!
//! Provides configuration structures for the main application and individual
//! flows. Supports deserialization from TOML files and environment variables.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;
use std::time::Duration;

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    flowgen_nats::client::DEFAULT_NATS_URL.to_string()
}

/// Default cache database name.
pub const DEFAULT_CACHE_DB_NAME: &str = "flowgen_cache";

/// Default cache key prefix for flows. The bucket already namespaces by
/// `flowgen`, so the prefix does not repeat it.
pub const DEFAULT_FLOWS_CACHE_PREFIX: &str = "flows";

/// Default cache key prefix for resources. The bucket already namespaces by
/// `flowgen`, so the prefix does not repeat it.
pub const DEFAULT_RESOURCES_CACHE_PREFIX: &str = "resources";

/// Supported flow configuration file extensions for recursive discovery.
pub const FLOW_CONFIG_EXTENSIONS: &[&str] = &["yaml", "yml", "json"];

pub use flowgen_core::identity::FlowIdentity;

/// Deserialization surface for a flow YAML file.
///
/// Public API is [`FlowConfig`] — `FlowConfigRaw` exists only because
/// serde deserializes the flow body without an identity, which the loader
/// supplies from the path. The loader wraps this into a `FlowConfig` via
/// [`FlowConfig::from_path`].
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowConfigRaw {
    /// Flow definition (tasks and optional labels).
    pub flow: Flow,
}

/// Top-level configuration for an individual flow.
///
/// Construct via [`FlowConfig::from_path`]. Deserialize the YAML into
/// [`FlowConfigRaw`] first, then wrap it with the identity from the path.
#[derive(PartialEq, Clone, Debug, Serialize)]
pub struct FlowConfig {
    /// Flow definition (tasks and optional labels).
    pub flow: Flow,
    /// Verbatim YAML source the loader read this flow from. Kept for the
    /// admin API so the UI can render the file as authored — round-tripping
    /// through `serde_yaml` introduces enum tags and `null` sentinels for
    /// `Option::None` fields, which is not what an operator wants to see.
    #[serde(skip)]
    pub raw_source: Option<String>,
    /// Flow identity, established at construction. Never mutated after.
    #[serde(skip)]
    identity: FlowIdentity,
}

impl FlowConfig {
    /// Builds a `FlowConfig` from a path-shaped identity. `path` is the
    /// identity string (relative to `flows.path` with extension stripped for
    /// a filesystem load, or the KV key suffix for a cache load, or the
    /// caller-supplied path for a programmatic flow). Validates task names.
    pub fn from_path(
        raw: FlowConfigRaw,
        path: String,
        raw_source: Option<String>,
    ) -> Result<Self, flowgen_core::validate::Error> {
        let config = Self {
            flow: raw.flow,
            raw_source,
            identity: FlowIdentity::new(path),
        };
        config.validate_tasks()?;
        Ok(config)
    }

    /// Returns the flow identity — the path-shaped source of truth. Used as
    /// the registry key, the tracing `flow=` field, REST `path`, and resource
    /// URIs. For cache and lease keys, project via [`FlowIdentity::as_key`].
    pub fn identity(&self) -> &str {
        self.identity.as_str()
    }

    /// Returns the typed identity, exposing both the human string
    /// ([`FlowIdentity::as_str`]) and the key-safe id
    /// ([`FlowIdentity::as_key`]).
    pub fn identity_ref(&self) -> &FlowIdentity {
        &self.identity
    }

    /// Returns the human-facing display name: `flow.name` when set,
    /// otherwise the basename of the identity path. `name` no longer affects
    /// identity — it is a pure display override. Used for UI lists and log
    /// fields, never for keying.
    pub fn display_name(&self) -> &str {
        match self.flow.name.as_deref() {
            Some(name) => name,
            None => match self.identity.as_str().rsplit('/').next() {
                Some(basename) => basename,
                None => self.identity.as_str(),
            },
        }
    }

    fn validate_tasks(&self) -> Result<(), flowgen_core::validate::Error> {
        use flowgen_core::validate::{validate_name, NameField};
        for task in &self.flow.tasks {
            validate_name(NameField::Task, task.name())?;
        }
        Ok(())
    }
}

/// Default value for parallel_instances.
fn default_parallel_instances() -> usize {
    1
}

/// Flow definition with task list. Identity always comes from the flow's
/// path (see [`FlowIdentity`]); `name` is a pure display override and does
/// not affect identity.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Flow {
    /// Optional display-name override for the UI and log fields. Does not
    /// affect identity — see [`FlowConfig::display_name`]. Accepted for
    /// backward compatibility with configs that still declare it; a flow
    /// with no `name` derives its display name from the path basename.
    #[serde(default)]
    pub name: Option<String>,
    /// Optional label for logging.
    pub labels: Option<Map<String, Value>>,
    /// List of tasks to execute in this flow.
    pub tasks: Vec<TaskType>,
    /// Whether this flow requires leader election (defaults to false if not specified).
    pub require_leader_election: Option<bool>,
    /// Number of parallel flow instances to run within the same process.
    ///
    /// Spawns multiple independent flow pipelines for higher throughput.
    /// Best used with leader election enabled. Defaults to 1.
    #[serde(default = "default_parallel_instances")]
    pub parallel_instances: usize,
}

/// Available task types in the flowgen ecosystem.
///
/// Each variant corresponds to a specific processor type from the
/// various flowgen worker crates. Task configurations are embedded
/// within each variant.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
// YAML configs reference task types by their lowercase tag (e.g. `convert:`,
// `script:`). The serde-driven variant naming therefore has to use the same
// shape — Rust's CamelCase convention does not apply to this public API.
#[expect(non_camel_case_types, reason = "variant names are YAML tags")]
pub enum TaskType {
    /// Data conversion task.
    convert(flowgen_core::task::convert::config::Processor),
    /// Iterate over arrays task.
    iterate(flowgen_core::task::iterate::config::Processor),
    /// Log output task.
    log(flowgen_core::task::log::config::Processor),
    /// Script execution task.
    script(flowgen_core::task::script::config::Processor),
    /// Buffer task for accumulating events into batches.
    buffer(flowgen_core::task::buffer::config::Processor),
    /// Object store operations (read, write, list, move).
    object_store(flowgen_object_store::config::Processor),
    /// Data generation task.
    generate(flowgen_core::task::generate::config::Subscriber),
    /// HTTP request task.
    http_request(flowgen_http::config::Processor),
    /// HTTP endpoint task: exposes a route on the worker HTTP server.
    http_endpoint(flowgen_http::config::Processor),
    /// HTML scrape task for extracting structured data using CSS selectors.
    html_scrape(flowgen_html::scrape::config::Processor),
    /// NATS JetStream subscriber task.
    nats_jetstream_subscriber(flowgen_nats::jetstream::config::Subscriber),
    /// NATS JetStream publisher task.
    nats_jetstream_publisher(flowgen_nats::jetstream::config::Publisher),
    /// Salesforce Pub/Sub API subscriber task.
    salesforce_pubsubapi_subscriber(flowgen_salesforce::pubsubapi::config::Subscriber),
    /// Salesforce Pub/Sub API publisher task.
    salesforce_pubsubapi_publisher(flowgen_salesforce::pubsubapi::config::Publisher),
    /// Salesforce Bulk API query job operations (create, get, delete, abort, get_results).
    salesforce_bulkapi_query_job(flowgen_salesforce::bulkapi::config::QueryJob),
    /// Salesforce REST API SObject operations (create, get, update, upsert, delete).
    salesforce_restapi_sobject(flowgen_salesforce::restapi::config::SObject),
    /// Salesforce REST API Composite operations for bulk record operations.
    salesforce_restapi_composite(flowgen_salesforce::restapi::config::Composite),
    /// Salesforce REST API SOSL search operations.
    salesforce_restapi_search(flowgen_salesforce::restapi::config::Search),
    /// Salesforce SOAP API merge operation for combining duplicate records.
    salesforce_soapapi_merge(flowgen_salesforce::soapapi::config::Merge),
    /// Salesforce Tooling API operations for metadata management.
    salesforce_toolingapi(flowgen_salesforce::toolingapi::config::Tooling),
    /// GCP BigQuery query task.
    gcp_bigquery_query(flowgen_gcp::bigquery::config::Query),
    /// GCP BigQuery Storage Read API task.
    gcp_bigquery_storage_read(flowgen_gcp::bigquery::config::StorageRead),
    /// GCP BigQuery unified job operations (create, get, cancel, delete).
    gcp_bigquery_job(flowgen_gcp::bigquery::config::Job),
    /// GCP BigQuery Storage Write API task for high-throughput streaming inserts.
    gcp_bigquery_storage_write(flowgen_gcp::bigquery::config::StorageWrite),
    /// Microsoft SQL Server query task.
    mssql_query(flowgen_mssql::config::Query),
    /// NATS Key-Value store operations (get, put, list, delete).
    nats_kv_store(flowgen_nats::jetstream::kv_store::Config),
    /// AI completion task for generating responses using LLMs.
    ai_completion(flowgen_ai_agent::completion::config::Processor),
    /// MCP tool task for exposing flows as MCP tools callable by LLMs.
    mcp_tool(flowgen_mcp::config::Processor),
    /// MCP resource task for exposing read-only content to MCP clients.
    mcp_resource(flowgen_mcp::resource::config::Processor),
    /// MCP prompt task for exposing slash-command templates to MCP clients.
    mcp_prompt(flowgen_mcp::prompt::config::Processor),
    /// LLM proxy task — registers a flow as a backend on the AI gateway server.
    llm_proxy(flowgen_ai_agent::ai_gateway::config::Processor),
    /// Git sync task for syncing flows and resources from a Git repository to the cache.
    git_sync(flowgen_git::sync::config::Processor),
    /// OCI sync task for pulling flow artifacts from an OCI registry into the cache.
    oci_sync(flowgen_oci::sync::config::Processor),
    /// Braze export user IDs task.
    braze_export_users_ids(flowgen_braze::export::users::config::Processor),
    /// MongoDB collection read/write task.
    mongodb_collection(flowgen_mongodb::config::Collection),
    /// MongoDB change stream reader task.
    mongodb_change_stream(flowgen_mongodb::config::ChangeStream),
}

impl TaskType {
    /// Returns the task type as a static string for event categorization.
    pub const fn as_str(&self) -> &'static str {
        match self {
            TaskType::convert(_) => "convert",
            TaskType::iterate(_) => "iterate",
            TaskType::log(_) => "log",
            TaskType::script(_) => "script",
            TaskType::buffer(_) => "buffer",
            TaskType::object_store(_) => "object_store",
            TaskType::generate(_) => "generate",
            TaskType::http_request(_) => "http_request",
            TaskType::http_endpoint(_) => "http_endpoint",
            TaskType::html_scrape(_) => "html_scrape",
            TaskType::nats_jetstream_subscriber(_) => "nats_jetstream_subscriber",
            TaskType::nats_jetstream_publisher(_) => "nats_jetstream_publisher",
            TaskType::salesforce_pubsubapi_subscriber(_) => "salesforce_pubsubapi_subscriber",
            TaskType::salesforce_pubsubapi_publisher(_) => "salesforce_pubsubapi_publisher",
            TaskType::salesforce_bulkapi_query_job(_) => "salesforce_bulkapi_query_job",
            TaskType::salesforce_restapi_sobject(_) => "salesforce_restapi_sobject",
            TaskType::salesforce_restapi_composite(_) => "salesforce_restapi_composite",
            TaskType::salesforce_restapi_search(_) => "salesforce_restapi_search",
            TaskType::salesforce_soapapi_merge(_) => "salesforce_soapapi_merge",
            TaskType::salesforce_toolingapi(_) => "salesforce_toolingapi",
            TaskType::gcp_bigquery_query(_) => "gcp_bigquery_query",
            TaskType::gcp_bigquery_storage_read(_) => "gcp_bigquery_storage_read",
            TaskType::gcp_bigquery_job(_) => "gcp_bigquery_job",
            TaskType::gcp_bigquery_storage_write(_) => "gcp_bigquery_storage_write",
            TaskType::mssql_query(_) => "mssql_query",
            TaskType::nats_kv_store(_) => "nats_kv_store",
            TaskType::ai_completion(_) => "ai_completion",
            TaskType::mcp_tool(_) => "mcp_tool",
            TaskType::mcp_resource(_) => "mcp_resource",
            TaskType::mcp_prompt(_) => "mcp_prompt",
            TaskType::llm_proxy(_) => "llm_proxy",
            TaskType::git_sync(_) => "git_sync",
            TaskType::oci_sync(_) => "oci_sync",
            TaskType::braze_export_users_ids(_) => "braze_export_users_ids",
            TaskType::mongodb_collection(_) => "mongodb_collection",
            TaskType::mongodb_change_stream(_) => "mongodb_change_stream",
        }
    }

    /// Returns the task name from its configuration.
    pub fn name(&self) -> &str {
        match self {
            TaskType::convert(c) => &c.name,
            TaskType::iterate(c) => &c.name,
            TaskType::log(c) => &c.name,
            TaskType::script(c) => &c.name,
            TaskType::buffer(c) => &c.name,
            TaskType::object_store(c) => &c.name,
            TaskType::generate(c) => &c.name,
            TaskType::http_request(c) => &c.name,
            TaskType::http_endpoint(c) => &c.name,
            TaskType::html_scrape(c) => &c.name,
            TaskType::nats_jetstream_subscriber(c) => &c.name,
            TaskType::nats_jetstream_publisher(c) => &c.name,
            TaskType::salesforce_pubsubapi_subscriber(c) => &c.name,
            TaskType::salesforce_pubsubapi_publisher(c) => &c.name,
            TaskType::salesforce_bulkapi_query_job(c) => &c.name,
            TaskType::salesforce_restapi_sobject(c) => &c.name,
            TaskType::salesforce_restapi_composite(c) => &c.name,
            TaskType::salesforce_restapi_search(c) => &c.name,
            TaskType::salesforce_soapapi_merge(c) => &c.name,
            TaskType::salesforce_toolingapi(c) => &c.name,
            TaskType::gcp_bigquery_query(c) => &c.name,
            TaskType::gcp_bigquery_storage_read(c) => &c.name,
            TaskType::gcp_bigquery_job(c) => &c.name,
            TaskType::gcp_bigquery_storage_write(c) => &c.name,
            TaskType::mssql_query(c) => &c.name,
            TaskType::nats_kv_store(c) => &c.name,
            TaskType::ai_completion(c) => &c.name,
            TaskType::mcp_tool(c) => &c.name,
            TaskType::mcp_resource(c) => &c.name,
            TaskType::mcp_prompt(c) => &c.name,
            TaskType::llm_proxy(c) => &c.name,
            TaskType::git_sync(c) => &c.name,
            TaskType::oci_sync(c) => &c.name,
            TaskType::braze_export_users_ids(c) => &c.name,
            TaskType::mongodb_collection(c) => &c.name,
            TaskType::mongodb_change_stream(c) => &c.name,
        }
    }

    /// Whether this task participates in the event pipeline.
    ///
    /// Registration-only tasks (`mcp_prompt`, `mcp_resource`) publish their
    /// entry to the shared MCP server at init and exit — the client-facing
    /// `prompts/get` and `resources/read` calls are served by the MCP HTTP
    /// server, never through the flow pipeline. They therefore have no
    /// inbound channel, no outbound channel, and no implicit dependency on
    /// the preceding task in the flow list.
    pub const fn has_pipeline_io(&self) -> bool {
        !matches!(self, TaskType::mcp_prompt(_) | TaskType::mcp_resource(_))
    }

    /// Returns the `depends_on` list if configured on the task.
    pub fn depends_on(&self) -> Option<&Vec<String>> {
        match self {
            TaskType::convert(c) => c.depends_on.as_ref(),
            TaskType::iterate(c) => c.depends_on.as_ref(),
            TaskType::log(c) => c.depends_on.as_ref(),
            TaskType::script(c) => c.depends_on.as_ref(),
            TaskType::buffer(c) => c.depends_on.as_ref(),
            TaskType::object_store(c) => c.depends_on.as_ref(),
            TaskType::generate(c) => c.depends_on.as_ref(),
            TaskType::http_request(c) => c.depends_on.as_ref(),
            TaskType::http_endpoint(c) => c.depends_on.as_ref(),
            TaskType::html_scrape(c) => c.depends_on.as_ref(),
            TaskType::nats_jetstream_subscriber(c) => c.depends_on.as_ref(),
            TaskType::nats_jetstream_publisher(c) => c.depends_on.as_ref(),
            TaskType::salesforce_pubsubapi_subscriber(c) => c.depends_on.as_ref(),
            TaskType::salesforce_pubsubapi_publisher(c) => c.depends_on.as_ref(),
            TaskType::salesforce_bulkapi_query_job(c) => c.depends_on.as_ref(),
            TaskType::salesforce_restapi_sobject(c) => c.depends_on.as_ref(),
            TaskType::salesforce_restapi_composite(c) => c.depends_on.as_ref(),
            TaskType::salesforce_restapi_search(c) => c.depends_on.as_ref(),
            TaskType::salesforce_soapapi_merge(c) => c.depends_on.as_ref(),
            TaskType::salesforce_toolingapi(c) => c.depends_on.as_ref(),
            TaskType::gcp_bigquery_query(c) => c.depends_on.as_ref(),
            TaskType::gcp_bigquery_storage_read(c) => c.depends_on.as_ref(),
            TaskType::gcp_bigquery_job(c) => c.depends_on.as_ref(),
            TaskType::gcp_bigquery_storage_write(c) => c.depends_on.as_ref(),
            TaskType::mssql_query(c) => c.depends_on.as_ref(),
            TaskType::nats_kv_store(c) => c.depends_on.as_ref(),
            TaskType::ai_completion(c) => c.depends_on.as_ref(),
            TaskType::mcp_tool(c) => c.depends_on.as_ref(),
            TaskType::mcp_resource(c) => c.depends_on.as_ref(),
            TaskType::mcp_prompt(c) => c.depends_on.as_ref(),
            TaskType::llm_proxy(c) => c.depends_on.as_ref(),
            TaskType::git_sync(c) => c.depends_on.as_ref(),
            TaskType::oci_sync(c) => c.depends_on.as_ref(),
            TaskType::braze_export_users_ids(c) => c.depends_on.as_ref(),
            TaskType::mongodb_collection(c) => c.depends_on.as_ref(),
            TaskType::mongodb_change_stream(c) => c.depends_on.as_ref(),
        }
    }
}

impl std::fmt::Display for TaskType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Main application configuration.
///
/// Flowgen ships as a single binary — there is no separate worker/server
/// split — so component sections live at the top level.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AppConfig {
    /// Optional cache configuration (shared across components).
    pub cache: Option<CacheOptions>,
    /// Flow discovery options (shared across components).
    pub flows: FlowOptions,
    /// Optional resource loading configuration (shared across components).
    pub resources: Option<ResourceOptions>,
    /// Optional HTTP server configuration for webhooks, health checks, and metrics.
    pub http_server: Option<HttpServerOptions>,
    /// Optional MCP server configuration for exposing flows as MCP tools.
    pub mcp_server: Option<McpServerOptions>,
    /// Optional AI gateway configuration for OpenAI-compatible chat completion endpoints.
    /// Runs on its own port so the surface can later migrate to gRPC or WebSocket
    /// independently of the webhook HTTP server.
    pub ai_gateway: Option<AiGatewayOptions>,
    /// Optional admin web UI configuration for inspecting loaded flows.
    pub web: Option<WebOptions>,
    /// Kubernetes-facing liveness/readiness listener. Independent of the
    /// API listeners so probes keep working regardless of which surfaces
    /// (http_server / mcp_server / ai_gateway / web) are enabled.
    /// Defaults to enabled on port 8081 if omitted.
    #[serde(default)]
    pub health: HealthOptions,
    /// Optional app-level retry configuration (can be overridden per task).
    pub retry: Option<flowgen_core::retry::RetryConfig>,
    /// Per-edge event channel capacity in events (defaults to 10,000).
    /// When full the upstream task blocks until downstream drains a slot.
    pub event_buffer_size: Option<usize>,
    /// Optional OpenTelemetry configuration for metrics and tracing.
    pub telemetry: Option<TelemetryOptions>,
}

/// Rejected configuration, reported before anything starts.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ValidationError {
    #[error(
        "`cache.db_name` was renamed to `cache.runtime.db_name` in 0.135. Rename it to keep \
         using bucket {db_name:?} — leaving it in place would silently fall back to the \
         default bucket and strand the existing data:\n\
         \x20 cache:\n\
         \x20   runtime:\n\
         \x20     db_name: {db_name:?}"
    )]
    RenamedCacheDbName { db_name: String },
}

impl AppConfig {
    /// Rejects configuration that would otherwise be accepted with the wrong
    /// meaning. Serde ignores unknown keys, so a renamed field needs an
    /// explicit check to avoid changing behaviour on upgrade without a word.
    pub fn validate(&self) -> Result<(), ValidationError> {
        match self.cache.as_ref().and_then(|c| c.db_name.as_ref()) {
            Some(db_name) => Err(ValidationError::RenamedCacheDbName {
                db_name: db_name.clone(),
            }),
            None => Ok(()),
        }
    }
}

/// Cache type for storage backend.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CacheType {
    /// NATS JetStream Key-Value store.
    Nats,
}

/// Cache configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct CacheOptions {
    /// Whether caching is enabled.
    pub enabled: bool,
    /// Cache backend type.
    #[serde(rename = "type")]
    pub cache_type: CacheType,
    /// Optional path to cache credentials file.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// NATS server URL (e.g., "nats://localhost:4222"). Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// Pre-0.135 spelling of `runtime.db_name`. Kept solely to reject it:
    /// serde ignores unknown keys, so without this an upgraded config would
    /// silently fall back to the default bucket and leave the operator's data
    /// in the old one.
    #[serde(default)]
    pub db_name: Option<String>,
    /// Runtime bucket, reachable from scripts through `ctx.cache`.
    #[serde(default)]
    pub runtime: BucketOptions,
    /// System bucket holding leader-election leases and peer registration.
    /// Kept apart from `runtime` so scripts cannot reach coordination state.
    #[serde(default)]
    pub system: SystemBucketOptions,
    /// Number of historical entries retained per key in the KV bucket.
    /// Only applies when the bucket is created; changing this on an existing
    /// bucket has no effect. Defaults to 10.
    pub history: Option<i64>,
    /// TTL for KV delete and purge tombstone markers.
    /// Accepts human-readable durations (e.g., "1h", "30m", "1d").
    /// Must be set to some duration for per-key TTLs on cache entries to work —
    /// this flag enables per-message TTL on the underlying JetStream stream.
    /// Defaults to 1 hour.
    #[serde(default, with = "humantime_serde")]
    pub tombstone_ttl: Option<std::time::Duration>,
}

/// Runtime bucket options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct BucketOptions {
    /// Bucket name (defaults to "flowgen_cache").
    #[serde(default = "default_runtime_db_name")]
    pub db_name: String,
}

impl Default for BucketOptions {
    fn default() -> Self {
        Self {
            db_name: default_runtime_db_name(),
        }
    }
}

/// System bucket options.
///
/// Both buckets sit on the connection `CacheOptions` describes, so this
/// inherits `url`, `credentials_path`, and `type`. With the cache disabled
/// the bucket falls back to an in-memory store, which cannot coordinate
/// across pods — see the warning `App` emits for leader-elected flows.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct SystemBucketOptions {
    /// Bucket name (defaults to "flowgen_system").
    #[serde(default = "default_system_db_name")]
    pub db_name: String,
}

impl Default for SystemBucketOptions {
    fn default() -> Self {
        Self {
            db_name: default_system_db_name(),
        }
    }
}

/// Default bucket name for the runtime cache.
fn default_runtime_db_name() -> String {
    DEFAULT_CACHE_DB_NAME.to_string()
}

/// Flow loading configuration.
///
/// Filesystem and cache sources are independent: the worker loads from each
/// configured source and merges the results. Configure both for hybrid mode
/// (a small set of bootstrap flows on disk plus user flows in the cache).
/// On a name collision the filesystem entry wins so a locally mounted flow
/// cannot be silently overridden by a stale cache entry.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowOptions {
    /// Path for discovering flow configuration files.
    /// Can be either:
    /// - A base directory path (e.g., "/flows") - will recursively discover all .yaml, .yml, and .json files
    /// - A glob pattern (e.g., "/flows/*.yaml") - for backward compatibility, uses pattern directly
    pub path: Option<PathBuf>,
    /// Optional cache-based flow loading configuration.
    /// When enabled, flows are loaded from the distributed cache instead of the filesystem.
    #[serde(default)]
    pub cache: Option<FlowCacheOptions>,
}

/// Default cache key prefix for flows.
fn default_flows_cache_prefix() -> String {
    DEFAULT_FLOWS_CACHE_PREFIX.to_string()
}

/// Default metadata bucket name for flow/resource loading.
fn default_system_db_name() -> String {
    "flowgen_system".to_string()
}

/// Cache-based flow loading options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct FlowCacheOptions {
    /// Whether to load flows from the cache instead of the filesystem.
    pub enabled: bool,
    /// Optional cache key prefix (defaults to "flows").
    #[serde(default = "default_flows_cache_prefix")]
    pub prefix: String,
    /// Metadata store bucket name (defaults to "flowgen_system").
    /// Separate from the runtime cache bucket to avoid key collisions.
    #[serde(default = "default_system_db_name")]
    pub db_name: String,
}

/// Resource loading configuration.
///
/// Filesystem and cache sources are independent: configure either, both, or
/// neither. When both are configured the loader tries the filesystem first
/// and falls back to the cache on a miss, so a locally mounted resource
/// always takes precedence over a cached copy with the same key.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize, Default)]
pub struct ResourceOptions {
    /// Optional base path for loading external resource files (SQL, JSON, HTML, etc.).
    /// Resource keys are resolved relative to this path.
    /// Example: with path="local/resources/", key="queries/orders.sql" resolves to "local/resources/queries/orders.sql".
    /// Omit to skip filesystem loading.
    #[serde(default)]
    pub path: Option<PathBuf>,
    /// Optional cache-based resource loading configuration.
    /// When enabled, resources are loaded from the distributed cache.
    /// Combine with `path` for hybrid loading (filesystem first, cache fallback).
    #[serde(default)]
    pub cache: Option<ResourceCacheOptions>,
}

/// Default cache key prefix for resources.
fn default_resources_cache_prefix() -> String {
    DEFAULT_RESOURCES_CACHE_PREFIX.to_string()
}

/// Cache-based resource loading options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct ResourceCacheOptions {
    /// Whether to load resources from the cache instead of the filesystem.
    pub enabled: bool,
    /// Optional cache key prefix (defaults to "resources").
    #[serde(default = "default_resources_cache_prefix")]
    pub prefix: String,
    /// Metadata store bucket name (defaults to "flowgen_system").
    /// Separate from the runtime cache bucket to avoid key collisions.
    #[serde(default = "default_system_db_name")]
    pub db_name: String,
}

/// Default MCP server port.
fn default_mcp_port() -> u16 {
    3001
}

/// Default MCP endpoint path.
fn default_mcp_path() -> String {
    "/mcp/v1".to_string()
}

/// MCP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct McpServerOptions {
    /// Whether MCP server is enabled. Must be explicitly enabled by the admin
    /// for `mcp_tool` tasks to be registered.
    pub enabled: bool,
    /// MCP server port number (defaults to 3001).
    #[serde(default = "default_mcp_port")]
    pub port: u16,
    /// MCP endpoint path (defaults to "/mcp").
    #[serde(default = "default_mcp_path")]
    pub path: String,
    /// Optional path to global credentials file for API key authentication.
    /// Individual `mcp_tool` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all `mcp_tool` flows on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
    /// URI scheme used when auto-generating `mcp_resource` URIs of the form
    /// `<scheme>://<flow_name>/<name>`. White-label deployments override
    /// this (e.g. `acme`) so URIs emitted to LLM clients carry the
    /// deployment's brand instead of `flowgen`.
    #[serde(default = "default_resource_uri_scheme")]
    pub resource_uri_scheme: String,
}

/// Default scheme for auto-generated MCP resource URIs.
fn default_resource_uri_scheme() -> String {
    "flowgen".to_string()
}

/// Default AI gateway port.
fn default_ai_gateway_port() -> u16 {
    3002
}

/// Default AI gateway path prefix.
///
/// `/v1` matches the de-facto OpenAI-compatible convention used by vLLM, Ollama,
/// LiteLLM, OpenRouter, and the OpenAI SDKs. Clients configure `base_url=http://host/v1`
/// and reach `POST /v1/chat/completions` and `GET /v1/models` directly.
fn default_ai_gateway_path() -> String {
    "/v1".to_string()
}

/// Default max request body size for the AI gateway (128 MiB). Sized for
/// modern 1 M-token context windows plus tool schemas and history —
/// roughly 4 bytes per token after JSON overhead means a full 1 M-token
/// prompt lands around 4 MB; the 128 MiB ceiling absorbs multi-turn
/// histories and worst-case tool-payload growth without needing per-flow
/// overrides.
fn default_ai_gateway_max_body_bytes() -> usize {
    128 * 1024 * 1024
}

/// AI gateway server configuration options.
///
/// Exposes registered AI gateway flows as a single OpenAI-compatible endpoint.
/// Routing across gateways is driven by the `model` field of the request body
/// (`model: "<gateway-name>/<downstream-model>"`), so OpenAI clients work with
/// stock `base_url` configuration and no per-gateway URL changes.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AiGatewayOptions {
    /// Whether the AI gateway server is enabled. Must be explicitly enabled by
    /// the admin for `llm_proxy` tasks to be registered.
    pub enabled: bool,
    /// AI gateway port number. Defaults to 3002.
    #[serde(default = "default_ai_gateway_port")]
    pub port: u16,
    /// Path prefix for AI gateway routes. Defaults to "/v1".
    /// The chat completion endpoint is served at `<path>/chat/completions` and
    /// the model list at `<path>/models`.
    #[serde(default = "default_ai_gateway_path")]
    pub path: String,
    /// Optional path to global credentials file for API key authentication.
    /// Individual `llm_proxy` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all AI gateway flows on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
    /// Maximum request body size in bytes. Defaults to 128 MiB, which
    /// covers 1 M-token prompts with tool schemas and multi-turn
    /// histories. Requests over this limit are rejected with
    /// `413 Payload Too Large`.
    #[serde(default = "default_ai_gateway_max_body_bytes")]
    pub max_body_bytes: usize,
}

/// Default admin web UI port.
fn default_web_port() -> u16 {
    8080
}

/// Default admin web UI path prefix.
fn default_web_path() -> String {
    "/".to_string()
}

/// Admin web UI configuration options.
///
/// Serves a lightweight, read-only dashboard for inspecting loaded flows.
/// The UI is built into the binary as static assets.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct WebOptions {
    /// Whether the admin web UI is enabled.
    pub enabled: bool,
    /// Port for the admin web server. Defaults to 8080.
    #[serde(default = "default_web_port")]
    pub port: u16,
    /// Path prefix for the admin web UI. Defaults to "/".
    #[serde(default = "default_web_path")]
    pub path: String,
    /// Base URL the admin server proxies the built-in Agents chat to, e.g.
    /// `http://127.0.0.1:3002/v1`. Omit to target the same-process AI gateway
    /// on loopback. Set when the gateway runs in a separate process or pod.
    #[serde(default)]
    pub ai_gateway_url: Option<String>,
    /// Settings for the built-in Agents chat (conversation history, etc.).
    #[serde(default)]
    pub agents: AgentsOptions,
    /// HTTP headers sent with every outbound request the admin server
    /// makes (currently the Agents chat proxy to the AI gateway). Used to
    /// identify this admin server to `llm_proxy`/`mcp_tool` scoping, e.g.
    /// `X-Flowgen-Client: flowgen-ui` matching a proxy's `headers` field.
    #[serde(default)]
    pub headers: std::collections::HashMap<String, String>,
    /// OIDC login for the admin UI (Okta, Zitadel, Auth0, or any
    /// standard-compliant IdP). Omit to leave the admin UI unauthenticated,
    /// as today. When set, `cookie_secret` is required.
    #[serde(default)]
    pub auth: Option<crate::login::LoginConfig>,
    /// Key used to encrypt the browser session cookie (holds the IdP's
    /// tokens — flowgen keeps no server-side session store, see
    /// `crate::login`). At least 64 bytes recommended; shorter values are
    /// expanded via HKDF, but a short secret is still a short secret.
    /// Required when `auth` is set.
    #[serde(default, serialize_with = "serialize_redacted_cookie_secret")]
    pub cookie_secret: Option<secrecy::SecretString>,
    /// Whether the login cookies carry the `Secure` attribute, which browsers
    /// require HTTPS to send. Defaults to `true`; set to `false` only for a
    /// plain-HTTP deployment (local testing, or a proxy that terminates TLS
    /// and forwards HTTP — in which case the proxy, not flowgen, should be
    /// trusted to keep the connection private).
    #[serde(default = "default_cookie_secure")]
    pub cookie_secure: bool,
}

fn default_cookie_secure() -> bool {
    true
}

impl PartialEq for WebOptions {
    /// Compares `cookie_secret` by presence only, matching `JwtConfig`'s
    /// convention — `SecretString` deliberately has no `PartialEq`.
    fn eq(&self, other: &Self) -> bool {
        self.enabled == other.enabled
            && self.port == other.port
            && self.path == other.path
            && self.ai_gateway_url == other.ai_gateway_url
            && self.agents == other.agents
            && self.headers == other.headers
            && self.auth == other.auth
            && self.cookie_secret.is_some() == other.cookie_secret.is_some()
            && self.cookie_secure == other.cookie_secure
    }
}

fn serialize_redacted_cookie_secret<S>(
    secret: &Option<secrecy::SecretString>,
    s: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    match secret {
        Some(_) => s.serialize_some("***"),
        None => s.serialize_none(),
    }
}

/// Settings for the built-in Agents chat surface.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct AgentsOptions {
    /// How long a conversation lives in the configured cache before it
    /// expires. Refreshed on every write, so the window counts from the last
    /// activity. Defaults to 30 days; set to `0` to persist indefinitely.
    /// Long-term history belongs in a database via a persistence flow, not
    /// this cache.
    #[serde(default = "default_conversation_history_ttl", with = "humantime_serde")]
    pub conversation_history_ttl: Option<Duration>,
}

impl Default for AgentsOptions {
    fn default() -> Self {
        Self {
            conversation_history_ttl: default_conversation_history_ttl(),
        }
    }
}

/// Default conversation history TTL for the built-in Agents chat: 30 days.
fn default_conversation_history_ttl() -> Option<Duration> {
    Some(Duration::from_secs(30 * 24 * 60 * 60))
}

/// Default health listener port.
fn default_health_port() -> u16 {
    8081
}

/// Default `enabled: true` for HealthOptions when the field is omitted.
fn default_health_enabled() -> bool {
    true
}

/// Kubernetes liveness/readiness listener configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(default)]
pub struct HealthOptions {
    /// Whether the health listener is enabled. Defaults to true.
    pub enabled: bool,
    /// Health listener port. Defaults to 8081.
    pub port: u16,
}

impl Default for HealthOptions {
    fn default() -> Self {
        Self {
            enabled: default_health_enabled(),
            port: default_health_port(),
        }
    }
}

/// Default webhook HTTP server port.
fn default_http_port() -> u16 {
    3000
}

/// Default webhook HTTP server path prefix.
fn default_http_path() -> String {
    "/api/flowgen/workers/v1".to_string()
}

/// HTTP server configuration options.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct HttpServerOptions {
    /// Whether HTTP server is enabled.
    pub enabled: bool,
    /// HTTP server port number. Defaults to 3000.
    #[serde(default = "default_http_port")]
    pub port: u16,
    /// Path prefix for all webhook routes. Defaults to "/api/flowgen/workers/v1".
    #[serde(default = "default_http_path")]
    pub path: String,
    /// Optional path to global credentials file for webhook authentication.
    /// Individual `http_endpoint` tasks can override this with their own `credentials_path`.
    pub credentials_path: Option<std::path::PathBuf>,
    /// Optional auth provider configuration for user identity resolution.
    /// Shared across all HTTP-facing tasks on this worker.
    pub auth: Option<flowgen_core::auth::AuthConfig>,
}

/// OpenTelemetry configuration for metrics, tracing, and log export.
///
/// When `enabled` is false the runtime skips telemetry entirely. When
/// enabled without a `backend` block the runtime falls back to the
/// in-memory backend, so `enabled: true` with no other fields is a
/// valid standalone configuration.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct TelemetryOptions {
    /// Whether OpenTelemetry is enabled.
    pub enabled: bool,
    /// Backend selection. Omitted means in-memory.
    #[serde(default)]
    pub backend: Option<TelemetryBackendOptions>,
    /// Service name for resource identification (defaults to "flowgen").
    #[serde(default = "default_service_name")]
    pub service_name: String,
    /// Metrics export interval (defaults to "60s").
    /// Accepts human-readable durations: "30s", "1m", "5m".
    #[serde(default = "default_metrics_interval", with = "humantime_serde")]
    pub metrics_export_interval: Duration,
}

/// User-facing backend selection.
///
/// Serialized as a tagged enum. Each variant carries only the fields
/// that make sense for that backend, so an invalid combination
/// (e.g. `logs_per_flow` on the `remote` backend) fails at parse time.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TelemetryBackendOptions {
    /// Keep all signals in-process behind bounded per-flow ring buffers.
    /// Intended for demo and single-node dev; multi-replica deploys
    /// should use `remote` instead.
    Memory {
        /// Log records retained per flow before oldest entries are
        /// dropped. Defaults to 1000.
        #[serde(default = "default_logs_per_flow")]
        logs_per_flow: usize,
        /// Metric samples retained per flow before oldest entries are
        /// dropped. Defaults to 1000.
        #[serde(default = "default_metrics_per_flow")]
        metrics_per_flow: usize,
    },
    /// Push all signals over OTLP/gRPC to a remote collector.
    Remote {
        /// gRPC endpoint of the collector (e.g. "http://otel-collector:4317").
        endpoint: String,
    },
}

fn default_service_name() -> String {
    "flowgen".to_string()
}

fn default_metrics_interval() -> Duration {
    Duration::from_secs(60)
}

fn default_logs_per_flow() -> usize {
    1000
}

fn default_metrics_per_flow() -> usize {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use std::path::PathBuf;

    #[test]
    fn test_flow_config_creation() {
        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("test_flow".to_string()),
                labels: None,
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
            },
        };
        let flow_config =
            FlowConfig::from_path(raw, "test_flow".to_string(), None).expect("valid identity");

        assert_eq!(flow_config.flow.name.as_deref(), Some("test_flow"));
        assert!(flow_config.flow.labels.is_none());
        assert!(flow_config.flow.tasks.is_empty());
        assert_eq!(flow_config.identity(), "test_flow");
    }

    #[test]
    fn test_flow_config_raw_serialization_roundtrips() {
        let mut labels = Map::new();
        labels.insert("environment".to_string(), Value::String("test".to_string()));

        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("serialize_test".to_string()),
                labels: Some(labels),
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
            },
        };

        let serialized = serde_json::to_string(&raw).unwrap();
        let deserialized: FlowConfigRaw = serde_json::from_str(&serialized).unwrap();
        assert_eq!(raw, deserialized);
    }

    #[test]
    fn from_path_ignores_missing_flow_name() {
        let raw = FlowConfigRaw {
            flow: Flow {
                name: None,
                labels: None,
                tasks: vec![],
                require_leader_election: None,
                parallel_instances: 1,
            },
        };
        let config = FlowConfig::from_path(raw, "demo/reader".to_string(), None)
            .expect("path is identity, name not required");
        assert_eq!(config.identity(), "demo/reader");
        assert_eq!(config.display_name(), "reader");
    }

    #[test]
    fn test_flow_creation() {
        let mut labels = Map::new();
        labels.insert("type".to_string(), Value::String("test".to_string()));

        let flow = Flow {
            name: Some("test_flow".to_string()),
            labels: Some(labels.clone()),
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
        };

        assert_eq!(flow.name.as_deref(), Some("test_flow"));
        assert_eq!(flow.labels, Some(labels));
        assert!(flow.tasks.is_empty());
    }

    #[test]
    fn test_flow_with_tasks() {
        let convert_config = flowgen_core::task::convert::config::Processor::default();
        let task = TaskType::convert(convert_config);

        let flow = Flow {
            name: Some("flow_with_tasks".to_string()),
            labels: None,
            tasks: vec![task],
            require_leader_election: None,
            parallel_instances: 1,
        };

        assert_eq!(flow.name.as_deref(), Some("flow_with_tasks"));
        assert!(flow.labels.is_none());
        assert_eq!(flow.tasks.len(), 1);
        assert!(matches!(flow.tasks[0], TaskType::convert(_)));
    }

    #[test]
    fn test_flow_serialization() {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Serializable Flow".to_string()),
        );

        let flow = Flow {
            name: Some("serialize_flow".to_string()),
            labels: Some(labels),
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
        };

        let serialized = serde_json::to_string(&flow).unwrap();
        let deserialized: Flow = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow, deserialized);
    }

    #[test]
    fn test_flow_clone() {
        let flow = Flow {
            name: Some("clone_test".to_string()),
            labels: None,
            tasks: vec![],
            require_leader_election: None,
            parallel_instances: 1,
        };

        let cloned = flow.clone();
        assert_eq!(flow, cloned);
    }

    #[test]
    fn test_task_variants() {
        let convert_task =
            TaskType::convert(flowgen_core::task::convert::config::Processor::default());
        let generate_task =
            TaskType::generate(flowgen_core::task::generate::config::Subscriber::default());

        assert!(matches!(convert_task, TaskType::convert(_)));
        assert!(matches!(generate_task, TaskType::generate(_)));
    }

    #[test]
    fn test_app_config_creation() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/test/cache")),
                url: "localhost:4222".to_string(),
                db_name: None,
                runtime: BucketOptions::default(),
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/test/flows/*")),
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        assert!(app_config.cache.is_some());
        assert!(app_config.cache.as_ref().unwrap().enabled);
        assert!(app_config.flows.path.is_some());
        assert!(app_config.telemetry.is_none());
        assert!(app_config.http_server.is_none());
        assert!(app_config.retry.is_none());
        assert!(app_config.event_buffer_size.is_none());
        assert!(app_config.resources.is_none());
    }

    #[test]
    fn test_app_config_without_cache() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: Some(PathBuf::from("/flows/*")),
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        assert!(app_config.cache.is_none());
        assert!(app_config.flows.path.is_some());
    }

    #[test]
    fn test_app_config_serialization() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: false,
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/serialize/cache")),
                url: "localhost:4222".to_string(),
                db_name: None,
                runtime: BucketOptions {
                    db_name: "test_db".to_string(),
                },
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: Some(PathBuf::from("/serialize/flows/*")),
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        let serialized = serde_json::to_string(&app_config).unwrap();
        let deserialized: AppConfig = serde_json::from_str(&serialized).unwrap();
        assert_eq!(app_config, deserialized);
    }

    #[test]
    fn test_app_config_clone() {
        let app_config = AppConfig {
            cache: Some(CacheOptions {
                enabled: true,
                cache_type: CacheType::Nats,
                credentials_path: Some(PathBuf::from("/clone/cache")),
                url: "localhost:4222".to_string(),
                db_name: None,
                runtime: BucketOptions::default(),
                system: SystemBucketOptions::default(),
                history: None,
                tombstone_ttl: None,
            }),
            flows: FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        let cloned = app_config.clone();
        assert_eq!(app_config, cloned);
    }

    #[test]
    fn validate_rejects_pre_0_135_cache_db_name() {
        let yaml = r#"
cache:
  enabled: true
  type: nats
  url: "localhost:4222"
  db_name: "legacy_bucket"
flows:
  path: "/flows"
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).expect("parses");
        let err = config.validate().expect_err("renamed key must be rejected");
        assert!(
            err.to_string().contains("legacy_bucket"),
            "the error must name the bucket the operator would lose: {err}"
        );
    }

    #[test]
    fn validate_accepts_the_new_bucket_layout() {
        let yaml = r#"
cache:
  enabled: true
  type: nats
  url: "localhost:4222"
  runtime:
    db_name: "legacy_bucket"
flows:
  path: "/flows"
"#;
        let config: AppConfig = serde_yaml::from_str(yaml).expect("parses");
        assert_eq!(
            config.cache.as_ref().unwrap().runtime.db_name,
            "legacy_bucket"
        );
        config.validate().expect("new layout is accepted");
    }

    #[test]
    fn test_cache_options_creation() {
        let cache_options = CacheOptions {
            enabled: true,
            cache_type: CacheType::Nats,
            credentials_path: Some(PathBuf::from("/test/credentials_path")),
            url: "localhost:4222".to_string(),
            db_name: None,
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        };

        assert!(cache_options.enabled);
        assert_eq!(
            cache_options.credentials_path,
            Some(PathBuf::from("/test/credentials_path"))
        );
    }

    #[test]
    fn test_cache_options_disabled() {
        let cache_options = CacheOptions {
            enabled: false,
            cache_type: CacheType::Nats,
            credentials_path: Some(PathBuf::from("/disabled/cache")),
            url: "localhost:4222".to_string(),
            db_name: None,
            runtime: BucketOptions {
                db_name: "custom_db".to_string(),
            },
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        };

        assert!(!cache_options.enabled);
        assert_eq!(
            cache_options.credentials_path,
            Some(PathBuf::from("/disabled/cache"))
        );
    }

    #[test]
    fn test_cache_options_serialization() {
        let cache_options = CacheOptions {
            enabled: true,
            cache_type: CacheType::Nats,
            credentials_path: Some(PathBuf::from("/serialize/credentials_path")),
            url: "localhost:4222".to_string(),
            db_name: None,
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: None,
            tombstone_ttl: None,
        };

        let serialized = serde_json::to_string(&cache_options).unwrap();
        let deserialized: CacheOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cache_options, deserialized);
    }

    #[test]
    fn test_flow_options_with_path() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/test/flows/*.toml")),
            cache: None,
        };

        assert!(flow_options.path.is_some());
        assert_eq!(
            flow_options.path.unwrap(),
            PathBuf::from("/test/flows/*.toml")
        );
    }

    #[test]
    fn test_flow_options_without_path() {
        let flow_options = FlowOptions {
            path: None,
            cache: None,
        };

        assert!(flow_options.path.is_none());
    }

    #[test]
    fn test_flow_options_serialization() {
        let flow_options = FlowOptions {
            path: Some(PathBuf::from("/serialize/flows/*.toml")),
            cache: None,
        };

        let serialized = serde_json::to_string(&flow_options).unwrap();
        let deserialized: FlowOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(flow_options, deserialized);
    }

    #[test]
    fn test_complex_flow_config() {
        let convert_config = flowgen_core::task::convert::config::Processor {
            name: "convert_task".to_string(),
            ..Default::default()
        };
        let generate_config = flowgen_core::task::generate::config::Subscriber {
            name: "generate_task".to_string(),
            ..Default::default()
        };

        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Complex Multi-Task Flow".to_string()),
        );
        labels.insert("complexity".to_string(), Value::String("high".to_string()));

        let raw = FlowConfigRaw {
            flow: Flow {
                name: Some("complex_flow".to_string()),
                labels: Some(labels.clone()),
                tasks: vec![
                    TaskType::convert(convert_config),
                    TaskType::generate(generate_config),
                ],
                require_leader_election: None,
                parallel_instances: 1,
            },
        };
        let flow_config =
            FlowConfig::from_path(raw, "complex_flow".to_string(), None).expect("valid identity");

        assert_eq!(flow_config.flow.name.as_deref(), Some("complex_flow"));
        assert_eq!(flow_config.flow.labels, Some(labels));
        assert_eq!(flow_config.flow.tasks.len(), 2);
        assert!(matches!(flow_config.flow.tasks[0], TaskType::convert(_)));
        assert!(matches!(flow_config.flow.tasks[1], TaskType::generate(_)));
    }

    #[test]
    fn test_http_server_options_creation() {
        let http_server_options = HttpServerOptions {
            enabled: true,
            port: 8080,
            path: "/workers".to_string(),
            credentials_path: None,
            auth: None,
        };

        assert!(http_server_options.enabled);
        assert_eq!(http_server_options.port, 8080);
    }

    #[test]
    fn test_http_server_options_without_port() {
        let http_server_options = HttpServerOptions {
            enabled: false,
            port: 3000,
            path: "/api/flowgen/workers/v1".to_string(),
            credentials_path: None,
            auth: None,
        };

        assert!(!http_server_options.enabled);
        assert_eq!(http_server_options.port, 3000);
    }

    #[test]
    fn test_app_config_with_http_server_options() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: Some(HttpServerOptions {
                enabled: true,
                port: 8080,
                path: "/workers".to_string(),
                credentials_path: None,
                auth: None,
            }),
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: None,
        };

        assert!(app_config.http_server.is_some());
        let http_server = app_config.http_server.as_ref().unwrap();
        assert!(http_server.enabled);
        assert_eq!(http_server.port, 8080);
        assert_eq!(http_server.path, "/workers");
    }

    #[test]
    fn test_telemetry_options_remote_backend() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: Some(TelemetryBackendOptions::Remote {
                endpoint: "http://otel-collector:4317".to_string(),
            }),
            service_name: "flowgen".to_string(),
            metrics_export_interval: Duration::from_secs(30),
        };

        assert!(telemetry_options.enabled);
        assert!(matches!(
            &telemetry_options.backend,
            Some(TelemetryBackendOptions::Remote { endpoint }) if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(telemetry_options.service_name, "flowgen");
        assert_eq!(
            telemetry_options.metrics_export_interval,
            Duration::from_secs(30)
        );
    }

    #[test]
    fn test_telemetry_options_defaults_to_no_backend() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: None,
            service_name: default_service_name(),
            metrics_export_interval: default_metrics_interval(),
        };

        assert!(telemetry_options.enabled);
        assert!(telemetry_options.backend.is_none());
        assert_eq!(telemetry_options.service_name, "flowgen");
        assert_eq!(
            telemetry_options.metrics_export_interval,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn test_telemetry_options_serialization_roundtrip() {
        let telemetry_options = TelemetryOptions {
            enabled: true,
            backend: Some(TelemetryBackendOptions::Remote {
                endpoint: "http://grafana:4317".to_string(),
            }),
            service_name: "flowgen-prod".to_string(),
            metrics_export_interval: Duration::from_secs(120),
        };

        let serialized = serde_json::to_string(&telemetry_options).unwrap();
        let deserialized: TelemetryOptions = serde_json::from_str(&serialized).unwrap();
        assert_eq!(telemetry_options, deserialized);
    }

    #[test]
    fn test_telemetry_backend_tagged_enum_yaml() {
        let yaml_remote = "type: remote\nendpoint: http://otel:4317\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_remote).unwrap();
        assert!(matches!(
            &parsed,
            TelemetryBackendOptions::Remote { endpoint } if endpoint == "http://otel:4317"
        ));

        let yaml_memory = "type: memory\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_memory).unwrap();
        assert!(matches!(
            parsed,
            TelemetryBackendOptions::Memory {
                logs_per_flow: 1000,
                metrics_per_flow: 1000,
            }
        ));

        let yaml_memory_custom = "type: memory\nlogs_per_flow: 250\nmetrics_per_flow: 500\n";
        let parsed: TelemetryBackendOptions = serde_yaml::from_str(yaml_memory_custom).unwrap();
        assert!(matches!(
            parsed,
            TelemetryBackendOptions::Memory {
                logs_per_flow: 250,
                metrics_per_flow: 500,
            }
        ));
    }

    #[test]
    fn test_app_config_with_telemetry() {
        let app_config = AppConfig {
            cache: None,
            flows: FlowOptions {
                path: None,
                cache: None,
            },
            resources: None,
            http_server: None,
            mcp_server: None,
            ai_gateway: None,
            web: None,
            health: Default::default(),
            retry: None,
            event_buffer_size: None,
            telemetry: Some(TelemetryOptions {
                enabled: true,
                backend: Some(TelemetryBackendOptions::Remote {
                    endpoint: "http://otel-collector:4317".to_string(),
                }),
                service_name: "flowgen".to_string(),
                metrics_export_interval: Duration::from_secs(60),
            }),
        };

        assert!(app_config.telemetry.is_some());
        let telemetry = app_config.telemetry.as_ref().unwrap();
        assert!(telemetry.enabled);
        assert!(matches!(
            &telemetry.backend,
            Some(TelemetryBackendOptions::Remote { endpoint }) if endpoint == "http://otel-collector:4317"
        ));
        assert_eq!(telemetry.service_name, "flowgen");
        assert_eq!(telemetry.metrics_export_interval, Duration::from_secs(60));
    }
}
