use crate::config::{AppConfig, FlowConfig, FlowConfigRaw};
use config::Config;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
};
use tracing::{error, info, warn, Instrument};

/// Tracks a running flow.
///
/// Held in the flow registry so the reconciler knows how to stop a flow
/// cleanly: cancel its tasks, await the join handle, deregister the flow's
/// lease background task from the `TaskManager` so the renewer stops bumping
/// the lease key, and bulk-deregister any webhook / MCP tool / AI gateway
/// entries the flow owns from the shared servers (via each server's own
/// `deregister_flow(flow_name)`).
pub struct FlowHandle {
    /// Flow identity: the path-shaped key from the loader (filesystem-
    /// relative or cache-key suffix), or `flow.name` when the flow was
    /// constructed programmatically. Registry key, cache namespace,
    /// activity keys, and log fields all derive from this.
    pub identity: String,
    /// Human-readable name extracted from `labels.display_name`. Falls back
    /// to `identity` (or its basename) in UI when absent.
    pub flow_display_name: Option<String>,
    /// Optional description extracted from flow labels.
    pub flow_description: Option<String>,
    /// Tags extracted from `labels.tags` (empty when none).
    pub flow_tags: Vec<String>,
    /// Whether the flow requires leader election.
    pub require_leader_election: bool,
    /// Number of tasks in the flow.
    pub task_count: usize,
    /// Wall-clock time when the flow's supervisor was spawned. Surfaced on
    /// the admin API as `last_run` until per-event tracking replaces it.
    pub started_at: std::time::SystemTime,
    /// YAML source of the flow config, serialized at registration time.
    /// Surfaced on the admin API so operators can inspect the loaded flow
    /// without shelling into the pod to `cat` the source file.
    pub flow_yaml: String,
    /// Token used to signal the flow's tasks to stop gracefully.
    pub cancellation_token: tokio_util::sync::CancellationToken,
    /// Join handle for the flow's background monitor task spawned by `run()`.
    pub join_handle: tokio::task::JoinHandle<()>,
    /// True when the flow was loaded from the filesystem at startup.
    /// Cache-sourced reload events must not overwrite filesystem flows.
    pub from_filesystem: bool,
    /// `TaskManager` owning this flow's lease renewal background task.
    ///
    /// Required at shutdown / hot-reload so the renewer can be aborted via
    /// `TaskManager::unregister(flow_name)`. Without this the spawned tokio
    /// task survives the `Flow` drop and keeps writing renewals to the lease
    /// key, racing the replacement flow's renewer under the same pod-level
    /// holder identity.
    pub task_manager: Option<Arc<flowgen_core::task::manager::TaskManager>>,
}

impl FlowHandle {
    /// Returns the flow identity (registry key, cache namespace, activity key).
    pub fn identity(&self) -> &str {
        &self.identity
    }

    /// Returns the display name, if any.
    pub fn display_name(&self) -> Option<&str> {
        self.flow_display_name.as_deref()
    }

    /// Returns the flow description, if any.
    pub fn description(&self) -> Option<&str> {
        self.flow_description.as_deref()
    }

    /// Returns the flow tags (empty when none).
    pub fn tags(&self) -> &[String] {
        &self.flow_tags
    }

    /// Returns true if the flow requires leader election.
    pub fn require_leader_election(&self) -> bool {
        self.require_leader_election
    }

    /// Returns the number of tasks in the flow.
    pub fn task_count(&self) -> usize {
        self.task_count
    }

    /// YAML source of the flow config.
    pub fn flow_yaml(&self) -> &str {
        &self.flow_yaml
    }

    /// Wall-clock start time of the flow's supervisor task.
    pub fn started_at(&self) -> std::time::SystemTime {
        self.started_at
    }

    /// True when the flow's supervisor is still executing. A `false` here
    /// means the flow has exited (usually via an unhandled task panic or
    /// error), so callers can surface it as an `error` status.
    pub fn is_running(&self) -> bool {
        !self.join_handle.is_finished()
    }
}

/// Errors that can occur during application execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Input/output operation failed.
    #[error("IO error on path {path}: {source}")]
    IO {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// File system error occurred while globbing flow configuration files.
    #[error("Error globbing flow configuration files: {source}")]
    Glob {
        #[source]
        source: glob::GlobError,
    },
    /// Invalid glob pattern provided for flow discovery.
    #[error("Invalid glob pattern: {source}")]
    Pattern {
        #[source]
        source: glob::PatternError,
    },
    /// Configuration parsing or deserialization error.
    #[error("Error parsing configuration: {source}")]
    Config {
        #[source]
        source: config::ConfigError,
    },
    /// Flow path is missing or invalid.
    #[error("Flow path is not configured or invalid. Please set 'flows.path' in your configuration (e.g., flows.path: \"/etc/app/flows/*.yaml\")")]
    InvalidFlowsPath,
    /// Two flows resolved to the same identity, which must be unique.
    #[error("Duplicate flow identity {identity:?}: two flows resolve to the same path. Flow identities must be unique.")]
    DuplicateFlowIdentity {
        /// The colliding identity shared by more than one flow.
        identity: String,
    },
    /// Flow build error.
    #[error("Flow build failed: {source}")]
    FlowBuild {
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// Flow initialization error.
    #[error("Flow initialization failed for {flow_name}: {source}")]
    FlowInit {
        flow_name: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// HTTP handler startup error.
    #[error("Failed to run HTTP handlers for {flow_name}: {source}")]
    HttpHandlerStartup {
        flow_name: String,
        #[source]
        source: Box<crate::flow::Error>,
    },
    /// HTTP handler setup completion error.
    #[error("Failed to complete HTTP handler setup: {source}")]
    HttpHandlerSetup {
        #[source]
        source: tokio::task::JoinError,
    },
    /// HTTP server startup error.
    #[error("Failed to start HTTP server: {source}")]
    HttpServerStart {
        #[source]
        source: flowgen_core::http_server::Error,
    },
    /// Admin UI login is enabled but has no cookie-signing secret set.
    #[error(
        "Admin UI login is enabled (web.auth) but web.cookie_secret is missing. Set \
         web.cookie_secret to a random string so logged-in sessions survive a restart."
    )]
    MissingCookieSecret,
    /// Admin UI could not reach or was rejected by the configured identity provider.
    #[error("Admin UI login could not start: {source}")]
    LoginClientStart {
        #[source]
        source: crate::login::LoginError,
    },
    /// MCP server startup error.
    #[error("Failed to start MCP server: {source}")]
    McpServerStart {
        #[source]
        source: flowgen_core::http_server::Error,
    },
    /// AI gateway server startup error.
    #[error("Failed to start AI gateway server: {source}")]
    AiGatewayServerStart {
        #[source]
        source: flowgen_core::http_server::Error,
    },
    /// Auth provider initialization failed.
    #[error("Failed to build auth provider: {source}")]
    AuthProviderInit {
        #[source]
        source: flowgen_core::auth::AuthError,
    },
    /// Failed to read MCP credentials file.
    #[error("Failed to read MCP credentials from {path}: {source}")]
    McpCredentialsRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to parse MCP credentials file.
    #[error("Failed to parse MCP credentials from {path}: {source}")]
    McpCredentialsParse {
        path: std::path::PathBuf,
        #[source]
        source: serde_json::Error,
    },
    /// Background task panic error.
    #[error("Background task panicked: {source}")]
    BackgroundTaskPanic {
        #[source]
        source: tokio::task::JoinError,
    },
    /// Flow file read error.
    #[error("Failed to read flow file {path:?}: {source}")]
    FlowFileRead {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Flow config parse error.
    #[error("Failed to parse flow config {path:?}: {source}")]
    FlowConfigParse {
        path: std::path::PathBuf,
        #[source]
        source: config::ConfigError,
    },
    /// Flow config deserialization error.
    #[error("Failed to deserialize flow config {path:?}: {source}")]
    FlowConfigDeserialize {
        path: std::path::PathBuf,
        #[source]
        source: config::ConfigError,
    },
    /// Flow file path canonicalization error.
    #[error("Failed to resolve canonical path for {path:?}: {source}")]
    FlowFileCanonicalize {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    /// Failed to initialize the system cache for flow/resource loading.
    #[error("Failed to connect to NATS: {source}")]
    SystemCacheInit {
        #[source]
        source: flowgen_nats::cache::Error,
    },
    /// Failed to list keys from the system cache.
    #[error("Failed to list flow keys from cache: {source}")]
    CacheListKeys {
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// Failed to read a flow from the system cache.
    #[error("Failed to read flow {key:?} from cache: {source}")]
    CacheFlowRead {
        key: String,
        #[source]
        source: flowgen_core::cache::Error,
    },
    /// Failed to parse flow content from cache as UTF-8.
    #[error("Flow {key:?} from cache is not valid UTF-8: {source}")]
    CacheFlowUtf8 {
        key: String,
        #[source]
        source: std::string::FromUtf8Error,
    },
    /// Failed to parse flow config from cache.
    #[error("Failed to parse flow config from cache key {key:?}: {source}")]
    CacheFlowConfigParse {
        key: String,
        #[source]
        source: config::ConfigError,
    },
}
/// Main application that loads and runs flows concurrently.
pub struct App {
    /// Global application configuration.
    pub config: AppConfig,
    /// Shared metrics store populated by the tracing activity layer, read
    /// by the admin web API for status and SSE.
    pub metrics_store: Arc<dyn flowgen_core::flow::activity::MetricsStore>,
    /// Cache backing flow/resource storage, activity publish, and runtime
    /// state. Built in `main` before tracing so the metrics store can be
    /// constructed with a real cache reference from the outset.
    pub cache: Arc<dyn flowgen_core::cache::Cache>,
    /// Backend-agnostic log query source used by the admin web API for
    /// history queries and SSE tail. `None` when the selected telemetry
    /// backend does not (yet) expose a query surface.
    pub logs_store: Option<Arc<dyn flowgen_core::telemetry::query::LogsStore>>,
}

/// The system bucket, opened once at startup, plus the name it was opened
/// under. Other buckets resolve through [`SystemBucket::open_sibling`] so a
/// config naming this same bucket reuses the connection instead of opening a
/// second one.
struct SystemBucket {
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    name: Option<String>,
}

impl SystemBucket {
    /// Opens the bucket named by `cache.system.db_name`. Yields an empty
    /// handle when the cache is disabled, leaving callers on their own
    /// buckets and the executor on a per-pod in-memory store.
    async fn open(app_config: &AppConfig) -> Self {
        let name = app_config.cache.as_ref().map(|c| c.system.db_name.clone());
        let cache = match &name {
            Some(db_name) => match App::init_cache(app_config, Some(db_name)).await {
                Ok(cache) => {
                    info!("Initialized system cache on bucket '{db_name}'.");
                    Some(cache)
                }
                Err(e) => {
                    warn!(error = %e, bucket = %db_name, "System cache unavailable");
                    None
                }
            },
            None => None,
        };
        Self { cache, name }
    }

    /// Returns the open system bucket when `db_name` names it, otherwise
    /// opens `db_name` as its own bucket.
    async fn open_sibling(
        &self,
        app_config: &AppConfig,
        db_name: &str,
    ) -> Result<Arc<dyn flowgen_core::cache::Cache>, Error> {
        match self
            .cache
            .as_ref()
            .filter(|_| self.name.as_deref() == Some(db_name))
        {
            Some(cache) => Ok(Arc::clone(cache)),
            None => App::init_cache(app_config, Some(db_name)).await,
        }
    }
}

impl App {
    /// Builds the runtime cache from config (NATS if enabled and reachable,
    /// otherwise in-memory). Callable before tracing is up so `main` can
    /// hand a real cache to the metrics store builder.
    pub async fn init_cache(
        app_config: &AppConfig,
        db_name: Option<&str>,
    ) -> Result<Arc<dyn flowgen_core::cache::Cache>, Error> {
        let Some(cache_config) = &app_config.cache else {
            return Ok(Arc::new(flowgen_core::cache::memory::MemoryCache::new()));
        };
        if !cache_config.enabled {
            return Ok(Arc::new(flowgen_core::cache::memory::MemoryCache::new()));
        }
        let db_name = db_name.unwrap_or(cache_config.runtime.db_name.as_str());
        let mut cache_builder =
            flowgen_nats::cache::CacheBuilder::new().url(cache_config.url.clone());
        if let Some(path) = cache_config.credentials_path.clone() {
            cache_builder = cache_builder.credentials_path(path);
        }
        if let Some(history) = cache_config.history {
            cache_builder = cache_builder.history(history);
        }
        if let Some(ttl) = cache_config.tombstone_ttl {
            cache_builder = cache_builder.tombstone_ttl(ttl);
        }
        match cache_builder.build() {
            Ok(builder) => match builder.init(db_name).await {
                Ok(nats_cache) => Ok(Arc::new(nats_cache)),
                Err(e) => Err(Error::SystemCacheInit { source: e }),
            },
            Err(e) => Err(Error::SystemCacheInit { source: e }),
        }
    }

    /// Loads flow configurations from a system cache bucket.
    /// Lists all keys under the given prefix, reads each value, and parses as FlowConfig.
    async fn load_flows_from_cache(
        cache: &dyn flowgen_core::cache::Cache,
        prefix: &str,
    ) -> Result<Vec<(String, FlowConfig)>, Error> {
        let keys = cache
            .list_keys(prefix)
            .await
            .map_err(|source| Error::CacheListKeys { source })?;

        let mut flow_configs = Vec::new();
        for key in keys {
            let bytes = match cache
                .get(&key)
                .await
                .map_err(|source| Error::CacheFlowRead {
                    key: key.clone(),
                    source,
                })? {
                Some(b) => b,
                None => {
                    warn!(key = %key, "Flow key listed but not found, skipping");
                    continue;
                }
            };

            let content =
                String::from_utf8(bytes.to_vec()).map_err(|source| Error::CacheFlowUtf8 {
                    key: key.clone(),
                    source,
                })?;

            // Determine format from key extension, default to YAML.
            let file_format = if key.ends_with(".json") {
                config::FileFormat::Json
            } else {
                config::FileFormat::Yaml
            };

            let config = Config::builder()
                .add_source(config::File::from_str(&content, file_format))
                .build()
                .map_err(|source| Error::CacheFlowConfigParse {
                    key: key.clone(),
                    source,
                })?;

            match config.try_deserialize::<FlowConfigRaw>() {
                Ok(raw) => {
                    let identity_path = match key
                        .strip_prefix(prefix)
                        .and_then(|rest| rest.strip_prefix('.'))
                    {
                        Some(p) => p.to_string(),
                        None => {
                            error!(
                                key = %key,
                                prefix = %prefix,
                                "Cache key does not start with the configured prefix, skipping"
                            );
                            continue;
                        }
                    };
                    match FlowConfig::from_path(raw, identity_path, Some(content.clone())) {
                        Ok(flow_config) => {
                            info!(
                                flow = %flow_config.identity(),
                                key = %key,
                                "Loaded flow from cache",
                            );
                            flow_configs.push((key, flow_config));
                        }
                        Err(reason) => {
                            error!(
                                key = %key,
                                error = %reason,
                                "Flow config from cache failed validation, skipping"
                            );
                        }
                    }
                }
                Err(source) => {
                    error!(
                        key = %key,
                        error = %source,
                        "Failed to deserialize flow config from cache, skipping"
                    );
                }
            }
        }

        Ok(flow_configs)
    }

    /// Loads flow configurations from the filesystem using the configured path.
    ///
    /// Returns an empty vector when `flows.path` is not configured. This allows
    /// the worker to run cache-only (every flow loaded from the system cache)
    /// or to mix sources (a few local flows mounted from disk, the rest pulled
    /// from the system cache by a sync flow).
    fn load_flows_from_filesystem(app_config: &AppConfig) -> Result<Vec<FlowConfig>, Error> {
        let flows_path = match app_config.flows.path.as_ref() {
            Some(path) => path,
            None => return Ok(Vec::new()),
        };

        let flows_path_str = flows_path.to_str().ok_or(Error::InvalidFlowsPath)?;

        // Root is the longest non-wildcard prefix, used to derive each flow's
        // identity from its path relative to the root.
        let root_str: String = flows_path_str
            .split('/')
            .take_while(|seg| !seg.contains('*'))
            .collect::<Vec<_>>()
            .join("/");
        let source_root = Some(std::path::PathBuf::from(&root_str));

        // Check if path contains wildcards (backward compatibility).
        let glob_patterns: Vec<String> = if flows_path_str.contains('*') {
            vec![flows_path_str.to_string()]
        } else {
            crate::config::FLOW_CONFIG_EXTENSIONS
                .iter()
                .map(|ext| format!("{}/**/*.{}", flows_path_str.trim_end_matches('/'), ext))
                .collect()
        };

        let mut flow_configs: Vec<FlowConfig> = Vec::new();
        let mut seen_paths = std::collections::HashSet::new();

        for glob_pattern in glob_patterns {
            let matched_flows: Vec<FlowConfig> = glob::glob(&glob_pattern)
                .map_err(|e| Error::Pattern { source: e })?
                .filter_map(|path| match path {
                    Ok(path) => {
                        // Skip entries with a hidden (dot-prefixed) path
                        // segment. A ConfigMap mount exposes each file three
                        // ways (clean name, `..data` symlink, timestamped
                        // `..<ts>` dir) and a recursive glob matches all three;
                        // only the clean name has no dot segment, which both
                        // dedupes the entry and keeps identity stable across
                        // reloads.
                        if let Ok(rel) = path.strip_prefix(&root_str) {
                            let hidden = rel.components().any(|c| {
                                matches!(c, std::path::Component::Normal(seg)
                                    if seg.to_string_lossy().starts_with('.'))
                            });
                            if hidden {
                                return None;
                            }
                        }

                        let canonical_path = match std::fs::canonicalize(&path) {
                            Ok(p) => p,
                            Err(source) => {
                                let err = Error::FlowFileCanonicalize {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{}. Skipping this flow.", err);
                                return None;
                            }
                        };

                        if seen_paths.contains(&canonical_path) {
                            return None;
                        }
                        seen_paths.insert(canonical_path.clone());

                        let contents = match std::fs::read_to_string(&path) {
                            Ok(c) => c,
                            Err(source) => {
                                let err = Error::FlowFileRead {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{}. Skipping this flow.", err);
                                return None;
                            }
                        };

                        let file_format = match path.extension().and_then(|s| s.to_str()) {
                            Some("yaml") | Some("yml") => config::FileFormat::Yaml,
                            Some("json") => config::FileFormat::Json,
                            _ => config::FileFormat::Json,
                        };

                        let config = match Config::builder()
                            .add_source(config::File::from_str(&contents, file_format))
                            .build()
                        {
                            Ok(c) => c,
                            Err(source) => {
                                let err = Error::FlowConfigParse {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{}. Skipping this flow.", err);
                                return None;
                            }
                        };

                        match config.try_deserialize::<FlowConfigRaw>() {
                            Ok(raw) => {
                                let identity_path = match compute_source_path(
                                    &path,
                                    source_root.as_deref(),
                                ) {
                                    Some(p) => p,
                                    None => {
                                        error!(
                                            path = %path.display(),
                                            "Cannot derive flow identity from path (outside `flows.path` or missing root), skipping this flow"
                                        );
                                        return None;
                                    }
                                };
                                match FlowConfig::from_path(
                                    raw,
                                    identity_path,
                                    Some(contents),
                                ) {
                                    Ok(flow_config) => {
                                        info!(
                                            flow = %flow_config.identity(),
                                            "Loaded flow",
                                        );
                                        Some(flow_config)
                                    }
                                    Err(reason) => {
                                        error!(
                                            path = %path.display(),
                                            error = %reason,
                                            "Flow config failed validation, skipping this flow"
                                        );
                                        None
                                    }
                                }
                            }
                            Err(source) => {
                                let err = Error::FlowConfigDeserialize {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{}. Skipping this flow.", err);
                                None
                            }
                        }
                    }
                    Err(source) => {
                        let err = Error::Glob { source };
                        error!("{}. Skipping.", err);
                        None
                    }
                })
                .collect();

            flow_configs.extend(matched_flows);
        }

        Ok(flow_configs)
    }

    /// Loads flow configurations from disk, builds flows, starts HTTP server, and runs all tasks concurrently.
    ///
    /// This method discovers flow configuration files using the glob pattern specified in the app config,
    /// parses each configuration file, builds flow instances, registers HTTP routes, starts the HTTP server,
    /// and finally runs all flow tasks concurrently along with the server.
    ///
    /// The shutdown_rx parameter allows graceful shutdown by releasing all leases when a shutdown signal is received.
    pub async fn start(self, shutdown_rx: tokio::sync::oneshot::Receiver<()>) -> Result<(), Error> {
        let app_config = Arc::new(self.config);

        // Load flows from filesystem and (optionally) from the distributed cache.
        // The two sources are merged so a worker can run any combination of:
        //   - filesystem only (no cache section, classic mode);
        //   - cache only (no `flows.path` set, every flow comes from the cache);
        //   - hybrid (a small set of bootstrap flows mounted from disk while the
        //     bulk of user flows lives in the cache, populated by a sync flow).
        // On name collisions the filesystem entry wins, which keeps locally
        // mounted bootstrap flows from being silently overridden by a stale
        // cache entry with the same name.
        let filesystem_flows = Self::load_flows_from_filesystem(&app_config)?;
        info!("Loaded {} flows from filesystem.", filesystem_flows.len());

        // The system bucket backs leader-election leases and peer
        // registration, so it must exist whenever the cache does —
        // independently of whether flows happen to load from the cache too.
        let system = SystemBucket::open(&app_config).await;

        // Resolved once and reused by the hot-reload watcher further down, so
        // cache-backed flows cost one connection rather than one per consumer.
        let flow_cache = match app_config.flows.cache.as_ref() {
            Some(opts) if opts.enabled => {
                match system.open_sibling(&app_config, &opts.db_name).await {
                    Ok(cache) => Some((cache, opts)),
                    Err(e) => {
                        warn!(error = %e, "Flows will load from filesystem only");
                        None
                    }
                }
            }
            _ => None,
        };

        let cache_flows = match &flow_cache {
            Some((cache, opts)) => {
                let configs = Self::load_flows_from_cache(cache.as_ref(), &opts.prefix).await?;
                info!("Loaded {} flows from cache.", configs.len());
                configs
            }
            None => Vec::new(),
        };

        let mut flow_configs = filesystem_flows;
        // Reject two filesystem flows resolving to the same identity before
        // anything keys on it — an unusual mount layout can collapse two files
        // onto one relative path, and a silent collision on the registry,
        // cache, and lease keys downstream is worse than failing fast here.
        let mut identities: HashSet<&str> = HashSet::new();
        for config in &flow_configs {
            if !identities.insert(config.identity()) {
                return Err(Error::DuplicateFlowIdentity {
                    identity: config.identity().to_string(),
                });
            }
        }
        // Record filesystem flow identities (path-shaped) before merging so the
        // reconciler can enforce the invariant that cache reload events never
        // overwrite filesystem flows.
        let filesystem_flow_paths: HashSet<String> = flow_configs
            .iter()
            .map(|f| f.identity().to_string())
            .collect();
        let mut seen_paths = filesystem_flow_paths.clone();
        for (cache_key, cache_flow) in cache_flows {
            if seen_paths.insert(cache_flow.identity().to_string()) {
                flow_configs.push(cache_flow);
            } else {
                warn!(
                    flow = %cache_flow.identity(),
                    key = %cache_key,
                    "Cache flow shadowed by a filesystem flow with the same identity, deleting stale cache entry"
                );
                if let Some((cache, _)) = &flow_cache {
                    if let Err(e) = cache.delete(&cache_key).await {
                        warn!(
                            key = %cache_key,
                            error = %e,
                            "Failed to delete shadowed cache key"
                        );
                    }
                }
            }
        }

        // Create shared webhook HTTP server if enabled.
        let http_server: Option<Arc<flowgen_http::server::EndpointServer>> =
            match app_config.http_server.as_ref() {
                Some(http_config) if http_config.enabled => {
                    let path = http_config.path.clone();
                    let auth_provider = match http_config.auth.clone() {
                        Some(auth_config) => Some(
                            auth_config
                                .build()
                                .await
                                .map_err(|source| Error::AuthProviderInit { source })?,
                        ),
                        None => None,
                    };
                    let server = flowgen_core::http_server::HttpServer::<
                        flowgen_http::server::EndpointDispatcher,
                    >::new(path)
                    .with_credentials_path(http_config.credentials_path.clone())
                    .with_auth_provider(auth_provider);
                    Some(Arc::new(server))
                }
                _ => None,
            };

        let mcp_enabled = app_config
            .mcp_server
            .as_ref()
            .map(|mcp| mcp.enabled)
            .unwrap_or(false);

        let has_mcp_tasks = flow_configs.iter().any(|fc| {
            fc.flow.tasks.iter().any(|t| {
                matches!(
                    t,
                    crate::config::TaskType::mcp_tool(_)
                        | crate::config::TaskType::mcp_resource(_)
                        | crate::config::TaskType::mcp_prompt(_)
                )
            })
        });

        if has_mcp_tasks && !mcp_enabled {
            warn!("Flows contain MCP registration tasks but mcp_server is not enabled in config; registrations will be skipped");
        }

        let mcp_server: Option<Arc<flowgen_mcp::server::McpServer>> = if mcp_enabled {
            let mcp_config = app_config.mcp_server.as_ref();
            // Load MCP credentials if configured.
            let credentials = mcp_config
                .and_then(|mcp_config| mcp_config.credentials_path.as_ref())
                .and_then(|path| match std::fs::read_to_string(path) {
                    Ok(content) => {
                        match serde_json::from_str::<flowgen_mcp::config::Credentials>(&content) {
                            Ok(creds) => Some(creds),
                            Err(source) => {
                                let err = Error::McpCredentialsParse {
                                    path: path.clone(),
                                    source,
                                };
                                error!("{err}");
                                None
                            }
                        }
                    }
                    Err(source) => {
                        let err = Error::McpCredentialsRead {
                            path: path.clone(),
                            source,
                        };
                        error!("{err}");
                        None
                    }
                });
            let path = mcp_config
                .map(|c| c.path.clone())
                .unwrap_or_else(|| flowgen_mcp::server::DEFAULT_MCP_PATH.to_string());
            let resource_uri_scheme = mcp_config
                .map(|c| c.resource_uri_scheme.clone())
                .unwrap_or_else(|| "flowgen".to_string());
            let auth_provider = match mcp_config.and_then(|c| c.auth.clone()) {
                Some(auth_config) => Some(
                    auth_config
                        .build()
                        .await
                        .map_err(|source| Error::AuthProviderInit { source })?,
                ),
                None => None,
            };
            Some(Arc::new(flowgen_mcp::server::new_mcp_server(
                path,
                credentials,
                auth_provider,
                resource_uri_scheme,
            )))
        } else {
            None
        };

        // Create AI gateway server if enabled and any flow contains llm_proxy tasks.
        let has_ai_gateway_tasks = flow_configs.iter().any(|fc| {
            fc.flow
                .tasks
                .iter()
                .any(|t| matches!(t, crate::config::TaskType::llm_proxy(_)))
        });
        let ai_gateway_enabled = app_config
            .ai_gateway
            .as_ref()
            .map(|g| g.enabled)
            .unwrap_or(false);

        if has_ai_gateway_tasks && !ai_gateway_enabled {
            warn!("Flows contain llm_proxy tasks but ai_gateway is not enabled, LLM proxy endpoints will not be registered");
        }

        let ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>> =
            if ai_gateway_enabled {
                let ai_config = app_config.ai_gateway.as_ref();
                let path = ai_config.map(|c| c.path.clone()).unwrap_or_else(|| {
                    flowgen_ai_agent::ai_gateway::server::DEFAULT_AI_GATEWAY_PATH.to_string()
                });
                let credentials_path = ai_config.and_then(|c| c.credentials_path.clone());
                let auth_provider = match ai_config.and_then(|c| c.auth.clone()) {
                    Some(auth_config) => Some(
                        auth_config
                            .build()
                            .await
                            .map_err(|source| Error::AuthProviderInit { source })?,
                    ),
                    None => None,
                };
                let extras = match ai_config {
                    Some(c) => flowgen_ai_agent::ai_gateway::server::AiGatewayExtras {
                        max_body_bytes: c.max_body_bytes,
                    },
                    None => flowgen_ai_agent::ai_gateway::server::AiGatewayExtras::default(),
                };
                let server = flowgen_core::http_server::HttpServer::<
                    flowgen_ai_agent::ai_gateway::server::AiGatewayDispatcher,
                >::new_with_extras(path, extras)
                .with_credentials_path(credentials_path)
                .with_auth_provider(auth_provider);
                Some(Arc::new(server))
            } else {
                None
            };

        let cache = Arc::clone(&self.cache);

        // Build the resource loader. Filesystem and cache sources are
        // independent: configure either, both, or neither. When both are
        // active, the loader tries the filesystem first and falls back to
        // the cache on a miss, matching the gradual-migration semantics used
        // for flow loading.
        let resource_loader = match app_config.resources.as_ref() {
            Some(resource_options) => {
                let base_path = resource_options.path.clone();

                let cache_source = match resource_options.cache.as_ref() {
                    Some(rc) if rc.enabled => {
                        match system.open_sibling(&app_config, &rc.db_name).await {
                            Ok(cache) => Some((cache, rc.prefix.clone())),
                            Err(e) => {
                                warn!(error = %e, "Resources will load from filesystem only");
                                None
                            }
                        }
                    }
                    _ => None,
                };

                match (base_path, cache_source) {
                    (Some(path), Some((cache, prefix))) => Some(
                        flowgen_core::resource::ResourceLoader::new(Some(path))
                            .with_cache(cache, prefix),
                    ),
                    (Some(path), None) => {
                        Some(flowgen_core::resource::ResourceLoader::new(Some(path)))
                    }
                    (None, Some((cache, prefix))) => Some(
                        flowgen_core::resource::ResourceLoader::from_cache(cache, prefix),
                    ),
                    (None, None) => None,
                }
            }
            None => None,
        };

        // Shared client registry for deduplicating connections to external services.
        // Created once at the worker level and shared across all flows so tasks with
        // identical credentials (e.g. same Salesforce org) reuse the same client.
        let client_registry = Arc::new(flowgen_core::client_registry::ClientRegistry::new());

        // Leases and peer keys go to the system bucket, out of reach of
        // `ctx.cache`. It is absent only when the cache is disabled, which
        // leaves the executor writing to a per-pod in-memory store.
        let executor_cache: Arc<dyn flowgen_core::cache::Cache> = match &system.cache {
            Some(cache) => Arc::clone(cache),
            None => {
                if flow_configs
                    .iter()
                    .any(|f| f.flow.require_leader_election.unwrap_or(false))
                {
                    warn!(
                        "Leader election is configured but the cache is disabled, so leases \
                         are held in memory per pod. Every pod will elect itself and run \
                         these flows concurrently. Enable `cache` to coordinate across pods."
                    );
                }
                Arc::clone(&cache)
            }
        };

        // Resolved once and shared via FlowBuilder so every flow's Executor
        // and this pod's PeerRegistry agree on who "this pod" is.
        let holder_identity = flowgen_core::executor::resolve_holder_identity();

        // One instance per pod, not per flow: every flow's TaskManager
        // consults the same registry so the peer list reflects actual pod
        // count.
        let peer_registry = Arc::new(flowgen_core::peer::PeerRegistry::new(
            Arc::clone(&executor_cache),
            holder_identity.clone(),
        ));
        if let Err(e) = peer_registry.register().await {
            warn!("Failed to register peer: {}", e);
        }
        // Renewal is stopped by aborting this handle (below, alongside the
        // other background handles) rather than its own cancellation token —
        // one fewer token to keep in sync with the shutdown sequence.
        let peer_renewal_handle =
            peer_registry.spawn_renewal(tokio_util::sync::CancellationToken::new());

        // Build all flows from configuration files.
        let mut flows: Vec<super::flow::Flow> = Vec::new();
        for config in flow_configs {
            let http_server = http_server.as_ref().map(Arc::clone);

            let mut flow_builder = super::flow::FlowBuilder::new()
                .config(Arc::new(config))
                .cache(Arc::clone(&cache))
                .system_cache(Arc::clone(&executor_cache))
                .client_registry(Arc::clone(&client_registry))
                .holder_identity(holder_identity.clone())
                .peer_registry(Arc::clone(&peer_registry));

            if let Some(server) = http_server {
                flow_builder = flow_builder.http_server(server);
            }

            if let Some(ref server) = mcp_server {
                flow_builder = flow_builder.mcp_server(Arc::clone(server));
            }

            if let Some(ref server) = ai_gateway_server {
                flow_builder = flow_builder.ai_gateway_server(Arc::clone(server));
            }

            if let Some(retry_config) = app_config.retry.as_ref() {
                flow_builder = flow_builder.retry(retry_config.clone());
            }

            if let Some(buffer_size) = app_config.event_buffer_size {
                flow_builder = flow_builder.event_buffer_size(buffer_size);
            }

            if let Some(ref loader) = resource_loader {
                flow_builder = flow_builder.resource_loader(loader.clone());
            }

            match flow_builder.build() {
                Ok(flow) => flows.push(flow),
                Err(source) => {
                    let err = Error::FlowBuild {
                        source: Box::new(source),
                    };
                    error!("{}", err);
                    continue;
                }
            };
        }

        // Initialize flow setup.
        for flow in &mut flows {
            if let Err(source) = flow.init().await {
                let err = Error::FlowInit {
                    flow_name: flow.name().to_string(),
                    source: Box::new(source),
                };
                error!("{}", err);
            }
        }

        let mut http_handler_tasks = Vec::new();
        for flow in &flows {
            match flow.start_tasks().await {
                Ok(handles) => http_handler_tasks.extend(handles),
                Err(source) => {
                    let err = Error::HttpHandlerStartup {
                        flow_name: flow.name().to_string(),
                        source: Box::new(source),
                    };
                    error!("{}", err);
                }
            }
        }

        if !http_handler_tasks.is_empty() {
            info!(
                "Waiting for {} HTTP handler(s) to complete setup...",
                http_handler_tasks.len()
            );
            let results = futures_util::future::join_all(http_handler_tasks).await;
            for result in results {
                if let Err(source) = result {
                    let err = Error::HttpHandlerSetup { source };
                    error!("{}", err);
                }
            }
        }

        let mut background_handles = Vec::new();
        background_handles.push(peer_renewal_handle);
        if let Some(ref http_server) = http_server {
            let configured_port = app_config
                .http_server
                .as_ref()
                .map(|http| http.port)
                .unwrap_or(flowgen_http::server::DEFAULT_ENDPOINT_PORT);
            info!(port = configured_port, path = %http_server.path(), "Starting HTTP server");
            let http_server = Arc::clone(http_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = http_server.start_server(configured_port).await {
                        let err = Error::HttpServerStart { source };
                        error!("{}", err);
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        if let Some(ref mcp_server) = mcp_server {
            let configured_port = app_config
                .mcp_server
                .as_ref()
                .map(|c| c.port)
                .unwrap_or(flowgen_mcp::server::DEFAULT_MCP_PORT);
            info!(port = configured_port, path = %mcp_server.path(), "Starting MCP server");
            let server = Arc::clone(mcp_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = server.start_server(configured_port).await {
                        error!("{}", Error::McpServerStart { source });
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        if let Some(ref ai_gateway_server) = ai_gateway_server {
            let configured_port = app_config
                .ai_gateway
                .as_ref()
                .map(|c| c.port)
                .unwrap_or(flowgen_ai_agent::ai_gateway::server::DEFAULT_AI_GATEWAY_PORT);
            info!(port = configured_port, path = %ai_gateway_server.path(), "Starting AI gateway server");
            let server = Arc::clone(ai_gateway_server);
            let span = tracing::Span::current();
            let server_handle = tokio::spawn(
                async move {
                    if let Err(source) = server.start_server(configured_port).await {
                        error!("{}", Error::AiGatewayServerStart { source });
                    }
                }
                .instrument(span),
            );
            background_handles.push(server_handle);
        }

        // Collect task managers for shutdown cleanup.
        let task_managers: Vec<Arc<flowgen_core::task::manager::TaskManager>> =
            flows.iter().filter_map(|f| f.task_manager()).collect();

        // Build the flow registry keyed by flow name. The watcher reconciler uses
        // this to stop and deregister flows on hot-reload events.
        let flow_registry: Arc<RwLock<HashMap<String, FlowHandle>>> =
            Arc::new(RwLock::new(HashMap::new()));

        // Start all background flow tasks and populate the registry.
        for flow in flows {
            let identity = flow.identity().to_string();
            let from_filesystem = filesystem_flow_paths.contains(&identity);
            let flow_display_name = flow
                .config
                .flow
                .labels
                .as_ref()
                .and_then(|labels| labels.get("display_name"))
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
            let flow_description = flow
                .config
                .flow
                .labels
                .as_ref()
                .and_then(|labels| labels.get("description"))
                .and_then(|value| value.as_str())
                .map(ToString::to_string);
            let flow_tags = flow
                .config
                .flow
                .labels
                .as_ref()
                .and_then(|labels| labels.get("tags"))
                .and_then(|value| value.as_array())
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(ToString::to_string))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let require_leader_election = flow.config.flow.require_leader_election.unwrap_or(false);
            let task_count = flow.config.flow.tasks.len();
            let flow_yaml = match flow.config.raw_source.clone() {
                Some(source) => source,
                None => match serde_yaml::to_string(&*flow.config) {
                    Ok(yaml) => yaml,
                    Err(source) => {
                        warn!(
                            error = %source,
                            "Failed to serialize flow config to YAML for admin API"
                        );
                        String::new()
                    }
                },
            };

            let cancellation_token = flow
                .cancellation_token()
                .unwrap_or_else(tokio_util::sync::CancellationToken::new);

            let task_manager = flow.task_manager();

            let join_handle = flow.run();

            if let Ok(mut registry) = flow_registry.write() {
                registry.insert(
                    identity.clone(),
                    FlowHandle {
                        identity,
                        flow_display_name,
                        flow_description,
                        flow_tags,
                        require_leader_election,
                        task_count,
                        started_at: std::time::SystemTime::now(),
                        flow_yaml,
                        cancellation_token,
                        join_handle,
                        from_filesystem,
                        task_manager,
                    },
                );
            }
        }

        // Start the admin web UI if enabled.
        let web_config = app_config.web.as_ref();
        if let Some(web_config) = web_config.filter(|w| w.enabled) {
            let port = web_config.port;
            let path = web_config.path.clone();

            // A broken `web.auth` must not silently serve the admin UI
            // unauthenticated, but it also shouldn't take down flow
            // processing — so it skips starting the admin web server rather
            // than the whole process.
            let auth_setup: Option<(Option<Arc<crate::login::LoginClient>>, _)> =
                match &web_config.auth {
                    Some(login_config) => match &web_config.cookie_secret {
                        None => {
                            error!("{}", Error::MissingCookieSecret);
                            None
                        }
                        Some(secret) => {
                            match crate::login::LoginClient::new(login_config.clone()).await {
                                Ok(client) => {
                                    use secrecy::ExposeSecret;
                                    let key = axum_extra::extract::cookie::Key::derive_from(
                                        secret.expose_secret().as_bytes(),
                                    );
                                    Some((Some(Arc::new(client)), key))
                                }
                                Err(source) => {
                                    error!("{}", Error::LoginClientStart { source });
                                    None
                                }
                            }
                        }
                    },
                    None => Some((None, axum_extra::extract::cookie::Key::generate())),
                };

            if let Some((login_client, cookie_key)) = auth_setup {
                let web_state = crate::web::WebState {
                    flow_registry: Arc::clone(&flow_registry),
                    prefix: String::new(),
                    resource_loader: resource_loader.clone(),
                    metrics_store: Arc::clone(&self.metrics_store),
                    logs_store: self.logs_store.clone(),
                    app_config: Arc::clone(&app_config),
                    conversation_cache: Arc::clone(&executor_cache),
                    system_bucket_present: system.cache.is_some(),
                    conversation_history_ttl: web_config.agents.conversation_history_ttl,
                    login_client,
                    cookie_key,
                    cookie_secure: web_config.cookie_secure,
                };
                let web_handle = tokio::spawn(async move {
                    if let Err(source) = crate::web::start_web_server(port, &path, web_state).await
                    {
                        error!("{}", source);
                    }
                });
                background_handles.push(web_handle);
            }
        }

        // Start the dedicated k8s health listener. Readiness reports true once
        // at least one flow is registered; liveness is always 200.
        if app_config.health.enabled {
            let port = app_config.health.port;
            let registry_for_health = Arc::clone(&flow_registry);
            let readiness: flowgen_core::health::ReadinessCheck =
                Arc::new(move || match registry_for_health.read() {
                    Ok(guard) => !guard.is_empty(),
                    Err(_) => false,
                });
            let health_handle = tokio::spawn(async move {
                if let Err(source) =
                    flowgen_core::health::start_health_server(port, readiness).await
                {
                    error!("{}", source);
                }
            });
            background_handles.push(health_handle);
        }

        // Spawn the hot-reload watcher and reconciler if the system cache supports watching.
        // The watcher subscribes to flow key changes and the reconciler applies them.
        let watcher_shutdown = tokio_util::sync::CancellationToken::new();
        let runtime_cache = Arc::clone(&cache);
        // Only cache-backed flows have keys to watch.
        if let Some((system_cache_arc, cache_opts)) = &flow_cache {
            let prefix = cache_opts.prefix.clone();
            let (watch_tx, watch_rx) =
                tokio::sync::mpsc::channel::<flowgen_core::cache::WatchEvent>(256);

            let watcher_handle = crate::watcher::spawn(
                Arc::clone(system_cache_arc) as Arc<dyn flowgen_core::cache::Cache>,
                prefix.clone(),
                watch_tx,
                watcher_shutdown.clone(),
            );
            background_handles.push(watcher_handle);

            let reconciler_ctx = crate::reconciler::ReconcilerContext {
                cache: Arc::clone(system_cache_arc) as Arc<dyn flowgen_core::cache::Cache>,
                system_cache: Arc::clone(&executor_cache),
                runtime_cache: Arc::clone(&runtime_cache),
                app_config: Arc::clone(&app_config),
                resource_loader: resource_loader.clone(),
                http_server: http_server.clone(),
                mcp_server: mcp_server.clone(),
                ai_gateway_server: ai_gateway_server.clone(),
                filesystem_flow_paths: Arc::new(filesystem_flow_paths.clone()),
                flow_registry: Arc::clone(&flow_registry),
                client_registry: Arc::clone(&client_registry),
                holder_identity: holder_identity.clone(),
                peer_registry: Arc::clone(&peer_registry),
            };
            let reconciler_shutdown = watcher_shutdown.clone();
            let reconciler_handle = tokio::spawn(async move {
                crate::reconciler::run(watch_rx, reconciler_ctx, reconciler_shutdown).await;
            });
            background_handles.push(reconciler_handle);
        }

        // Wait for shutdown signal. In production, flows run indefinitely until shutdown.
        shutdown_rx.await.ok();

        info!("Shutdown signal received, stopping all flows...");

        // Stop the watcher and reconciler first so no new reload events arrive
        // while flows are being shut down.
        watcher_shutdown.cancel();

        // Cancel all running flows via their cancellation tokens, then abort server
        // handles (HTTP, MCP) which do not use tokens.
        if let Ok(registry) = flow_registry.read() {
            for handle in registry.values() {
                handle.cancellation_token.cancel();
            }
        }
        for handle in &background_handles {
            handle.abort();
        }

        // Await all server background tasks and all flow join handles.
        let _ = futures::future::join_all(background_handles).await;
        let flow_join_handles: Vec<tokio::task::JoinHandle<()>> = match flow_registry.write() {
            Ok(mut registry) => registry
                .drain()
                .map(|(_, handle)| handle.join_handle)
                .collect(),
            Err(_) => Vec::new(),
        };
        let _ = futures::future::join_all(flow_join_handles).await;

        // All flows have now fully stopped. Clean up leases to allow new pods to acquire leadership.
        // At this point it is safe to delete leases because no flows are processing events.
        for task_manager in task_managers {
            if let Err(e) = task_manager.shutdown().await {
                warn!("Failed to shutdown task manager: {}", e);
            }
        }

        // Remove this pod from the peer list so remaining pods stop hashing
        // flows onto it as a preferred owner.
        if let Err(e) = peer_registry.deregister().await {
            warn!("Failed to deregister peer: {}", e);
        }

        info!("Shutdown complete, all flows stopped and leases released");
        Ok(())
    }
}

/// Derives a flow's identity from its path relative to the flows root.
///
/// Both paths are compared as authored, not as resolved on disk: the caller
/// passes the un-canonicalized path so a symlinked mount (for example a
/// Kubernetes ConfigMap's `..data` indirection) does not leak into the
/// identity. `.` and redundant separators are normalized purely, so a
/// relative or `.`-laden root still strips cleanly. Returns `None` when the
/// file lies outside the root or either input is missing.
fn compute_source_path(
    file: &std::path::Path,
    source_root: Option<&std::path::Path>,
) -> Option<String> {
    use std::path::Component;

    // Normalize away `CurDir` (`.`) and separator noise without resolving
    // symlinks or `..`, so stripping is stable regardless of how the caller
    // spelled the path.
    let normalize = |p: &std::path::Path| -> std::path::PathBuf {
        p.components()
            .filter(|c| !matches!(c, Component::CurDir))
            .collect()
    };

    let root = normalize(source_root?);
    let rel = normalize(file);
    let rel = rel.strip_prefix(&root).ok()?;

    let s = rel.with_extension("").to_string_lossy().replace('\\', "/");
    match s.is_empty() {
        true => None,
        false => Some(s),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    #[test]
    fn compute_source_path_strips_root_and_extension() {
        let root = PathBuf::from("/etc/flows");
        let file = PathBuf::from("/etc/flows/demo/nba_email_demo.yaml");
        assert_eq!(
            compute_source_path(&file, Some(&root)),
            Some("demo/nba_email_demo".to_string())
        );
    }

    #[test]
    fn compute_source_path_flat_layout() {
        let root = PathBuf::from("/etc/flows");
        let file = PathBuf::from("/etc/flows/single.yml");
        assert_eq!(
            compute_source_path(&file, Some(&root)),
            Some("single".to_string())
        );
    }

    #[test]
    fn compute_source_path_returns_none_when_outside_root() {
        let root = PathBuf::from("/etc/flows");
        let file = PathBuf::from("/tmp/elsewhere/flow.yaml");
        assert!(compute_source_path(&file, Some(&root)).is_none());
    }

    #[test]
    fn compute_source_path_returns_none_without_root() {
        let file = PathBuf::from("/etc/flows/x.yaml");
        assert!(compute_source_path(&file, None).is_none());
    }

    #[test]
    fn compute_source_path_deeply_nested() {
        let root = PathBuf::from("/repo/flows");
        let file = PathBuf::from("/repo/flows/a/b/c/deep.json");
        assert_eq!(
            compute_source_path(&file, Some(&root)),
            Some("a/b/c/deep".to_string())
        );
    }

    #[test]
    fn compute_source_path_normalizes_dot_segments_in_relative_root() {
        let root = PathBuf::from("flows/./");
        let file = PathBuf::from("flows/demo/reader.yaml");
        assert_eq!(
            compute_source_path(&file, Some(&root)),
            Some("demo/reader".to_string())
        );
    }

    #[test]
    fn identity_is_stable_across_configmap_reloads() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("flows");
        std::fs::create_dir(&root).unwrap();

        let data_a = root.join("..2026_07_23_09_00_00.111111111");
        std::fs::create_dir(&data_a).unwrap();
        std::fs::write(data_a.join("sync_workspace.yaml"), "flow:\n  tasks: []\n").unwrap();
        symlink(&data_a, root.join("..data")).unwrap();
        symlink(
            "..data/sync_workspace.yaml",
            root.join("sync_workspace.yaml"),
        )
        .unwrap();

        let identity_before = derive_configmap_identity(&root);

        let data_b = root.join("..2026_07_24_18_30_00.222222222");
        std::fs::create_dir(&data_b).unwrap();
        std::fs::write(data_b.join("sync_workspace.yaml"), "flow:\n  tasks: []\n").unwrap();
        std::fs::remove_file(root.join("..data")).unwrap();
        symlink(&data_b, root.join("..data")).unwrap();

        let identity_after = derive_configmap_identity(&root);

        assert_eq!(identity_before, vec!["sync_workspace".to_string()]);
        assert_eq!(
            identity_before, identity_after,
            "identity must not change when the ConfigMap is reloaded"
        );

        let unfiltered: Vec<String> = glob::glob(&format!("{}/**/*.yaml", root.display()))
            .unwrap()
            .flatten()
            .filter_map(|p| compute_source_path(&p, Some(&root)))
            .collect();
        assert!(
            unfiltered
                .iter()
                .any(|id| id.contains("..2026_07_24_18_30_00.222222222")),
            "without the hidden-segment filter the timestamped path leaks into identity"
        );
    }

    fn derive_configmap_identity(root: &std::path::Path) -> Vec<String> {
        let pattern = format!("{}/**/*.yaml", root.display());
        let mut identities: Vec<String> = glob::glob(&pattern)
            .unwrap()
            .flatten()
            .filter(|path| match path.strip_prefix(root) {
                Ok(rel) => !rel.components().any(|c| {
                    matches!(c, std::path::Component::Normal(seg)
                            if seg.to_string_lossy().starts_with('.'))
                }),
                Err(_) => true,
            })
            .filter_map(|path| compute_source_path(&path, Some(root)))
            .collect();
        identities.sort();
        identities
    }
}
