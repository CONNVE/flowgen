//! Flow execution and task orchestration.
//!
//! This module manages the lifecycle of flows, including task registry creation,
//! channel wiring, leader election, and event propagation through task chains.
//!
//! ## Architecture
//!
//! The core pattern uses a `TaskRegistry` that builds a linear chain of tasks
//! connected by MPSC channels:
//!
//! ```text
//! [Task 0] --channel[0]--> [Task 1] --channel[1]--> [Task 2]
//! ```
//!
//! For N tasks, we create N-1 channels. Each task receives:
//! - `input_rx`: Receiver from previous channel (None for first task)
//! - `output_tx`: Sender to next channel (None for last task)
//!
//! ## Channel Ownership
//!
//! All tasks are spawned together using the SAME `TaskRegistry` to ensure
//! channels are properly connected. This is critical for webhook flows where
//! blocking tasks (webhooks) must send events to background tasks:
//!
//! 1. `start_tasks()` spawns ALL tasks (webhooks and background)
//! 2. Returns only blocking handles to wait for webhook registration
//! 3. Background tasks are already running with connected channels
//! 4. `monitor_tasks()` monitors the pre-spawned tasks
//!
//! This prevents channel mismatch bugs where webhooks send to one channel
//! while background tasks receive from a different channel.

use crate::config::{FlowConfig, TaskType};
use flowgen_core::{event::Event, task::runner::Runner};
use std::sync::Arc;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::{debug, error, info, warn, Instrument};

// Event buffer size for MPSC channels. This needs to be large enough to handle
// bursts from tasks like iterate that fan-out large arrays (e.g., 100k+ rows).
// Set to 10M (~1.2 GB memory) to provide ample headroom for high-volume processing.
// With MPSC, this buffer is distributed across N-1 channels for N tasks.
const DEFAULT_EVENT_BUFFER_SIZE: usize = 10_000_000;

/// Errors that can occur during flow execution.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    /// Error in convert processor task.
    #[error(transparent)]
    ConverProcessor(#[from] flowgen_core::task::convert::processor::Error),
    /// Error in iterate processor task.
    #[error(transparent)]
    IterateProcessor(#[from] flowgen_core::task::iterate::processor::Error),
    /// Error in log processor task.
    #[error(transparent)]
    LogProcessor(#[from] flowgen_core::task::log::processor::Error),
    /// Error in script processor task.
    #[error(transparent)]
    ScriptProcessor(#[from] flowgen_core::task::script::processor::Error),
    /// Error in buffer processor task.
    #[error(transparent)]
    BufferProcessor(#[from] flowgen_core::task::buffer::processor::Error),
    /// Error in Salesforce Pub/Sub API subscriber task.
    #[error(transparent)]
    SalesforcePubSubSubscriber(#[from] flowgen_salesforce::pubsubapi::subscriber::Error),
    /// Error in Salesforce Pub/Sub API publisher task.
    #[error(transparent)]
    SalesforcePubsubPublisher(#[from] flowgen_salesforce::pubsubapi::publisher::Error),
    /// Error in HTTP request processor task.
    #[error(transparent)]
    HttpRequestProcessor(#[from] flowgen_http::request::Error),
    /// Error in HTTP endpoint processor task.
    #[error(transparent)]
    HttpEndpointProcessor(#[from] flowgen_http::endpoint::Error),
    /// Error in HTML scrape processor task.
    #[error(transparent)]
    HtmlScrapeProcessor(#[from] flowgen_html::scrape::processor::Error),
    /// Error in HTTP server task.
    #[error(transparent)]
    HttpServer(#[from] flowgen_core::http_server::Error),
    /// Error in NATS JetStream publisher task.
    #[error(transparent)]
    NatsJetStreamPublisher(#[from] flowgen_nats::jetstream::publisher::Error),
    /// Error in NATS JetStream subscriber task.
    #[error(transparent)]
    NatsJetStreamSubscriber(#[from] flowgen_nats::jetstream::subscriber::Error),
    /// Error in object store read task.
    #[error(transparent)]
    ObjectStoreRead(#[from] flowgen_object_store::read::Error),
    /// Error in object store write task.
    #[error(transparent)]
    ObjectStoreWrite(#[from] flowgen_object_store::write::Error),
    /// Error in object store list task.
    #[error(transparent)]
    ObjectStoreList(#[from] flowgen_object_store::list::Error),
    /// Error in object store move task.
    #[error(transparent)]
    ObjectStoreMove(#[from] flowgen_object_store::r#move::Error),
    /// Error in generate subscriber task.
    #[error(transparent)]
    GenerateSubscriber(#[from] flowgen_core::task::generate::subscriber::Error),
    /// Error in cache operations.
    #[error(transparent)]
    Cache(#[from] flowgen_nats::cache::Error),
    /// Missing required builder attribute.
    #[error("Missing required builder attribute: {0}")]
    MissingBuilderAttribute(String),
    /// Task context not initialized (init must be called first).
    #[error("Task context not initialized: init() must be called first")]
    TaskContextNotInitialized,
    /// Task manager not initialized (init must be called first).
    #[error("Task manager not initialized: init() must be called first")]
    TaskManagerNotInitialized,
    /// Invalid lease configuration.
    #[error("Invalid lease configuration: {0}")]
    LeaseConfig(#[from] flowgen_core::executor::LeaseConfigError),
    /// Task manager error.
    #[error("Task manager error: {0}")]
    TaskManager(#[from] flowgen_core::task::manager::Error),
    /// Failed to register flow for leader election.
    #[error("Error registering flow for leader election: {0}")]
    LeaderElectionRegistrationFailed(String),
    /// Leadership channel closed unexpectedly.
    #[error("Leadership channel closed unexpectedly")]
    LeadershipChannelClosed,
    /// Error in Salesforce Bulk API query job operations.
    #[error(transparent)]
    SalesforceBulkApiQueryJob(#[from] flowgen_salesforce::bulkapi::query_job::Error),
    /// Error in Salesforce REST API SObject operations.
    #[error(transparent)]
    SalesforceRestApiSObject(#[from] flowgen_salesforce::restapi::sobject::Error),
    /// Error in Salesforce REST API Composite operations.
    #[error(transparent)]
    SalesforceRestApiComposite(#[from] flowgen_salesforce::restapi::composite::Error),
    /// Error in Salesforce REST API SOSL search operations.
    #[error(transparent)]
    SalesforceRestApiSearch(#[from] flowgen_salesforce::restapi::search::Error),
    /// Error in Salesforce SOAP API merge operations.
    #[error(transparent)]
    SalesforceSoapApiMerge(#[from] flowgen_salesforce::soapapi::merge::Error),
    /// Error in Salesforce Tooling API operations.
    #[error(transparent)]
    SalesforceTooling(#[from] flowgen_salesforce::toolingapi::processor::Error),
    /// Error in GCP BigQuery query task.
    #[error(transparent)]
    GcpBigQueryQuery(#[from] flowgen_gcp::bigquery::query::Error),
    /// Error in GCP BigQuery Storage Read task.
    #[error(transparent)]
    GcpBigQueryStorageRead(#[from] flowgen_gcp::bigquery::storage_read::Error),
    /// Error in GCP BigQuery unified job operations.
    #[error(transparent)]
    GcpBigQueryJob(#[from] flowgen_gcp::bigquery::job::Error),
    /// Error in GCP BigQuery Storage Write task.
    #[error(transparent)]
    GcpBigQueryStorageWrite(#[from] flowgen_gcp::bigquery::storage_write::Error),
    /// Error in Microsoft SQL Server query task.
    #[error(transparent)]
    MssqlQuery(#[from] flowgen_mssql::query::Error),
    /// Error in AI completion task.
    #[error(transparent)]
    AiCompletion(#[from] flowgen_ai_agent::completion::processor::Error),
    /// Error in MCP tool processor task.
    #[error(transparent)]
    McpToolProcessor(#[from] flowgen_mcp::processor::Error),
    /// Error in MCP resource processor task.
    #[error(transparent)]
    McpResourceProcessor(#[from] flowgen_mcp::resource::processor::Error),
    /// Error in MCP prompt processor task.
    #[error(transparent)]
    McpPromptProcessor(#[from] flowgen_mcp::prompt::processor::Error),
    /// Error in NATS KV store task.
    #[error(transparent)]
    NatsKvStore(#[from] flowgen_nats::jetstream::kv_store::Error),
    /// Error in AI gateway task.
    #[error(transparent)]
    LlmProxy(#[from] flowgen_ai_agent::ai_gateway::llm_proxy::Error),
    /// Error in Git sync task.
    #[error(transparent)]
    GitSync(#[from] flowgen_git::sync::processor::Error),
    /// Error in OCI sync task.
    #[error(transparent)]
    OciSync(#[from] flowgen_oci::sync::processor::Error),
    /// Error in Braze export user IDs task.
    #[error(transparent)]
    BrazeExportUsersIds(#[from] flowgen_braze::export::users::processor::Error),
    /// Error in MongoDB collection task.
    #[error(transparent)]
    MongoDbCollection(#[from] flowgen_mongodb::collection::Error),
    /// Error in MongoDB change stream task.
    #[error(transparent)]
    MongoDbChangeStream(#[from] flowgen_mongodb::change_stream::Error),
    /// Failed to store background task handles for later monitoring.
    #[error("Error storing background task handles")]
    BackgroundHandlesStoreFailed,
    /// Failed to store the task context after starting tasks.
    #[error("Error storing task context after start_tasks")]
    TaskContextStoreFailed,
    /// Failed to retrieve background task handles for monitoring.
    #[error("Error retrieving background task handles")]
    BackgroundHandlesRetrieveFailed,
    /// Flow cannot be initialized because HTTP server is not enabled.
    #[error("Flow cannot be initialized as http_server is not enabled")]
    HttpServerNotEnabled,
    /// Flow cannot be initialized because MCP server is not enabled.
    #[error("Flow cannot be initialized as mcp_server is not configured. Add worker.mcp_server to config or remove mcp_tool/mcp_resource/mcp_prompt tasks.")]
    McpServerNotEnabled,
    /// Flow cannot be initialized because AI gateway server is not enabled.
    #[error("Flow cannot be initialized as ai_gateway is not configured. Add worker.ai_gateway to config or remove llm_proxy tasks.")]
    AiGatewayServerNotEnabled,
    /// Flow configuration error (duplicate task names, invalid dependencies, etc.).
    #[error("Flow configuration error: {0}")]
    ConfigError(String),
}

/// Descriptor for a task with its channel endpoints.
#[derive(Debug)]
struct TaskDescriptor {
    /// Unique task identifier (from original task array index).
    id: usize,
    /// Task configuration and type.
    task_type: TaskType,
    /// Input channel receiver (None for source tasks).
    input_rx: Option<mpsc::Receiver<Event>>,
    /// Output channel sender (None for sink tasks).
    output_tx: Option<mpsc::Sender<Event>>,
    /// Whether this is a blocking setup task (e.g., webhook registration).
    is_blocking: bool,
    /// Number of leaf tasks reachable through this task's output channel.
    ///
    /// Source tasks use this to size the per-flow completion channel so the
    /// upstream message is acknowledged only after every leaf signals. Leaf
    /// tasks have a value of one (the task itself). The flow builder computes
    /// this from the wired topology.
    downstream_leaves: usize,
}

/// Central registry for managing all tasks in a flow.
#[derive(Debug)]
struct TaskRegistry {
    /// All tasks in execution order.
    tasks: Vec<TaskDescriptor>,
}

/// Type alias for a task join handle.
type TaskHandle = JoinHandle<Result<(), Error>>;

/// Handle collections separated by task type.
pub struct TaskHandles {
    /// Handles for blocking setup tasks (e.g., webhook route registration).
    pub blocking_handles: Vec<TaskHandle>,
    /// Handles for long-running background tasks.
    pub background_handles: Vec<TaskHandle>,
    /// Task context containing the cancellation token for this tenure.
    pub task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl TaskRegistry {
    /// Creates a new builder for constructing a task registry.
    fn builder(flow_config: Arc<FlowConfig>, buffer_size: usize) -> TaskRegistryBuilder {
        TaskRegistryBuilder {
            flow_config,
            buffer_size,
        }
    }

    /// Separates tasks into blocking (setup) and background tasks.
    fn partition(self) -> (Vec<TaskDescriptor>, Vec<TaskDescriptor>) {
        let mut blocking = Vec::new();
        let mut background = Vec::new();

        for task in self.tasks {
            if task.is_blocking {
                blocking.push(task);
            } else {
                background.push(task);
            }
        }

        (blocking, background)
    }
}

/// Builder for constructing a task registry with proper channel wiring.
#[derive(Debug)]
struct TaskRegistryBuilder {
    flow_config: Arc<FlowConfig>,
    buffer_size: usize,
}

impl TaskRegistryBuilder {
    /// Builds a task registry with channels wired between tasks.
    ///
    /// Dispatches to the DAG path when any task declares `depends_on`,
    /// otherwise uses the linear chain for backward compatibility.
    fn build(self) -> Result<TaskRegistry, Error> {
        let tasks_config = &self.flow_config.flow.tasks;

        if tasks_config.is_empty() {
            return Ok(TaskRegistry { tasks: Vec::new() });
        }

        // The DAG path also owns the implicit-parent walk that skips
        // registration-only tasks, so route through it whenever any task
        // opts out of pipeline I/O even if none declares `depends_on`.
        let needs_dag = tasks_config.iter().any(|t| t.depends_on().is_some())
            || tasks_config.iter().any(|t| !t.has_pipeline_io());
        if needs_dag {
            self.build_dag(tasks_config)
        } else {
            self.build_linear(tasks_config)
        }
    }

    /// Linear chain: task N receives from task N-1. No branching.
    fn build_linear(&self, tasks_config: &[TaskType]) -> Result<TaskRegistry, Error> {
        let task_count = tasks_config.len();
        let mut channels: Vec<(mpsc::Sender<Event>, mpsc::Receiver<Event>)> = (0..task_count
            .saturating_sub(1))
            .map(|_| mpsc::channel(self.buffer_size))
            .collect();

        let mut task_descriptors = Vec::with_capacity(task_count);

        for (idx, task_type) in tasks_config.iter().enumerate() {
            // Blocking setup tasks (webhooks, MCP registrations, LLM proxy).
            let is_blocking = matches!(
                task_type,
                TaskType::http_endpoint(_)
                    | TaskType::mcp_tool(_)
                    | TaskType::mcp_resource(_)
                    | TaskType::mcp_prompt(_)
                    | TaskType::llm_proxy(_)
            );

            let input_rx = if idx > 0 {
                channels
                    .get_mut(idx - 1)
                    .map(|(_, rx)| std::mem::replace(rx, mpsc::channel(1).1))
            } else {
                None
            };

            let output_tx = if idx < task_count - 1 {
                channels.get(idx).map(|(tx, _)| tx.clone())
            } else {
                None
            };

            task_descriptors.push(TaskDescriptor {
                id: idx,
                task_type: task_type.clone(),
                input_rx,
                output_tx,
                is_blocking,
                // Linear chains have a single terminal task, so every task
                // sees exactly one leaf in its downstream subgraph.
                downstream_leaves: 1,
            });
        }

        Ok(TaskRegistry {
            tasks: task_descriptors,
        })
    }

    /// DAG wiring: tasks declare their upstream via `depends_on`.
    ///
    /// Tasks without `depends_on` fall back to depending on the previous task
    /// in the list, so mixed flows (some tasks with `depends_on`, some without)
    /// work intuitively. Registration-only tasks (`has_pipeline_io() == false`)
    /// are skipped when resolving the implicit parent and never get an
    /// implicit parent themselves — they neither emit nor consume events, so
    /// wiring them into the DAG would either stall a downstream task on a
    /// channel that never sends or attach them to a channel they will never
    /// read. Fan-out points (multiple tasks depending on the same parent)
    /// use a dispatcher that clones events to each child channel.
    fn build_dag(&self, tasks_config: &[TaskType]) -> Result<TaskRegistry, Error> {
        let task_count = tasks_config.len();

        let mut name_to_idx: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (idx, task_type) in tasks_config.iter().enumerate() {
            let name = task_type.name().to_string();
            if name_to_idx.contains_key(&name) {
                return Err(Error::ConfigError(format!(
                    "Duplicate task name '{name}' in flow"
                )));
            }
            name_to_idx.insert(name, idx);
        }

        // Resolve each task's parent indices from `depends_on` names.
        // Registration-only tasks never get an implicit parent, and the
        // implicit-parent walk skips over any earlier registration-only task
        // so a normal task following one still chains to the last real
        // pipeline task.
        let mut parent_indices: Vec<Vec<usize>> = Vec::with_capacity(task_count);
        for (idx, task_type) in tasks_config.iter().enumerate() {
            let parents = match task_type.depends_on() {
                Some(deps) => {
                    let mut indices = Vec::with_capacity(deps.len());
                    for dep_name in deps {
                        let &parent_idx = name_to_idx.get(dep_name).ok_or_else(|| {
                            Error::ConfigError(format!(
                                "Task '{}' depends on unknown task '{dep_name}'",
                                task_type.name()
                            ))
                        })?;
                        if parent_idx >= idx {
                            return Err(Error::ConfigError(format!(
                                "Task '{}' depends on '{dep_name}' which appears later in the list",
                                task_type.name()
                            )));
                        }
                        indices.push(parent_idx);
                    }
                    indices
                }
                None if !task_type.has_pipeline_io() => vec![],
                None => (0..idx)
                    .rev()
                    .find(|&i| tasks_config[i].has_pipeline_io())
                    .map(|i| vec![i])
                    .unwrap_or_default(),
            };
            parent_indices.push(parents);
        }

        // Group children by parent to identify fan-out points.
        let mut children_per_parent: std::collections::HashMap<usize, Vec<usize>> =
            std::collections::HashMap::new();
        for (child_idx, parents) in parent_indices.iter().enumerate() {
            for &parent_idx in parents {
                children_per_parent
                    .entry(parent_idx)
                    .or_default()
                    .push(child_idx);
            }
        }

        // Each non-source task owns a single inbound mpsc channel.
        // Multiple parents can therefore feed the same child by sharing
        // clones of that channel's sender, which gives correct fan-in
        // semantics: every event from every parent reaches the child in the
        // order it was produced, with backpressure applied by the channel.
        let mut inbound_tx: Vec<Option<mpsc::Sender<Event>>> =
            (0..task_count).map(|_| None).collect();
        let mut child_rx: Vec<Option<mpsc::Receiver<Event>>> =
            (0..task_count).map(|_| None).collect();
        for (idx, parents) in parent_indices.iter().enumerate() {
            if !parents.is_empty() {
                let (tx, rx) = mpsc::channel(self.buffer_size);
                inbound_tx[idx] = Some(tx);
                child_rx[idx] = Some(rx);
            }
        }

        // Wire each parent's outbound sender to its children's inbound
        // channels. A parent with a single child sends directly into that
        // child's inbound channel. A parent with multiple children gets a
        // dispatcher that clones every event into each child's inbound
        // channel, preserving backpressure on each branch.
        let mut parent_tx: Vec<Option<mpsc::Sender<Event>>> =
            (0..task_count).map(|_| None).collect();

        for (&parent_idx, children) in &children_per_parent {
            if children.len() == 1 {
                let child_inbound = inbound_tx[children[0]].clone().ok_or_else(|| {
                    Error::ConfigError(format!(
                        "Internal error: missing inbound channel for task index {}",
                        children[0]
                    ))
                })?;
                parent_tx[parent_idx] = Some(child_inbound);
            } else {
                let (dispatcher_tx, dispatcher_rx) = mpsc::channel(self.buffer_size);
                parent_tx[parent_idx] = Some(dispatcher_tx);

                let mut child_senders = Vec::with_capacity(children.len());
                for &child_idx in children {
                    let child_inbound = inbound_tx[child_idx].clone().ok_or_else(|| {
                        Error::ConfigError(format!(
                            "Internal error: missing inbound channel for task index {child_idx}"
                        ))
                    })?;
                    child_senders.push(child_inbound);
                }

                tokio::spawn(async move {
                    let mut rx = dispatcher_rx;
                    while let Some(event) = rx.recv().await {
                        for tx in &child_senders {
                            if tx.send(event.clone()).await.is_err() {
                                break;
                            }
                        }
                    }
                });
            }
        }
        // Drop the sender side of inbound channels held only by the wiring
        // bookkeeping. Each child still holds clones via parent_tx, so the
        // channel stays open until every parent has finished.
        drop(inbound_tx);

        // Compute the number of distinct leaf tasks reachable from each task's
        // output. Tasks are kept in topological order (parents always precede
        // children) so a single reverse pass propagates leaf sets up the
        // directed acyclic graph. Summing child counts would double-count when
        // two branches re-merge (e.g. fan-out -> buffer fan-in), inflating the
        // source's completion expectation beyond the real number of leaves.
        let mut reachable_leaves: Vec<std::collections::HashSet<usize>> =
            vec![std::collections::HashSet::new(); task_count];
        for idx in (0..task_count).rev() {
            match children_per_parent.get(&idx) {
                None => {
                    reachable_leaves[idx].insert(idx);
                }
                Some(children) => {
                    let mut set = std::collections::HashSet::new();
                    for &c in children {
                        set.extend(reachable_leaves[c].iter().copied());
                    }
                    reachable_leaves[idx] = set;
                }
            }
        }
        let downstream_leaves: Vec<usize> = reachable_leaves.iter().map(|s| s.len()).collect();

        let mut task_descriptors = Vec::with_capacity(task_count);
        for (idx, task_type) in tasks_config.iter().enumerate() {
            let is_blocking = matches!(
                task_type,
                TaskType::http_endpoint(_)
                    | TaskType::mcp_tool(_)
                    | TaskType::mcp_resource(_)
                    | TaskType::mcp_prompt(_)
                    | TaskType::llm_proxy(_)
            );
            task_descriptors.push(TaskDescriptor {
                id: idx,
                task_type: task_type.clone(),
                input_rx: child_rx[idx].take(),
                output_tx: parent_tx[idx].take(),
                is_blocking,
                downstream_leaves: downstream_leaves[idx],
            });
        }

        Ok(TaskRegistry {
            tasks: task_descriptors,
        })
    }
}

pub struct Flow {
    /// The flow's static configuration, loaded from a file.
    pub config: Arc<FlowConfig>,
    /// An optional shared webhook HTTP server, passed in from the main application.
    http_server: Option<Arc<flowgen_http::server::EndpointServer>>,
    /// An optional shared MCP server, passed in from the main application.
    mcp_server: Option<Arc<flowgen_mcp::server::McpServer>>,
    /// An optional shared AI gateway server, passed in from the main application.
    ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>>,
    /// Shared runtime cache, passed in from the main application. Holds
    /// per-flow state (replay IDs, counters, last-run timestamps, business
    /// data) and is exposed to user scripts via `ctx.cache`.
    cache: Arc<dyn flowgen_core::cache::Cache>,
    /// System cache, passed in from the main application. Holds cluster-wide
    /// coordination state (lease keys for leader election). Kept separate
    /// from the runtime `cache` so user scripts cannot reach lease keys, and
    /// so retention policies can differ (short history plus per-message TTL
    /// for leases, longer retention for per-flow state). In single-binary
    /// or in-memory deployments this is the same `Arc` as `cache`; the
    /// separation is enforced only by the operator in the main config.
    system_cache: Arc<dyn flowgen_core::cache::Cache>,
    /// Event channel buffer size for this flow (from app config or DEFAULT).
    event_buffer_size: Option<usize>,
    /// Optional app-level retry configuration, passed in from the main application.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Optional resource loader for loading external files, passed in from the main application.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared client registry for deduplicating connections to external services.
    client_registry: Arc<flowgen_core::client_registry::ClientRegistry>,
    /// This pod's holder identity, resolved once at the app level so every
    /// flow's Executor agrees on it. `None` falls back to per-flow
    /// resolution (tests).
    holder_identity: Option<String>,
    /// Optional shared peer registry for flow distribution via consistent
    /// hashing, passed in from the main application. One instance per pod.
    peer_registry: Option<Arc<flowgen_core::peer::PeerRegistry>>,
    /// The task manager, responsible for leader election. Initialized by `init()`.
    task_manager: Option<Arc<flowgen_core::task::manager::TaskManager>>,
    /// Background task handles spawned by start_tasks for monitor_tasks to monitor.
    background_handles: Arc<std::sync::Mutex<Option<Vec<TaskHandle>>>>,
    /// Task context from the most recent start_tasks call; used to read registration
    /// info (registered_http_routes, registered_mcp_tools) before run() consumes self.
    last_task_context: Arc<std::sync::Mutex<Option<Arc<flowgen_core::task::context::TaskContext>>>>,
}

impl Flow {
    /// Returns the flow identity — the loader-assigned `source_path` when
    /// present (filesystem/cache load), otherwise the programmatic
    /// `flow.name`. Same value as `identity()`; kept as `name()` for
    /// existing call sites that predate the path-as-identity refactor.
    pub fn name(&self) -> &str {
        self.identity()
    }

    /// Returns the flow identity — used as the tracing `flow=` field, the
    /// registry key, and the cache namespace so activity keys line up with
    /// the admin API.
    pub fn identity(&self) -> &str {
        self.config.identity()
    }

    /// Returns a reference to the task manager if initialized.
    pub fn task_manager(&self) -> Option<Arc<flowgen_core::task::manager::TaskManager>> {
        self.task_manager.as_ref().map(Arc::clone)
    }

    /// Returns the cache backing this flow's leases and coordination state.
    pub fn system_cache(&self) -> Arc<dyn flowgen_core::cache::Cache> {
        Arc::clone(&self.system_cache)
    }

    /// Returns the cancellation token for the most recently started task tenure.
    ///
    /// Used by the reconciler to cancel a running flow before replacing it. Returns
    /// `None` if `start_tasks()` has not yet been called (e.g. leader-elected flows
    /// that have not yet acquired leadership).
    pub fn cancellation_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.last_task_context
            .lock()
            .ok()
            .and_then(|guard| guard.as_ref().map(|ctx| ctx.cancellation_token.clone()))
    }

    /// Determines if the flow should run with leader election.
    ///
    /// If a flow contains any webhook tasks, it will always be treated as
    /// non-leader-elected by disregarding the `required_leader_election` flag.
    fn is_leader_elected(&self) -> bool {
        let has_blocking_tasks = self.config.flow.tasks.iter().any(|task| {
            matches!(
                task,
                TaskType::http_endpoint(_)
                    | TaskType::mcp_tool(_)
                    | TaskType::mcp_resource(_)
                    | TaskType::mcp_prompt(_)
                    | TaskType::llm_proxy(_)
            )
        });

        if has_blocking_tasks {
            if self.config.flow.require_leader_election.unwrap_or(false) {
                info!(
                    "Flow {} contains a blocking task (webhook, MCP registration, or llm_proxy); `required_leader_election` flag will be ignored.",
                    self.identity()
                );
            }
            return false; // Flows with blocking tasks are never leader elected.
        }

        // Otherwise, respect the configuration.
        self.config.flow.require_leader_election.unwrap_or(false)
    }

    /// Initializes shared resources for the flow, such as the TaskManager.
    /// This must be called before any other run methods.
    pub async fn init(&mut self) -> Result<(), Error> {
        if self.task_manager.is_some() {
            return Ok(()); // Already initialized
        }

        // Validate: Flow with http_endpoint tasks requires HTTP server to be configured.
        let has_webhook_tasks = self
            .config
            .flow
            .tasks
            .iter()
            .any(|task| matches!(task, TaskType::http_endpoint(_) | TaskType::llm_proxy(_)));

        if has_webhook_tasks && self.http_server.is_none() {
            return Err(Error::HttpServerNotEnabled);
        }

        // Validate: any MCP registration task requires the MCP server.
        let has_mcp_tasks = self.config.flow.tasks.iter().any(|task| {
            matches!(
                task,
                TaskType::mcp_tool(_) | TaskType::mcp_resource(_) | TaskType::mcp_prompt(_)
            )
        });

        if has_mcp_tasks && self.mcp_server.is_none() {
            return Err(Error::McpServerNotEnabled);
        }

        // Create an Executor backed by the system cache. Lease keys must
        // stay isolated from user-script-accessible runtime cache to keep
        // coordination state outside any flow's reach.
        let lease_config = match &self.holder_identity {
            Some(holder_identity) => flowgen_core::executor::LeaseConfig {
                holder_identity: holder_identity.clone(),
                ..Default::default()
            },
            None => flowgen_core::executor::LeaseConfig::default(),
        };
        let executor =
            flowgen_core::executor::Executor::new(self.system_cache.clone(), lease_config)?;
        let executor = Arc::new(executor);

        let mut task_manager_builder =
            flowgen_core::task::manager::TaskManagerBuilder::new().executor(executor);
        if let Some(registry) = &self.peer_registry {
            task_manager_builder = task_manager_builder.peer_registry(Arc::clone(registry));
        }
        let task_manager = task_manager_builder.build()?;
        let task_manager = Arc::new(task_manager.start().await);

        self.task_manager = Some(task_manager);

        Ok(())
    }

    /// Creates a fresh TaskContext with a new cancellation token.
    /// This is called each time tasks are spawned (e.g., on leadership acquisition).
    fn create_task_context(&self) -> Result<Arc<flowgen_core::task::context::TaskContext>, Error> {
        let task_manager = self
            .task_manager
            .as_ref()
            .ok_or_else(|| Error::TaskManagerNotInitialized)?
            .clone();

        // Create a fresh cancellation token for this task tenure.
        let cancellation_token = tokio_util::sync::CancellationToken::new();

        // Shared response registry for streaming progress between tasks in this flow.
        let response_registry = Arc::new(flowgen_core::registry::ResponseRegistry::new());

        let mut task_context_builder = flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name(self.identity().to_string())
            .source_path(Some(self.identity().to_string()))
            .flow_labels(self.config.flow.labels.clone())
            .task_manager(task_manager)
            .cache(Arc::clone(&self.cache))
            .response_registry(response_registry)
            .resource_loader(self.resource_loader.clone())
            .cancellation_token(cancellation_token)
            .client_registry(Arc::clone(&self.client_registry));

        if let Some(retry_config) = &self.retry {
            task_context_builder = task_context_builder.retry(retry_config.clone());
        }

        Ok(Arc::new(task_context_builder.build().map_err(|e| {
            Error::MissingBuilderAttribute(e.to_string())
        })?))
    }

    /// Spawns all tasks and returns handles separated by type.
    ///
    /// This method spawns ALL tasks together (both blocking and background) using a
    /// single `TaskRegistry`. This ensures all tasks use the SAME channels for event
    /// propagation, which is critical for webhook flows.
    ///
    /// ## Why All Tasks Are Spawned Together
    ///
    /// For webhook flows, the webhook (blocking) task must send events to background
    /// tasks through a shared channel. If we spawn them separately with different
    /// registries, they get different channels and events are lost.
    ///
    /// ## Example Flow
    ///
    /// ```text
    /// TaskRegistry creates:
    ///   channel[0]: webhook -> script
    ///   channel[1]: script -> nats
    ///
    /// Spawns:
    ///   webhook (blocking, registers route, keeps running)
    ///   script (background, processes events)
    ///   nats (background, publishes to NATS)
    ///
    /// Returns:
    ///   blocking_handles: [webhook_handle]
    ///   background_handles: [script_handle, nats_handle]
    /// ```
    ///
    /// The caller should await `blocking_handles` before starting the HTTP server,
    /// but `background_handles` are already running and connected.
    pub async fn spawn_all_tasks(&self) -> Result<TaskHandles, Error> {
        // Create a fresh task context with a new cancellation token for this tenure.
        let task_context = self.create_task_context()?;

        // Build task registry with all tasks properly wired.
        let buffer_size = self.event_buffer_size.unwrap_or(DEFAULT_EVENT_BUFFER_SIZE);
        let registry = TaskRegistry::builder(self.config.clone(), buffer_size).build()?;

        // Separate blocking (setup) tasks from background tasks.
        let (blocking_tasks, background_tasks) = registry.partition();

        // Spawn all blocking tasks (webhooks).
        let mut blocking_handles = Vec::new();
        for task_desc in blocking_tasks {
            let handle = spawn_task(
                task_desc,
                task_context.clone(),
                self.http_server.clone(),
                self.mcp_server.clone(),
                self.ai_gateway_server.clone(),
            )
            .await?;
            blocking_handles.push(handle);
        }

        // Spawn all background tasks.
        let mut background_handles = Vec::new();
        for task_desc in background_tasks {
            let handle = spawn_task(
                task_desc,
                task_context.clone(),
                self.http_server.clone(),
                self.mcp_server.clone(),
                self.ai_gateway_server.clone(),
            )
            .await?;
            background_handles.push(handle);
        }

        Ok(TaskHandles {
            blocking_handles,
            background_handles,
            task_context,
        })
    }

    /// Spawns multiple independent flow pipelines for higher throughput.
    ///
    /// Creates N separate task registries, each with its own event channels.
    /// All instances share the same cancellation token for coordinated shutdown.
    async fn spawn_parallel_instances(&self, count: usize) -> Result<TaskHandles, Error> {
        let task_context = self.create_task_context()?;
        let buffer_size = self.event_buffer_size.unwrap_or(DEFAULT_EVENT_BUFFER_SIZE);

        let mut all_blocking_tasks = Vec::new();
        let mut all_background_tasks = Vec::new();

        // Build all task registries and collect task descriptors.
        for _ in 0..count {
            let registry = TaskRegistry::builder(self.config.clone(), buffer_size).build()?;
            let (blocking_tasks, background_tasks) = registry.partition();
            all_blocking_tasks.extend(blocking_tasks);
            all_background_tasks.extend(background_tasks);
        }

        // Spawn all blocking tasks.
        let mut blocking_handles = Vec::new();
        for task_desc in all_blocking_tasks {
            let handle = spawn_task(
                task_desc,
                task_context.clone(),
                self.http_server.clone(),
                self.mcp_server.clone(),
                self.ai_gateway_server.clone(),
            )
            .await?;
            blocking_handles.push(handle);
        }

        // Spawn all background tasks.
        let mut background_handles = Vec::new();
        for task_desc in all_background_tasks {
            let handle = spawn_task(
                task_desc,
                task_context.clone(),
                self.http_server.clone(),
                self.mcp_server.clone(),
                self.ai_gateway_server.clone(),
            )
            .await?;
            background_handles.push(handle);
        }

        Ok(TaskHandles {
            blocking_handles,
            background_handles,
            task_context,
        })
    }

    /// Starts all tasks for non-leader-elected flows immediately.
    ///
    /// Leader-elected flows return early — their tasks are spawned later by
    /// `monitor_tasks()` after acquiring leadership.
    ///
    /// Returns blocking handles (webhook registrations) that must complete
    /// before the HTTP server starts. Background handles are stored for
    /// `monitor_tasks()` to monitor.
    #[tracing::instrument(skip(self), name = "flow.run", fields(flow = %self.identity()))]
    pub async fn start_tasks(&self) -> Result<Vec<JoinHandle<Result<(), Error>>>, Error> {
        if self.is_leader_elected() {
            return Ok(Vec::new());
        }

        // Spawn all tasks and return only the blocking handles.
        let handles = self.spawn_all_tasks().await?;

        // Store background handles for monitor_tasks to monitor.
        let mut lock = self
            .background_handles
            .lock()
            .map_err(|_| Error::BackgroundHandlesStoreFailed)?;
        *lock = Some(handles.background_handles);

        // Store task context so registration info is accessible before run() consumes self.
        self.last_task_context
            .lock()
            .map_err(|_| Error::TaskContextStoreFailed)
            .map(|mut guard| *guard = Some(Arc::clone(&handles.task_context)))?;

        Ok(handles.blocking_handles)
    }

    /// Starts the main, long-running execution of the flow.
    ///
    /// This spawns a single master task that manages the flow's lifecycle,
    /// including leader election and running all background tasks.
    #[tracing::instrument(skip(self), name = "flow.run", fields(flow = %self.identity()))]
    pub fn run(self) -> JoinHandle<()> {
        let flow_id = self.identity().to_string();
        tokio::spawn(
            async move {
                if let Err(e) = self.monitor_tasks().await {
                    error!("Flow {} terminated with an error: {}", flow_id, e);
                }
            }
            .instrument(tracing::Span::current()),
        )
    }

    /// The main internal run loop for the flow.
    ///
    /// For leader-elected flows: waits for leadership, then spawns and monitors tasks.
    /// For non-leader-elected flows: monitors tasks already spawned by `start_tasks()`.
    async fn monitor_tasks(self) -> Result<(), Error> {
        let is_leader_elected = self.is_leader_elected();
        let task_manager = self
            .task_manager
            .as_ref()
            .ok_or_else(|| Error::TaskManagerNotInitialized)?
            .clone();
        // `register` takes the typed identity: it Displays as the human path in
        // logs and projects to the key-safe form for the lease and peer keys.
        let flow_id = self.identity().to_string();
        let lease_id = self.config.identity_ref().clone();

        let leader_election_options = if is_leader_elected {
            Some(flowgen_core::task::manager::LeaderElectionOptions {})
        } else {
            None
        };

        let mut leadership_rx = task_manager
            .register(lease_id, leader_election_options)
            .await
            .map_err(|e| Error::LeaderElectionRegistrationFailed(e.to_string()))?;

        let mut tasks_spawned = false;

        // Wait for initial leadership state.
        loop {
            match leadership_rx.recv().await {
                Some(flowgen_core::task::manager::LeaderElectionResult::Leader) => {
                    if !tasks_spawned {
                        info!("Acquired leadership, spawning tasks");
                        tasks_spawned = true;

                        // Drain duplicate Leader messages.
                        while let Ok(flowgen_core::task::manager::LeaderElectionResult::Leader) =
                            leadership_rx.try_recv()
                        {
                            debug!("Flow {} received duplicate Leader message during initial drain, ignoring", flow_id);
                        }
                        break;
                    } else {
                        debug!(
                            "Flow {} received duplicate Leader message, ignoring",
                            flow_id
                        );
                    }
                }
                Some(flowgen_core::task::manager::LeaderElectionResult::NotLeader) => {
                    debug!("Flow {} is not leader, waiting for leadership", flow_id);
                    tasks_spawned = false;
                }
                Some(flowgen_core::task::manager::LeaderElectionResult::NoElection) => {
                    debug!(
                        "No leader election for flow {}, spawning tasks immediately",
                        flow_id
                    );
                    tasks_spawned = true;
                    break;
                }
                None => {
                    info!("Leadership channel closed, exiting");
                    return Ok(());
                }
            }
        }

        let mut current_task_handles = if !is_leader_elected {
            // Retrieve already-spawned background tasks.
            let mut lock = self
                .background_handles
                .lock()
                .map_err(|_| Error::BackgroundHandlesRetrieveFailed)?;

            // For non-leader-elected flows, we just need the raw handles
            let handles = lock
                .take()
                .ok_or_else(|| Error::BackgroundHandlesRetrieveFailed)?;

            // Create a dummy TaskHandles since non-leader flows don't use cancellation
            TaskHandles {
                blocking_handles: Vec::new(),
                background_handles: handles,
                task_context: self.create_task_context()?, // Won't be used but needed for the type
            }
        } else {
            // Spawn tasks for this leadership tenure.
            let parallel_instances = self.config.flow.parallel_instances;

            if parallel_instances <= 1 {
                self.spawn_all_tasks().await?
            } else {
                info!(
                    "Spawning {} parallel instances for flow: {}",
                    parallel_instances,
                    self.identity()
                );
                self.spawn_parallel_instances(parallel_instances).await?
            }
        };

        let mut background_tasks: Vec<TaskHandle> = current_task_handles
            .blocking_handles
            .into_iter()
            .chain(current_task_handles.background_handles)
            .collect();

        if background_tasks.is_empty() {
            info!("No tasks to monitor");
            return Ok(());
        }

        if is_leader_elected {
            // Monitor leadership and tasks.
            loop {
                let all_completed = loop {
                    tokio::select! {
                        biased;
                        Some(status) = leadership_rx.recv() => {
                            match status {
                                flowgen_core::task::manager::LeaderElectionResult::NotLeader => {
                                    debug!("Lost leadership, signaling cancellation and aborting tasks");

                                    // Signal all tasks to stop via cancellation token
                                    current_task_handles.task_context.cancellation_token.cancel();

                                    // Also abort tasks immediately
                                    for task in &background_tasks {
                                        task.abort();
                                    }
                                    break false;
                                }
                                flowgen_core::task::manager::LeaderElectionResult::Leader => {
                                    if tasks_spawned {
                                        debug!("Flow {} received duplicate Leader message while tasks running, ignoring", flow_id);
                                    } else {
                                        warn!("Received unexpected Leader message, tasks already running");
                                    }
                                }
                                flowgen_core::task::manager::LeaderElectionResult::NoElection => {}
                            }
                        }
                        results = futures::future::join_all(&mut background_tasks), if !background_tasks.is_empty() => {
                            for (idx, result) in results.iter().enumerate() {
                                if let Err(e) = result {
                                    error!("Task {} failed: {}", idx, e);
                                }
                            }
                            info!("All tasks completed");
                            break true;
                        }
                    }
                };

                if all_completed {
                    break;
                }

                // Lost leadership - wait to re-acquire.
                background_tasks.clear();
                tasks_spawned = false;

                loop {
                    match leadership_rx.recv().await {
                        Some(flowgen_core::task::manager::LeaderElectionResult::Leader) => {
                            if !tasks_spawned {
                                info!("Re-acquired leadership, spawning tasks");
                                tasks_spawned = true;

                                // Drain duplicate Leader messages.
                                while let Ok(
                                    flowgen_core::task::manager::LeaderElectionResult::Leader,
                                ) = leadership_rx.try_recv()
                                {
                                    debug!("Flow {} received duplicate Leader message during re-acquisition drain, ignoring", flow_id);
                                }
                                break;
                            } else {
                                debug!("Flow {} received duplicate Leader message during re-acquisition, ignoring", flow_id);
                            }
                        }
                        Some(flowgen_core::task::manager::LeaderElectionResult::NotLeader) => {
                            debug!("Flow {} still not leader, waiting", flow_id);
                            tasks_spawned = false;
                        }
                        Some(flowgen_core::task::manager::LeaderElectionResult::NoElection) => {
                            tasks_spawned = true;
                            break;
                        }
                        None => {
                            info!("Leadership channel closed, exiting");
                            return Ok(());
                        }
                    }
                }

                // Re-spawn tasks after re-acquiring leadership.
                current_task_handles = self.spawn_all_tasks().await?;
                background_tasks = std::mem::take(&mut current_task_handles.blocking_handles)
                    .into_iter()
                    .chain(std::mem::take(&mut current_task_handles.background_handles))
                    .collect();
            }
        } else {
            // Wait for all tasks to complete.
            let results = futures::future::join_all(background_tasks).await;

            for (idx, result) in results.iter().enumerate() {
                if let Err(e) = result {
                    error!("Task {} failed: {}", idx, e);
                }
            }
            info!("All tasks completed");
        }
        Ok(())
    }
}

/// Spawns a single task based on its descriptor with proper channel wiring.
///
/// Returns a JoinHandle for the spawned task.
async fn spawn_task(
    task_desc: TaskDescriptor,
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    http_server: Option<Arc<flowgen_http::server::EndpointServer>>,
    mcp_server: Option<Arc<flowgen_mcp::server::McpServer>>,
    ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>>,
) -> Result<JoinHandle<Result<(), Error>>, Error> {
    let task_id = task_desc.id;
    let rx = task_desc.input_rx;
    let tx = task_desc.output_tx;
    let task_type_str = task_desc.task_type.as_str();
    let span = tracing::Span::current();

    // Each task receives a context whose `leaf_count` reflects the leaves
    // reachable from this specific task's output. Source and intermediate
    // tasks (such as iterate) read this value when constructing per-flow or
    // per-element completion channels.
    let task_context = {
        let mut ctx = (*task_context).clone();
        ctx.leaf_count = task_desc.downstream_leaves;
        // Stagger init for background tasks to avoid thundering-herd on
        // external services. Blocking tasks (webhooks) and generate (cron)
        // must start immediately.
        if !task_desc.is_blocking && !matches!(task_desc.task_type, TaskType::generate(_)) {
            let retry_config = flowgen_core::retry::RetryConfig::merge(&ctx.retry, &None);
            ctx.startup_delay = Some(retry_config.startup_jitter());
        }
        Arc::new(ctx)
    };

    let handle = match task_desc.task_type {
        TaskType::convert(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::convert::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::iterate(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::iterate::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::log(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_core::task::log::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::buffer(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::buffer::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::script(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::script::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::generate(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_core::task::generate::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::http_request(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_http::request::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::http_endpoint(config) => {
            let config = Arc::new(config);
            let http_server = http_server.clone().ok_or(Error::HttpServerNotEnabled)?;
            tokio::spawn(
                async move {
                    let mut builder = flowgen_http::endpoint::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context)
                        .http_server(http_server);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::html_scrape(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_html::scrape::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::nats_jetstream_subscriber(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_nats::jetstream::subscriber::SubscriberBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::nats_jetstream_publisher(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_nats::jetstream::publisher::PublisherBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_pubsubapi_subscriber(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::pubsubapi::subscriber::SubscriberBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_pubsubapi_publisher(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::pubsubapi::publisher::PublisherBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_bulkapi_query_job(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::bulkapi::query_job::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_restapi_sobject(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_salesforce::restapi::sobject::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_restapi_composite(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::restapi::composite::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_restapi_search(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_salesforce::restapi::search::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_soapapi_merge(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_salesforce::soapapi::merge::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::salesforce_toolingapi(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_salesforce::toolingapi::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::object_store(config) => {
            let operation = config.operation.clone();
            let config = Arc::new(config);
            match operation {
                flowgen_object_store::config::Operation::Read => tokio::spawn(
                    async move {
                        let mut builder = flowgen_object_store::read::ReadProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                        if let Some(rx) = rx {
                            builder = builder.receiver(rx);
                        }
                        if let Some(tx) = tx {
                            builder = builder.sender(tx);
                        }
                        builder.build().await?.run().await?;
                        Ok(())
                    }
                    .instrument(span),
                ),
                flowgen_object_store::config::Operation::Write => tokio::spawn(
                    async move {
                        let mut builder = flowgen_object_store::write::WriteProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                        if let Some(rx) = rx {
                            builder = builder.receiver(rx);
                        }
                        if let Some(tx) = tx {
                            builder = builder.sender(tx);
                        }
                        builder.build().await?.run().await?;
                        Ok(())
                    }
                    .instrument(span),
                ),
                flowgen_object_store::config::Operation::List => tokio::spawn(
                    async move {
                        let mut builder = flowgen_object_store::list::ListProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                        if let Some(rx) = rx {
                            builder = builder.receiver(rx);
                        }
                        if let Some(tx) = tx {
                            builder = builder.sender(tx);
                        }
                        builder.build().await?.run().await?;
                        Ok(())
                    }
                    .instrument(span),
                ),
                flowgen_object_store::config::Operation::Move => tokio::spawn(
                    async move {
                        let mut builder = flowgen_object_store::r#move::MoveProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                        if let Some(rx) = rx {
                            builder = builder.receiver(rx);
                        }
                        if let Some(tx) = tx {
                            builder = builder.sender(tx);
                        }
                        builder.build().await?.run().await?;
                        Ok(())
                    }
                    .instrument(span),
                ),
            }
        }
        TaskType::gcp_bigquery_query(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::query::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_storage_read(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::storage_read::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_job(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::job::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::gcp_bigquery_storage_write(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_gcp::bigquery::storage_write::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::mssql_query(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_mssql::query::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::nats_kv_store(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_nats::jetstream::kv_store::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::ai_completion(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_ai_agent::completion::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::mcp_tool(config) => {
            let config = Arc::new(config);
            let mcp_server = mcp_server.clone().ok_or(Error::McpServerNotEnabled)?;
            tokio::spawn(
                async move {
                    let mut builder = flowgen_mcp::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context)
                        .mcp_server(mcp_server);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::mcp_resource(config) => {
            let config = Arc::new(config);
            let mcp_server = mcp_server.clone().ok_or(Error::McpServerNotEnabled)?;
            // mcp_resource emits no events; drop the pipeline sender.
            drop(tx);
            tokio::spawn(
                async move {
                    let builder = flowgen_mcp::resource::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context)
                        .mcp_server(mcp_server);
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::mcp_prompt(config) => {
            let config = Arc::new(config);
            let mcp_server = mcp_server.clone().ok_or(Error::McpServerNotEnabled)?;
            // mcp_prompt emits no events; drop the pipeline sender.
            drop(tx);
            tokio::spawn(
                async move {
                    let builder = flowgen_mcp::prompt::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context)
                        .mcp_server(mcp_server);
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::llm_proxy(config) => {
            let config = Arc::new(config);
            let ai_gateway_server = ai_gateway_server
                .clone()
                .ok_or(Error::AiGatewayServerNotEnabled)?;
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_ai_agent::ai_gateway::llm_proxy::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context)
                            .ai_gateway_server(ai_gateway_server);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::git_sync(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_git::sync::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::oci_sync(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_oci::sync::processor::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
        TaskType::braze_export_users_ids(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_braze::export::users::processor::ProcessorBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }

        TaskType::mongodb_collection(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder = flowgen_mongodb::collection::ProcessorBuilder::new()
                        .config(config)
                        .task_id(task_id)
                        .task_type(task_type_str)
                        .task_context(task_context);
                    if let Some(rx) = rx {
                        builder = builder.receiver(rx);
                    }
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build()?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }

        TaskType::mongodb_change_stream(config) => {
            let config = Arc::new(config);
            tokio::spawn(
                async move {
                    let mut builder =
                        flowgen_mongodb::change_stream::ChangeStreamReaderBuilder::new()
                            .config(config)
                            .task_id(task_id)
                            .task_type(task_type_str)
                            .task_context(task_context);
                    if let Some(tx) = tx {
                        builder = builder.sender(tx);
                    }
                    builder.build().await?.run().await?;
                    Ok(())
                }
                .instrument(span),
            )
        }
    };

    Ok(handle)
}

/// Builder for creating Flow instances.
#[derive(Default)]
pub struct FlowBuilder {
    /// Optional flow configuration.
    config: Option<Arc<FlowConfig>>,
    /// Optional shared webhook HTTP server.
    http_server: Option<Arc<flowgen_http::server::EndpointServer>>,
    /// Optional shared MCP server.
    mcp_server: Option<Arc<flowgen_mcp::server::McpServer>>,
    /// Optional shared AI gateway server.
    ai_gateway_server: Option<Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>>,
    /// Shared runtime cache instance.
    cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// System cache instance. Backs the executor's lease store and is kept
    /// out of user-script reach. Required at build time.
    system_cache: Option<Arc<dyn flowgen_core::cache::Cache>>,
    /// Optional event channel buffer size.
    event_buffer_size: Option<usize>,
    /// Optional app-level retry configuration.
    retry: Option<flowgen_core::retry::RetryConfig>,
    /// Resource loader for loading external files.
    resource_loader: Option<flowgen_core::resource::ResourceLoader>,
    /// Shared client registry for connection pooling.
    client_registry: Option<Arc<flowgen_core::client_registry::ClientRegistry>>,
    /// This pod's holder identity, shared across every flow's Executor.
    holder_identity: Option<String>,
    /// Shared peer registry for flow distribution via consistent hashing.
    peer_registry: Option<Arc<flowgen_core::peer::PeerRegistry>>,
}

impl FlowBuilder {
    /// Creates a new FlowBuilder with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the flow configuration.
    pub fn config(mut self, config: Arc<FlowConfig>) -> Self {
        self.config = Some(config);
        self
    }

    /// Sets the shared webhook HTTP server.
    pub fn http_server(mut self, server: Arc<flowgen_http::server::EndpointServer>) -> Self {
        self.http_server = Some(server);
        self
    }

    /// Sets the shared MCP server.
    pub fn mcp_server(mut self, server: Arc<flowgen_mcp::server::McpServer>) -> Self {
        self.mcp_server = Some(server);
        self
    }

    /// Sets the shared AI gateway server.
    pub fn ai_gateway_server(
        mut self,
        server: Arc<flowgen_ai_agent::ai_gateway::server::AiGatewayServer>,
    ) -> Self {
        self.ai_gateway_server = Some(server);
        self
    }

    /// Sets the shared runtime cache instance.
    pub fn cache(mut self, cache: Arc<dyn flowgen_core::cache::Cache>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Sets the system cache instance used for leader-election leases.
    ///
    /// In multi-bucket deployments this points at a separate bucket from
    /// the runtime cache; in single-binary or in-memory deployments it may
    /// share the same underlying `Arc`.
    pub fn system_cache(mut self, cache: Arc<dyn flowgen_core::cache::Cache>) -> Self {
        self.system_cache = Some(cache);
        self
    }

    /// Sets the event channel buffer size.
    pub fn event_buffer_size(mut self, size: usize) -> Self {
        self.event_buffer_size = Some(size);
        self
    }

    /// Sets the app-level retry configuration.
    pub fn retry(mut self, retry: flowgen_core::retry::RetryConfig) -> Self {
        self.retry = Some(retry);
        self
    }

    /// Sets the resource loader for loading external files.
    pub fn resource_loader(
        mut self,
        resource_loader: flowgen_core::resource::ResourceLoader,
    ) -> Self {
        self.resource_loader = Some(resource_loader);
        self
    }

    /// Sets the shared client registry for connection pooling across tasks.
    pub fn client_registry(
        mut self,
        client_registry: Arc<flowgen_core::client_registry::ClientRegistry>,
    ) -> Self {
        self.client_registry = Some(client_registry);
        self
    }

    /// Sets this pod's holder identity, shared across every flow's Executor.
    pub fn holder_identity(mut self, holder_identity: String) -> Self {
        self.holder_identity = Some(holder_identity);
        self
    }

    /// Sets the shared peer registry for flow distribution via consistent hashing.
    pub fn peer_registry(mut self, peer_registry: Arc<flowgen_core::peer::PeerRegistry>) -> Self {
        self.peer_registry = Some(peer_registry);
        self
    }

    /// Builds a Flow instance from the configured options.
    ///
    /// # Errors
    /// Returns `Error::MissingBuilderAttribute` if required fields are not set.
    pub fn build(self) -> Result<Flow, Error> {
        Ok(Flow {
            config: self
                .config
                .ok_or_else(|| Error::MissingBuilderAttribute("config".to_string()))?,
            http_server: self.http_server,
            mcp_server: self.mcp_server,
            ai_gateway_server: self.ai_gateway_server,
            cache: self
                .cache
                .ok_or_else(|| Error::MissingBuilderAttribute("cache".to_string()))?,
            system_cache: self
                .system_cache
                .ok_or_else(|| Error::MissingBuilderAttribute("system_cache".to_string()))?,
            event_buffer_size: self.event_buffer_size,
            retry: self.retry,
            resource_loader: self.resource_loader,
            client_registry: match self.client_registry {
                Some(registry) => registry,
                None => Arc::new(flowgen_core::client_registry::ClientRegistry::new()),
            },
            holder_identity: self.holder_identity,
            peer_registry: self.peer_registry,
            task_manager: None,
            background_handles: Arc::new(std::sync::Mutex::new(None)),
            last_task_context: Arc::new(std::sync::Mutex::new(None)),
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Flow, FlowConfig, FlowConfigRaw};

    #[test]
    fn test_flow_builder_new() {
        let builder = FlowBuilder::new();
        assert!(builder.config.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_default() {
        let builder = FlowBuilder::default();
        assert!(builder.config.is_none());
        assert!(builder.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_config() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let builder = FlowBuilder::new().config(flow_config.clone());
        assert_eq!(builder.config, Some(flow_config));
    }

    #[test]
    fn test_flow_builder_http_server() {
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));
        let builder = FlowBuilder::new().http_server(server.clone());
        assert!(builder.http_server.is_some());
    }

    #[test]
    fn test_flow_builder_build_missing_config() {
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));

        let result = FlowBuilder::new().http_server(server).build();

        assert!(matches!(
            result,
            Err(Error::MissingBuilderAttribute(attr)) if attr == "config"
        ));
    }

    #[test]
    fn test_flow_builder_build_without_http_server() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;

        let result = FlowBuilder::new()
            .config(flow_config)
            .cache(cache.clone())
            .system_cache(cache)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert!(flow.http_server.is_none());
    }

    #[test]
    fn test_flow_builder_build_success() {
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("success_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "success_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );
        let server = Arc::new(flowgen_core::http_server::HttpServer::<
            flowgen_http::server::EndpointDispatcher,
        >::new(
            flowgen_http::server::DEFAULT_ENDPOINT_PATH.to_string()
        ));
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;

        let result = FlowBuilder::new()
            .config(flow_config.clone())
            .http_server(server)
            .cache(cache.clone())
            .system_cache(cache)
            .build();

        assert!(result.is_ok());
        let flow = result.unwrap();
        assert_eq!(flow.config, flow_config);
        assert!(flow.task_manager.is_none());
    }

    #[test]
    fn test_task_registry_creates_n_minus_1_channels() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 3);

        let task0_has_input = registry.tasks[0].input_rx.is_some();
        let task0_has_output = registry.tasks[0].output_tx.is_some();

        let task1_has_input = registry.tasks[1].input_rx.is_some();
        let task1_has_output = registry.tasks[1].output_tx.is_some();

        let task2_has_input = registry.tasks[2].input_rx.is_some();
        let task2_has_output = registry.tasks[2].output_tx.is_some();

        assert!(!task0_has_input, "First task should not have input");
        assert!(task0_has_output, "First task should have output");

        assert!(task1_has_input, "Middle task should have input");
        assert!(task1_has_output, "Middle task should have output");

        assert!(task2_has_input, "Last task should have input");
        assert!(!task2_has_output, "Last task should not have output");
    }

    #[test]
    fn test_task_registry_single_task() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![TaskType::script(
                            flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            },
                        )],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 1);

        let task = &registry.tasks[0];
        assert!(task.input_rx.is_none(), "Single task should not have input");
        assert!(
            task.output_tx.is_none(),
            "Single task should not have output"
        );
    }

    #[test]
    fn test_task_registry_partition_blocking_vs_background() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::http_endpoint(flowgen_http::config::Processor {
                                name: "h".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();
        let (blocking, background) = registry.partition();

        assert_eq!(blocking.len(), 1, "Should have 1 blocking task (webhook)");
        assert_eq!(
            background.len(),
            2,
            "Should have 2 background tasks (scripts)"
        );

        assert!(
            blocking[0].is_blocking,
            "Blocking task should have is_blocking=true"
        );
        assert!(
            !background[0].is_blocking,
            "Background task should have is_blocking=false"
        );
        assert!(
            !background[1].is_blocking,
            "Background task should have is_blocking=false"
        );
    }

    #[test]
    fn test_task_registry_empty_flow() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks.len(), 0);
    }

    #[test]
    fn test_task_registry_preserves_task_order() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("test_flow".to_string()),
                        labels: None,
                        tasks: vec![
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                            TaskType::script(flowgen_core::task::script::config::Processor {
                                name: "s".to_string(),
                                ..Default::default()
                            }),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "test_flow".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        assert_eq!(registry.tasks[0].id, 0);
        assert_eq!(registry.tasks[1].id, 1);
        assert_eq!(registry.tasks[2].id, 2);
    }

    #[tokio::test]
    async fn test_downstream_leaves_fan_out_fan_in() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("fan_out_fan_in".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("trigger", None),
                            script_with_deps("read_a", Some(vec!["trigger"])),
                            script_with_deps("convert_a", Some(vec!["read_a"])),
                            script_with_deps("read_b", Some(vec!["trigger"])),
                            script_with_deps("convert_b", Some(vec!["read_b"])),
                            script_with_deps("buffer", Some(vec!["convert_a", "convert_b"])),
                            script_with_deps("join", Some(vec!["buffer"])),
                            script_with_deps("write", Some(vec!["join"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "fan_out_fan_in".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("write"), 1, "write is the only leaf");
        assert_eq!(leaves("join"), 1);
        assert_eq!(leaves("buffer"), 1);
        assert_eq!(leaves("convert_a"), 1);
        assert_eq!(leaves("convert_b"), 1);
        assert_eq!(leaves("read_a"), 1);
        assert_eq!(leaves("read_b"), 1);
        assert_eq!(
            leaves("trigger"),
            1,
            "trigger sees one terminal leaf, not two — both branches re-merge at buffer"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_true_fan_out() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("fan_out".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("source", None),
                            script_with_deps("leaf_a", Some(vec!["source"])),
                            script_with_deps("leaf_b", Some(vec!["source"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "fan_out".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("leaf_a"), 1);
        assert_eq!(leaves("leaf_b"), 1);
        assert_eq!(leaves("source"), 2, "two distinct downstream leaves");
    }

    #[tokio::test]
    async fn test_downstream_leaves_nested_diamonds() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("nested_diamonds".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("src", None),
                            script_with_deps("a1", Some(vec!["src"])),
                            script_with_deps("a2", Some(vec!["src"])),
                            script_with_deps("mid", Some(vec!["a1", "a2"])),
                            script_with_deps("b1", Some(vec!["mid"])),
                            script_with_deps("b2", Some(vec!["mid"])),
                            script_with_deps("sink", Some(vec!["b1", "b2"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "nested_diamonds".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("sink"), 1);
        assert_eq!(leaves("b1"), 1);
        assert_eq!(leaves("b2"), 1);
        assert_eq!(leaves("mid"), 1);
        assert_eq!(leaves("a1"), 1);
        assert_eq!(leaves("a2"), 1);
        assert_eq!(
            leaves("src"),
            1,
            "nested diamonds re-merge — naive sum would be 4"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_mixed_fan_out_and_re_merge() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("mixed".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("src", None),
                            script_with_deps("a", Some(vec!["src"])),
                            script_with_deps("b", Some(vec!["src"])),
                            script_with_deps("c", Some(vec!["src"])),
                            script_with_deps("merge", Some(vec!["a", "b"])),
                            script_with_deps("leaf_x", Some(vec!["merge"])),
                            script_with_deps("leaf_y", Some(vec!["c"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "mixed".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        let leaves = |name: &str| {
            registry
                .tasks
                .iter()
                .find(|t| t.task_type.name() == name)
                .expect("task missing")
                .downstream_leaves
        };

        assert_eq!(leaves("leaf_x"), 1);
        assert_eq!(leaves("leaf_y"), 1);
        assert_eq!(leaves("merge"), 1);
        assert_eq!(leaves("a"), 1);
        assert_eq!(leaves("b"), 1);
        assert_eq!(leaves("c"), 1);
        assert_eq!(
            leaves("src"),
            2,
            "merged branch counts once, independent branch adds the second leaf"
        );
    }

    #[tokio::test]
    async fn test_downstream_leaves_linear_chain() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script_with_deps = |name: &str, deps: Option<Vec<&str>>| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: deps.map(|v| v.into_iter().map(String::from).collect()),
                ..Default::default()
            })
        };

        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("linear".to_string()),
                        labels: None,
                        tasks: vec![
                            script_with_deps("t0", None),
                            script_with_deps("t1", Some(vec!["t0"])),
                            script_with_deps("t2", Some(vec!["t1"])),
                            script_with_deps("t3", Some(vec!["t2"])),
                        ],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "linear".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        for task in &registry.tasks {
            assert_eq!(
                task.downstream_leaves,
                1,
                "{} should see 1 leaf",
                task.task_type.name()
            );
        }
    }

    #[tokio::test]
    async fn test_registration_only_tasks_get_no_implicit_parent() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let mcp_prompt = |name: &str| {
            TaskType::mcp_prompt(flowgen_mcp::prompt::config::Processor {
                name: name.to_string(),
                description: format!("desc for {name}"),
                arguments: Vec::new(),
                template: Some(flowgen_core::resource::Source::Inline("body".to_string())),
                messages: None,
                headers: std::collections::HashMap::new(),
                depends_on: None,
                retry: None,
            })
        };

        // Two mcp_prompts back-to-back without any `depends_on:` — both
        // should register as independent sources, neither should inherit
        // the other as a parent.
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("registration_only".to_string()),
                        labels: None,
                        tasks: vec![mcp_prompt("a"), mcp_prompt("b")],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "registration_only".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        for task in &registry.tasks {
            assert!(
                task.input_rx.is_none(),
                "{} must have no inbound channel",
                task.task_type.name()
            );
            assert!(
                task.output_tx.is_none(),
                "{} must have no outbound channel",
                task.task_type.name()
            );
        }
    }

    #[tokio::test]
    async fn test_implicit_parent_skips_registration_only() {
        use crate::config::{Flow, FlowConfig, FlowConfigRaw, TaskType};

        let script = |name: &str| {
            TaskType::script(flowgen_core::task::script::config::Processor {
                name: name.to_string(),
                depends_on: None,
                ..Default::default()
            })
        };
        let mcp_prompt = |name: &str| {
            TaskType::mcp_prompt(flowgen_mcp::prompt::config::Processor {
                name: name.to_string(),
                description: format!("desc for {name}"),
                arguments: Vec::new(),
                template: Some(flowgen_core::resource::Source::Inline("body".to_string())),
                messages: None,
                headers: std::collections::HashMap::new(),
                depends_on: None,
                retry: None,
            })
        };

        // script → mcp_prompt → script: the trailing script should chain
        // back to the first script through the mcp_prompt, not treat the
        // prompt as its parent.
        let flow_config = Arc::new(
            FlowConfig::from_path(
                FlowConfigRaw {
                    flow: Flow {
                        name: Some("skip_registration".to_string()),
                        labels: None,
                        tasks: vec![script("first"), mcp_prompt("register"), script("last")],
                        require_leader_election: None,
                        parallel_instances: 1,
                    },
                },
                "skip_registration".to_string(),
                None,
            )
            .expect("test fixture must have valid identity"),
        );

        let registry = TaskRegistry::builder(flow_config, 100).build().unwrap();

        let prompt = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "register")
            .expect("prompt missing");
        assert!(prompt.input_rx.is_none(), "prompt has no inbound channel");
        assert!(prompt.output_tx.is_none(), "prompt has no outbound channel");

        let first = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "first")
            .expect("first missing");
        let last = registry
            .tasks
            .iter()
            .find(|t| t.task_type.name() == "last")
            .expect("last missing");
        assert!(
            first.output_tx.is_some(),
            "first still emits — its child is `last`, not `register`"
        );
        assert!(
            last.input_rx.is_some(),
            "last consumes — its parent is `first`, walked past `register`"
        );
    }
}
