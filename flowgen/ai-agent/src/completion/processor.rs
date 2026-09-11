//! AI completion processor for generating text responses using LLMs.
//!
//! Provides both standard and streaming completion modes with support for
//! multiple AI providers through the Rig framework.

use crate::agent::CompletionChunk as AgentChunk;
use flowgen_core::config::ConfigExt;
use flowgen_core::event::{Event, EventBuilder, EventData, EventExt, SharedCompletionTx};
use futures_util::StreamExt;
use rig::tool::server::ToolServerHandle;
use rig::tool::{rmcp::McpClientHandler, server::ToolServer};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver, Sender};
use tracing::{error, info, Instrument};

/// Errors that can occur during completion processing.
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
    #[error("JSON serialization error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("Missing credentials path for provider: {provider}")]
    MissingCredentialsPath { provider: String },
    #[error("Failed to read credentials file at {path:?}: {source}")]
    ReadCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file at {path:?}: {source}")]
    ParseCredentials {
        path: std::path::PathBuf,
        #[source]
        source: serde_json::Error,
    },
    #[error("Rig client error: {}", _0)]
    RigClient(String),
    #[error("Completion generation failed: {}", _0)]
    CompletionFailed(String),
    #[error("Agent client creation failed: {source}")]
    AgentClient {
        #[source]
        source: crate::agent::Error,
    },
    #[error("Resource loading failed: {source}")]
    ResourceLoad {
        #[source]
        source: flowgen_core::resource::Error,
    },
    #[error("Failed to load MCP credentials for {url}: {source}")]
    McpCredentials {
        url: String,
        #[source]
        source: flowgen_core::credentials::Error,
    },
    #[error("OAuth 2.0 token fetch failed for MCP server at {url}: {source}")]
    McpOAuth2 {
        url: String,
        #[source]
        source: flowgen_core::credentials::OAuth2Error,
    },
    #[error("Failed to connect to MCP server at {url}: {reason}")]
    McpConnection { url: String, reason: String },
    #[error("Invalid Authorization header for MCP server at {url}: {source}")]
    McpAuthHeader {
        url: String,
        #[source]
        source: http::header::InvalidHeaderValue,
    },
    #[error("Invalid header name '{name}' for MCP server at {url}: {source}")]
    McpHeaderName {
        url: String,
        name: String,
        #[source]
        source: http::header::InvalidHeaderName,
    },
    #[error("Invalid value for header '{name}' for MCP server at {url}: {source}")]
    McpHeaderValue {
        url: String,
        name: String,
        #[source]
        source: http::header::InvalidHeaderValue,
    },
    #[error(
        "Client registry type mismatch — same provider/model/credentials key used with incompatible client types"
    )]
    ClientRegistryMismatch,
    #[error("Event data error: {source}")]
    Event {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to deserialize passthrough payload from event data: {source}")]
    PassthroughDeserialize {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to build passthrough request: {source}")]
    PassthroughBuild {
        #[source]
        source: super::passthrough::Error,
    },
}

/// Response structure for completion events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionResponse {
    /// Generated text response.
    pub text: String,
    /// Model used for generation.
    pub model: String,
    /// Provider that generated the response.
    pub provider: String,
    /// Optional usage statistics.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub usage: Option<CompletionUsage>,
    /// Tool invocations returned by the model. Populated only on
    /// tool-passthrough responses; the client is expected to execute
    /// each call and reply with a tool-role follow-up.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<crate::ai_gateway::config::ToolCall>,
}

/// Streaming chunk for completion events.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionChunk {
    /// Text content of this chunk.
    pub text: String,
    /// Whether this is the final chunk.
    pub is_final: bool,
    /// Index of this chunk in the sequence.
    pub index: usize,
    /// Complete tool calls carried in this chunk. Populated only on
    /// passthrough streams; the gateway emits one open/arguments
    /// delta pair per call and switches the final chunk's
    /// finish_reason to `tool_calls`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<crate::ai_gateway::config::ToolCall>,
    /// Reasoning text; when set, `text` is empty on that chunk.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<String>,
    /// Provider-reported token usage. Populated only on the final
    /// chunk (`is_final: true`) so the gateway can emit an OpenAI
    /// `stream_options.include_usage`-style usage frame before `[DONE]`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<CompletionUsage>,
}

/// Token usage statistics for completions.
///
/// Fields carry OpenAI wire names for a straight forward through
/// `ai_gateway::config::Usage` to the gateway response. `total_tokens`
/// may be greater than `prompt_tokens + completion_tokens` because
/// some providers count cached-input separately.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionUsage {
    /// Number of tokens in the prompt.
    pub prompt_tokens: u64,
    /// Number of tokens in the completion.
    pub completion_tokens: u64,
    /// Total tokens used.
    pub total_tokens: u64,
}

impl From<rig::completion::Usage> for CompletionUsage {
    fn from(u: rig::completion::Usage) -> Self {
        Self {
            prompt_tokens: u.input_tokens,
            completion_tokens: u.output_tokens,
            total_tokens: u.total_tokens,
        }
    }
}

impl From<CompletionUsage> for crate::ai_gateway::config::Usage {
    fn from(u: CompletionUsage) -> Self {
        Self {
            prompt_tokens: u.prompt_tokens,
            completion_tokens: u.completion_tokens,
            total_tokens: u.total_tokens,
        }
    }
}

/// API credentials for AI providers.
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub struct Credentials {
    /// API key for the provider (required for most providers).
    pub api_key: String,
    /// Optional organization ID (OpenAI, Anthropic).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub organization_id: Option<String>,
    /// Optional project ID (Google/Gemini).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Optional region (AWS Bedrock).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
}

/// One live MCP server connection plus the credentials used to establish
/// it. Kept together so a later refresh check can tell whether the
/// `Authorization` header has changed since `service` was connected.
struct McpServerConnection {
    url: String,
    /// `None` when the server has no `credentials_path` — never needs refresh.
    creds: Option<flowgen_core::credentials::HttpCredentials>,
    /// Static headers from config, merged with the auth header on (re)connect.
    static_headers: std::collections::HashMap<http::HeaderName, http::HeaderValue>,
    /// Header value active on `service` at connect time, for change detection.
    active_header: Option<String>,
    service: rmcp::service::RunningService<rmcp::service::RoleClient, McpClientHandler>,
}

/// Tool server handle plus the `RunningService` connections backing it. The
/// services must stay alive as long as tools are used — dropping a service
/// closes its connection.
///
/// `connections` is behind a `Mutex` (rather than stored as a plain `Vec` at
/// init time) because OAuth 2.0 client-credentials tokens expire while these
/// connections are held open for the processor's entire lifetime;
/// [`McpTools::ensure_fresh`] reconnects any server whose token has rotated.
struct McpTools {
    handle: ToolServerHandle,
    client_info: rmcp::model::ClientInfo,
    connections: tokio::sync::Mutex<Vec<McpServerConnection>>,
}

impl McpTools {
    /// Re-checks the `Authorization` header for every MCP server backed by
    /// `oauth2_client_credentials` and reconnects any whose token has
    /// rotated since the last (re)connect. Bearer/Basic/no-auth servers are
    /// static and skipped.
    ///
    /// Reconnecting registers tools under the same names via the shared
    /// `handle`, so callers with an existing `ToolServerHandle` clone see
    /// the refreshed connection without rebuilding their `AgentClient`.
    async fn ensure_fresh(&self) -> Result<(), Error> {
        let mut connections = self.connections.lock().await;
        for conn in connections.iter_mut() {
            let Some(creds) = &conn.creds else {
                continue;
            };
            let header_value =
                creds
                    .authorization_header_async()
                    .await
                    .map_err(|source| Error::McpOAuth2 {
                        url: conn.url.clone(),
                        source,
                    })?;
            if header_value == conn.active_header {
                continue;
            }

            let handler = McpClientHandler::new(self.client_info.clone(), self.handle.clone());
            let mut headers = conn.static_headers.clone();
            if let Some(header_value) = &header_value {
                let value = http::HeaderValue::from_str(header_value).map_err(|source| {
                    Error::McpAuthHeader {
                        url: conn.url.clone(),
                        source,
                    }
                })?;
                headers.insert(http::header::AUTHORIZATION, value);
            }
            let transport = build_mcp_transport(&conn.url, headers);
            let service = handler
                .connect(transport)
                .await
                .map_err(|e| Error::McpConnection {
                    url: conn.url.clone(),
                    reason: e.to_string(),
                })?;

            info!(url = %conn.url, "Reconnected MCP server with refreshed OAuth 2.0 token");
            conn.service = service;
            conn.active_header = header_value;
        }
        Ok(())
    }
}

/// Builds the streamable-HTTP transport for an MCP server with the given
/// headers (auth header merged with any static config headers).
fn build_mcp_transport(
    url: &str,
    headers: std::collections::HashMap<http::HeaderName, http::HeaderValue>,
) -> rmcp::transport::StreamableHttpClientTransport<reqwest::Client> {
    let mut transport_config =
        rmcp::transport::streamable_http_client::StreamableHttpClientTransportConfig::with_uri(url);
    if !headers.is_empty() {
        transport_config = transport_config.custom_headers(headers);
    }
    rmcp::transport::StreamableHttpClientTransport::from_config(transport_config)
}

/// Connects to the configured MCP servers and discovers their tools. `None`
/// when none are configured.
async fn connect_mcp_tools(config: &super::config::Processor) -> Result<Option<McpTools>, Error> {
    if config.mcp_servers.is_empty() {
        return Ok(None);
    }

    let handle = ToolServer::new().run();
    let client_info = rmcp::model::ClientInfo::new(
        rmcp::model::ClientCapabilities::default(),
        rmcp::model::Implementation::from_build_env(),
    );

    let mut connections = Vec::with_capacity(config.mcp_servers.len());
    for mcp_config in &config.mcp_servers {
        let handler = McpClientHandler::new(client_info.clone(), handle.clone());

        let mut oauth_creds = None;
        let mut header_value = None;

        if let Some(ref creds_path) = mcp_config.credentials_path {
            let creds = flowgen_core::credentials::load_http_credentials(creds_path)
                .await
                .map_err(|source| Error::McpCredentials {
                    url: mcp_config.url.clone(),
                    source,
                })?;
            // Route through custom_headers rather than auth_header: rmcp's
            // auth_header wraps its value in `Bearer <...>`, which would
            // double-prefix Basic credentials. Use the async variant so
            // OAuth 2.0 client credentials tokens are fetched/refreshed
            // transparently.
            header_value =
                creds
                    .authorization_header_async()
                    .await
                    .map_err(|source| Error::McpOAuth2 {
                        url: mcp_config.url.clone(),
                        source,
                    })?;
            // Only OAuth 2.0 credentials need a later refresh check — static
            // bearer/basic headers never change.
            if creds.oauth2_client_credentials.is_some() {
                oauth_creds = Some(creds);
            }
        }

        let mut static_headers = std::collections::HashMap::new();
        for (name, value) in mcp_config.headers.iter().flatten() {
            let header_name = http::HeaderName::try_from(name.as_str()).map_err(|source| {
                Error::McpHeaderName {
                    url: mcp_config.url.clone(),
                    name: name.clone(),
                    source,
                }
            })?;
            let header_value =
                http::HeaderValue::from_str(value).map_err(|source| Error::McpHeaderValue {
                    url: mcp_config.url.clone(),
                    name: name.clone(),
                    source,
                })?;
            static_headers.insert(header_name, header_value);
        }

        let mut headers = static_headers.clone();
        if let Some(header_value) = &header_value {
            let value = http::HeaderValue::from_str(header_value).map_err(|source| {
                Error::McpAuthHeader {
                    url: mcp_config.url.clone(),
                    source,
                }
            })?;
            headers.insert(http::header::AUTHORIZATION, value);
        }
        let transport = build_mcp_transport(&mcp_config.url, headers);

        let service = handler
            .connect(transport)
            .await
            .map_err(|e| Error::McpConnection {
                url: mcp_config.url.clone(),
                reason: e.to_string(),
            })?;

        info!(url = %mcp_config.url, "Connected to MCP server and discovered tools");

        connections.push(McpServerConnection {
            url: mcp_config.url.clone(),
            creds: oauth_creds,
            static_headers,
            active_header: header_value,
            service,
        });
    }

    Ok(Some(McpTools {
        handle,
        client_info,
        connections: tokio::sync::Mutex::new(connections),
    }))
}

/// Builds an `AgentClient` from a rendered config.
///
/// Loads credentials, applies optional provider tuning (temperature, max
/// tokens, static context), attaches the shared MCP tool server handle if
/// any, and returns a ready-to-use client. Used by `EventHandler::resolve_client`
/// on a cache miss in the worker-wide client registry.
async fn build_agent_client(
    config: super::config::Processor,
    tool_server_handle: Option<ToolServerHandle>,
    task_context: &flowgen_core::task::context::TaskContext,
    static_context: Option<Vec<flowgen_core::resource::Source>>,
) -> Result<crate::agent::AgentClient, Error> {
    let credentials =
        match config.provider {
            super::config::Provider::Custom => {
                if let Some(path) = &config.credentials_path {
                    Some(load_credentials(path).await?)
                } else {
                    None
                }
            }
            _ => {
                let path = config.credentials_path.as_ref().ok_or_else(|| {
                    Error::MissingCredentialsPath {
                        provider: config.provider.as_str().to_string(),
                    }
                })?;
                Some(load_credentials(path).await?)
            }
        };

    let mut builder =
        crate::agent::ClientBuilder::new(config.provider.clone(), config.model.clone());

    if let Some(creds) = credentials {
        builder = builder.credentials(creds);
    }
    if let Some(endpoint) = &config.endpoint {
        builder = builder.endpoint(endpoint.clone());
    }
    if let Some(headers) = &config.headers {
        builder = builder.headers(headers.clone());
    }
    if let Some(temp) = config.temperature {
        builder = builder.temperature(temp);
    }
    if let Some(tokens) = config.max_tokens {
        builder = builder.max_tokens(tokens);
    }

    if let Some(contexts) = static_context {
        for (idx, context_source) in contexts.iter().enumerate() {
            let resolved_text = context_source
                .resolve(task_context.resource_loader.as_ref())
                .await
                .map_err(|source| Error::ResourceLoad { source })?;
            builder = builder.static_context_document(rig::completion::Document {
                id: format!("context_{idx}"),
                text: resolved_text,
                additional_props: std::collections::HashMap::new(),
            });
        }
    }

    // Always set: rig defaults max_turns to 0, which breaks tool use.
    builder = builder.max_turns(config.max_turns);

    if let Some(handle) = tool_server_handle {
        builder = builder.tool_server_handle(handle);
    }

    if let Some(thinking) = &config.thinking {
        let params = thinking
            .to_additional_params(&config.provider)
            .map_err(|source| Error::SerdeJson { source })?;
        if let Some(params) = params {
            builder = builder.additional_params(params);
        }
    }

    builder
        .build()
        .map_err(|source| Error::AgentClient { source })
}

/// True if `err` represents an upstream HTTP 4xx (treated as permanent).
///
/// Rig surfaces provider errors as opaque strings; matched textually since
/// the typed enum doesn't carry the status. Replace once rig exposes it.
fn is_upstream_client_error(err: &Error) -> bool {
    err.to_string().contains("Invalid status code 4")
}

/// Loads credentials from a JSON file.
async fn load_credentials(path: &std::path::Path) -> Result<Credentials, Error> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| Error::ReadCredentials {
            path: path.to_path_buf(),
            source: e,
        })?;

    serde_json::from_str::<Credentials>(&content).map_err(|e| Error::ParseCredentials {
        path: path.to_path_buf(),
        source: e,
    })
}

/// Handles individual completion operations.
pub struct EventHandler {
    /// Processor configuration settings.
    config: Arc<super::config::Processor>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Event sender for passing completion responses.
    tx: Option<Sender<Event>>,
    /// Task type identifier.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
    /// MCP connections, held open for the handler's lifetime. `None` when no
    /// MCP servers are configured.
    mcp_tools: Option<Arc<McpTools>>,
}

impl EventHandler {
    /// Builds an error message tagged with the rendered provider/model/endpoint
    /// so the caller can tell which backend failed. Returns `None` if rendering
    /// itself errors — the caller then falls back to the raw error string.
    fn render_error_context(&self, event: &Event, err: &Error) -> Option<String> {
        let event_value = serde_json::value::Value::try_from(event).ok()?;
        let rendered = self.config.render(&event_value).ok()?;
        let endpoint = rendered.endpoint.as_deref().unwrap_or("default");
        Some(format!(
            "{:?}/{} @ {}: {}",
            rendered.provider, rendered.model, endpoint, err
        ))
    }

    /// Resolves the agent client for this event.
    ///
    /// Renders the config against the event so per-event templated fields
    /// (`endpoint`, `model`, `credentials_path`) become concrete values, then
    /// looks the resulting `(provider, model, endpoint, credentials_path)` up
    /// in the worker-wide client registry. The first event for a given key
    /// pays the credential-load and client-build cost; subsequent events reuse
    /// the cached `AgentClient`.
    ///
    /// MCP connections are established once in [`connect_mcp_tools`] and held
    /// for the processor's lifetime; before returning an MCP-backed client,
    /// this refreshes any OAuth 2.0 connection whose token has rotated (see
    /// [`McpTools::ensure_fresh`]), since rmcp bakes the `Authorization`
    /// header into the transport at connect time.
    async fn resolve_client(&self, event: &Event) -> Result<Arc<crate::agent::AgentClient>, Error> {
        let event_value = serde_json::value::Value::try_from(event)
            .map_err(|source| Error::EventBuilder { source })?;
        let rendered = self
            .config
            .render(&event_value)
            .map_err(|source| Error::ConfigRender { source })?;

        let key = flowgen_core::client_registry::ClientKeyBuilder::new(self.task_type)
            .field("provider", &rendered.provider.as_str())
            .field("model", &rendered.model)
            .field("endpoint", &rendered.endpoint)
            .field("credentials_path", &rendered.credentials_path)
            .build();

        if let Some(mcp_tools) = &self.mcp_tools {
            mcp_tools.ensure_fresh().await?;
        }
        let tool_server_handle = self.mcp_tools.as_ref().map(|t| t.handle.clone());
        let task_context = Arc::clone(&self.task_context);
        let static_context = self.config.static_context.clone();

        self.task_context
            .client_registry
            .get_or_init(key, || async move {
                build_agent_client(rendered, tool_server_handle, &task_context, static_context)
                    .await
            })
            .await
            .map_err(|e| match e {
                flowgen_core::client_registry::Error::Init { source } => source,
                flowgen_core::client_registry::Error::TypeMismatch => Error::ClientRegistryMismatch,
            })
    }

    /// Processes an event by generating a completion.
    ///
    /// Dispatches to streaming or non-streaming path based on config.
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        if self.config.stream {
            self.handle_streaming(event).await
        } else {
            self.handle_non_streaming(event).await
        }
    }

    /// Non-streaming completion: waits for the full response and emits a single event.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle_non_streaming(&self, event: Event) -> Result<(), Error> {
        let started_at = std::time::Instant::now();
        let client = self.resolve_client(&event).await?;
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let (rendered_prompt, rendered_system_prompt) = self.render_prompts(&event).await?;

            // Tool-use passthrough path: forward client-supplied tools to
            // the provider without running rig's agent loop, then surface
            // whatever tool_calls the model returns.
            if self.should_passthrough(&event)? {
                let response = self
                    .passthrough_response(&client, &event, rendered_system_prompt)
                    .await?;
                return self
                    .send_response(response, completion_tx_arc, started_at)
                    .await;
            }

            let completion = client
                .complete(&rendered_prompt, rendered_system_prompt.as_deref())
                .await
                .map_err(|source| Error::AgentClient { source })?;

            let response = CompletionResponse {
                text: completion.text,
                model: client.model().to_string(),
                provider: self.config.provider.as_str().to_string(),
                usage: Some(completion.usage.into()),
                tool_calls: Vec::new(),
            };

            self.send_response(response, completion_tx_arc, started_at)
                .await
        })
        .await
    }

    /// Returns true when the caller opted into passthrough and the event
    /// actually carries client-supplied tool definitions.
    fn should_passthrough(&self, event: &Arc<Event>) -> Result<bool, Error> {
        if !self.config.tool_passthrough {
            return Ok(false);
        }
        let data = event
            .data_as_json()
            .map_err(|source| Error::Event { source })?;
        let has_tools = match data.get("tools").and_then(|v| v.as_array()) {
            Some(arr) => !arr.is_empty(),
            None => false,
        };
        Ok(has_tools)
    }

    /// Passthrough dispatch: convert OpenAI-shape event data into a rig
    /// `CompletionRequest`, delegate to the client, and package the
    /// response back into the flowgen event contract.
    async fn passthrough_response(
        &self,
        client: &crate::agent::AgentClient,
        event: &Arc<Event>,
        rendered_system_prompt: Option<String>,
    ) -> Result<CompletionResponse, Error> {
        let data = event
            .data_as_json()
            .map_err(|source| Error::Event { source })?;

        let messages: Vec<crate::ai_gateway::config::Message> = match data.get("messages") {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|source| Error::PassthroughDeserialize { source })?,
            None => Vec::new(),
        };
        let tools: Vec<crate::ai_gateway::config::ToolDefinition> = match data.get("tools") {
            Some(v) if !v.is_null() => serde_json::from_value(v.clone())
                .map_err(|source| Error::PassthroughDeserialize { source })?,
            _ => Vec::new(),
        };
        let tool_choice = match data.get("tool_choice") {
            Some(v) if !v.is_null() => Some(v.clone()),
            _ => None,
        };
        let temperature = data
            .get("temperature")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);
        let max_tokens = data
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .map(|t| t as u32);

        let request = super::passthrough::build_request(
            Some(client.model().to_string()),
            rendered_system_prompt,
            &messages,
            &tools,
            tool_choice,
            temperature,
            max_tokens,
        )
        .map_err(|source| Error::PassthroughBuild { source })?;

        let resp = client
            .completion_passthrough(request)
            .await
            .map_err(|source| Error::AgentClient { source })?;

        Ok(CompletionResponse {
            text: super::passthrough::text_from_choice(&resp.choice),
            model: client.model().to_string(),
            provider: self.config.provider.as_str().to_string(),
            usage: Some(resp.usage.into()),
            tool_calls: super::passthrough::tool_calls_from_choice(&resp.choice),
        })
    }

    /// Streaming passthrough: reads OpenAI-shape event data, opens a
    /// rig streaming completion, and forwards text deltas as
    /// intermediate `CompletionChunk` events. Tool calls surface on
    /// the final chunk so the gateway can emit the OpenAI
    /// `tool_calls` delta + `finish_reason: "tool_calls"` sequence.
    async fn passthrough_stream_response(
        &self,
        client: &crate::agent::AgentClient,
        event: &Arc<Event>,
        rendered_system_prompt: Option<String>,
        started_at: std::time::Instant,
    ) -> Result<(), Error> {
        use crate::agent::PassthroughStreamEvent;
        use futures_util::StreamExt;

        let completion_tx_arc = Arc::clone(event).completion_tx.clone();
        let data = event
            .data_as_json()
            .map_err(|source| Error::Event { source })?;

        let messages: Vec<crate::ai_gateway::config::Message> = match data.get("messages") {
            Some(v) => serde_json::from_value(v.clone())
                .map_err(|source| Error::PassthroughDeserialize { source })?,
            None => Vec::new(),
        };
        let tools: Vec<crate::ai_gateway::config::ToolDefinition> = match data.get("tools") {
            Some(v) if !v.is_null() => serde_json::from_value(v.clone())
                .map_err(|source| Error::PassthroughDeserialize { source })?,
            _ => Vec::new(),
        };
        let tool_choice = match data.get("tool_choice") {
            Some(v) if !v.is_null() => Some(v.clone()),
            _ => None,
        };
        let temperature = data
            .get("temperature")
            .and_then(|v| v.as_f64())
            .map(|t| t as f32);
        let max_tokens = data
            .get("max_tokens")
            .and_then(|v| v.as_u64())
            .map(|t| t as u32);

        let request = super::passthrough::build_request(
            Some(client.model().to_string()),
            rendered_system_prompt,
            &messages,
            &tools,
            tool_choice,
            temperature,
            max_tokens,
        )
        .map_err(|source| Error::PassthroughBuild { source })?;

        let mut stream = client
            .completion_stream_passthrough(request)
            .await
            .map_err(|source| Error::AgentClient { source })?;

        let mut index: usize = 0;
        let mut accumulated_text = String::new();
        let mut collected_tool_calls: Vec<crate::ai_gateway::config::ToolCall> = Vec::new();
        let mut final_usage: Option<CompletionUsage> = None;

        while let Some(item) = stream.next().await {
            if self.task_context.cancellation_token.is_cancelled() {
                return Ok(());
            }
            match item {
                Ok(PassthroughStreamEvent::Text(text)) if text.is_empty() => {}
                Ok(PassthroughStreamEvent::Text(text)) => {
                    accumulated_text.push_str(&text);
                    let chunk = CompletionChunk {
                        text,
                        is_final: false,
                        index,
                        tool_calls: Vec::new(),
                        reasoning: None,
                        usage: None,
                    };
                    let payload = serde_json::to_value(&chunk)
                        .map_err(|source| Error::SerdeJson { source })?;
                    let e = EventBuilder::new()
                        .data(EventData::Json(payload))
                        .subject(self.config.name.clone())
                        .task_id(self.task_id)
                        .task_type(self.task_type)
                        .build()
                        .map_err(|source| Error::EventBuilder { source })?;
                    e.send_silent(self.tx.as_ref())
                        .await
                        .map_err(|source| Error::SendMessage { source })?;
                    index += 1;
                }
                Ok(PassthroughStreamEvent::ToolCall(call)) => {
                    collected_tool_calls
                        .push(super::passthrough::rig_tool_call_to_oai_public(&call));
                }
                Ok(PassthroughStreamEvent::Done(usage)) => {
                    final_usage = usage.map(Into::into);
                    break;
                }
                Err(source) => return Err(Error::CompletionFailed(source.to_string())),
            }
        }

        let final_chunk = CompletionChunk {
            text: accumulated_text,
            is_final: true,
            index,
            tool_calls: collected_tool_calls,
            reasoning: None,
            usage: final_usage.clone(),
        };
        let payload =
            serde_json::to_value(&final_chunk).map_err(|source| Error::SerdeJson { source })?;

        let completion_ctx = crate::meta::CompletionContext {
            provider: self.config.provider.as_str().to_string(),
            model: client.model().to_string(),
            prompt_tokens: final_usage.as_ref().map(|u| u.prompt_tokens),
            completion_tokens: final_usage.as_ref().map(|u| u.completion_tokens),
            total_tokens: final_usage.as_ref().map(|u| u.total_tokens),
            latency_ms: started_at.elapsed().as_millis() as u64,
        };
        let mut meta = serde_json::Map::new();
        completion_ctx.insert_into(&mut meta);

        let mut e = EventBuilder::new()
            .data(EventData::Json(payload))
            .subject(self.config.name.clone())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .meta(meta)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;

        match self.tx {
            None => {
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(e.data_as_json().ok());
                }
            }
            Some(_) => {
                e.completion_tx = completion_tx_arc.clone();
            }
        }

        e.send_with_logging(self.tx.as_ref())
            .context(crate::meta::PROVIDER, &completion_ctx.provider)
            .context(crate::meta::MODEL, &completion_ctx.model)
            .context(crate::meta::LATENCY_MS, completion_ctx.latency_ms)
            .context(
                crate::meta::PROMPT_TOKENS,
                completion_ctx.prompt_tokens.unwrap_or(0),
            )
            .context(
                crate::meta::COMPLETION_TOKENS,
                completion_ctx.completion_tokens.unwrap_or(0),
            )
            .context(
                crate::meta::TOTAL_TOKENS,
                completion_ctx.total_tokens.unwrap_or(0),
            )
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }

    /// Streaming completion: emits intermediate chunk events as they arrive,
    /// then a final chunk with `is_final: true`. Only the final event carries
    /// the completion signal so downstream tasks see all chunks before the
    /// source considers the request complete.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle_streaming(&self, event: Event) -> Result<(), Error> {
        let started_at = std::time::Instant::now();
        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        // Extract correlation_id from event meta for streaming progress back to source tasks.
        let correlation_id = event
            .meta
            .as_ref()
            .and_then(|m| m.get(flowgen_core::registry::CORRELATION_ID))
            .and_then(|v| v.as_str())
            .map(String::from);

        let client = self.resolve_client(&event).await?;

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            let (rendered_prompt, rendered_system_prompt) = self.render_prompts(&event).await?;

            // Streaming passthrough path — mirrors the non-streaming
            // branch in `handle_non_streaming`. Feeds an OpenAI-shape
            // event stream back through the same downstream channel.
            if self.should_passthrough(&event)? {
                return self
                    .passthrough_stream_response(
                        &client,
                        &event,
                        rendered_system_prompt,
                        started_at,
                    )
                    .await;
            }

            let mut stream = client
                .complete_stream(&rendered_prompt, rendered_system_prompt.as_deref())
                .await;

            let mut index: usize = 0;
            let mut accumulated_text = String::new();
            let mut final_usage: Option<CompletionUsage> = None;

            while let Some(chunk) = stream.next().await {
                if self.task_context.cancellation_token.is_cancelled() {
                    return Ok(());
                }

                match chunk {
                    AgentChunk::Text(text) => {
                        accumulated_text.push_str(&text);

                        // Push streaming progress to the response registry if a
                        // correlation_id is present (webhook/ai_gateway SSE streaming).
                        if let (Some(cid), Some(registry)) =
                            (&correlation_id, &self.task_context.response_registry)
                        {
                            registry
                                .send_progress(
                                    cid,
                                    flowgen_core::registry::ProgressEvent {
                                        task: self.config.name.clone(),
                                        status: text.clone(),
                                        tool: None,
                                    },
                                )
                                .await;
                        }

                        let chunk_event = CompletionChunk {
                            text,
                            is_final: false,
                            index,
                            tool_calls: Vec::new(),
                            reasoning: None,
                            usage: None,
                        };
                        let data = serde_json::to_value(&chunk_event)
                            .map_err(|source| Error::SerdeJson { source })?;

                        // Intermediate chunks do not carry the completion signal,
                        // and we route through `send_silent` because per-token
                        // logging would flood the operator on streamed responses.
                        // The final chunk below still goes through `send_with_logging`
                        // so completion stays observable.
                        let e = EventBuilder::new()
                            .data(EventData::Json(data))
                            .subject(self.config.name.clone())
                            .task_id(self.task_id)
                            .task_type(self.task_type)
                            .build()
                            .map_err(|source| Error::EventBuilder { source })?;

                        e.send_silent(self.tx.as_ref())
                            .await
                            .map_err(|source| Error::SendMessage { source })?;
                        index += 1;
                    }
                    AgentChunk::Reasoning(text) => {
                        let chunk_event = CompletionChunk {
                            text: String::new(),
                            is_final: false,
                            index,
                            tool_calls: Vec::new(),
                            reasoning: Some(text),
                            usage: None,
                        };
                        let data = serde_json::to_value(&chunk_event)
                            .map_err(|source| Error::SerdeJson { source })?;

                        let e = EventBuilder::new()
                            .data(EventData::Json(data))
                            .subject(self.config.name.clone())
                            .task_id(self.task_id)
                            .task_type(self.task_type)
                            .build()
                            .map_err(|source| Error::EventBuilder { source })?;

                        e.send_silent(self.tx.as_ref())
                            .await
                            .map_err(|source| Error::SendMessage { source })?;
                        index += 1;
                    }
                    AgentChunk::ToolCall { name, arguments } => {
                        // Surface as a progress step on the same channel as text
                        // deltas so it stays ordered relative to them.
                        if let (Some(cid), Some(registry)) =
                            (&correlation_id, &self.task_context.response_registry)
                        {
                            registry
                                .send_progress(
                                    cid,
                                    flowgen_core::registry::ProgressEvent {
                                        task: self.config.name.clone(),
                                        status: String::new(),
                                        tool: Some(flowgen_core::registry::ToolStep {
                                            name,
                                            arguments,
                                        }),
                                    },
                                )
                                .await;
                        }
                    }
                    AgentChunk::Final { text, usage } => {
                        accumulated_text = text;
                        final_usage = Some(usage.into());
                    }
                    AgentChunk::Error(err) => {
                        // Logging only would hang the source until ack_timeout.
                        return Err(Error::CompletionFailed(err.to_string()));
                    }
                }
            }

            // Emit the final chunk with the complete accumulated text.
            let final_chunk = CompletionChunk {
                text: accumulated_text,
                is_final: true,
                index,
                tool_calls: Vec::new(),
                reasoning: None,
                usage: final_usage.clone(),
            };
            let data =
                serde_json::to_value(&final_chunk).map_err(|source| Error::SerdeJson { source })?;

            let completion_ctx = crate::meta::CompletionContext {
                provider: self.config.provider.as_str().to_string(),
                model: client.model().to_string(),
                prompt_tokens: final_usage.as_ref().map(|u| u.prompt_tokens),
                completion_tokens: final_usage.as_ref().map(|u| u.completion_tokens),
                total_tokens: final_usage.as_ref().map(|u| u.total_tokens),
                latency_ms: started_at.elapsed().as_millis() as u64,
            };
            let mut meta = serde_json::Map::new();
            completion_ctx.insert_into(&mut meta);

            let mut e = EventBuilder::new()
                .data(EventData::Json(data))
                .subject(self.config.name.clone())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .meta(meta)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Only the final event carries the completion signal.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .context(crate::meta::PROVIDER, &completion_ctx.provider)
                .context(crate::meta::MODEL, &completion_ctx.model)
                .context(crate::meta::LATENCY_MS, completion_ctx.latency_ms)
                .context(
                    crate::meta::PROMPT_TOKENS,
                    completion_ctx.prompt_tokens.unwrap_or(0),
                )
                .context(
                    crate::meta::COMPLETION_TOKENS,
                    completion_ctx.completion_tokens.unwrap_or(0),
                )
                .context(
                    crate::meta::TOTAL_TOKENS,
                    completion_ctx.total_tokens.unwrap_or(0),
                )
                .await
                .map_err(|source| Error::SendMessage { source })?;

            Ok(())
        })
        .await
    }

    /// Renders prompt and optional system prompt from the event data.
    async fn render_prompts(&self, event: &Arc<Event>) -> Result<(String, Option<String>), Error> {
        let event_value = serde_json::value::Value::try_from(event.as_ref())
            .map_err(|source| Error::EventBuilder { source })?;

        let rendered_prompt = self
            .config
            .prompt
            .render(self.task_context.resource_loader.as_ref(), &event_value)
            .await
            .map_err(|source| Error::ResourceLoad { source })?;

        let rendered_system_prompt = if let Some(ref system_prompt) = self.config.system_prompt {
            Some(
                system_prompt
                    .render(self.task_context.resource_loader.as_ref(), &event_value)
                    .await
                    .map_err(|source| Error::ResourceLoad { source })?,
            )
        } else {
            None
        };

        Ok((rendered_prompt, rendered_system_prompt))
    }

    /// Sends a completion response as an event. `started_at` is the
    /// wall-clock start of the handle call so latency lands on
    /// `event.meta` and the structured log line.
    async fn send_response(
        &self,
        response: CompletionResponse,
        completion_tx_arc: Option<SharedCompletionTx>,
        started_at: std::time::Instant,
    ) -> Result<(), Error> {
        let response_value =
            serde_json::to_value(&response).map_err(|source| Error::SerdeJson { source })?;

        let completion_ctx = crate::meta::CompletionContext {
            provider: response.provider.clone(),
            model: response.model.clone(),
            prompt_tokens: response.usage.as_ref().map(|u| u.prompt_tokens),
            completion_tokens: response.usage.as_ref().map(|u| u.completion_tokens),
            total_tokens: response.usage.as_ref().map(|u| u.total_tokens),
            latency_ms: started_at.elapsed().as_millis() as u64,
        };

        // Start from the upstream event's meta so custom fields stashed by
        // preceding tasks (via ctx.meta in scripts, for example) survive
        // through the completion — then layer the completion context on top.
        // Replacing meta wholesale here silently drops per-event state that
        // downstream tasks depend on.
        let mut event = EventBuilder::new()
            .data(EventData::Json(response_value))
            .subject(self.config.name.clone())
            .task_id(self.task_id)
            .task_type(self.task_type)
            .build()
            .map_err(|source| Error::EventBuilder { source })?;
        let meta = event.meta.get_or_insert_with(serde_json::Map::new);
        completion_ctx.insert_into(meta);

        // Signal completion or pass through to next task.
        match self.tx {
            None => {
                // Leaf task: signal completion.
                if let Some(arc) = completion_tx_arc.as_ref() {
                    arc.signal_completion(event.data_as_json().ok());
                }
            }
            Some(_) => {
                // Pass through completion_tx to next task.
                event.completion_tx = completion_tx_arc.clone();
            }
        }

        event
            .send_with_logging(self.tx.as_ref())
            .context(crate::meta::PROVIDER, &completion_ctx.provider)
            .context(crate::meta::MODEL, &completion_ctx.model)
            .context(crate::meta::LATENCY_MS, completion_ctx.latency_ms)
            .context(
                crate::meta::PROMPT_TOKENS,
                completion_ctx.prompt_tokens.unwrap_or(0),
            )
            .context(
                crate::meta::COMPLETION_TOKENS,
                completion_ctx.completion_tokens.unwrap_or(0),
            )
            .context(
                crate::meta::TOTAL_TOKENS,
                completion_ctx.total_tokens.unwrap_or(0),
            )
            .await
            .map_err(|source| Error::SendMessage { source })?;

        Ok(())
    }
}

/// AI completion processor.
#[derive(Debug)]
pub struct Processor {
    /// Completion task configuration.
    config: Arc<super::config::Processor>,
    /// Channel sender for passing completion events.
    tx: Option<Sender<Event>>,
    /// Channel receiver for incoming events.
    rx: Receiver<Event>,
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

    /// Initializes the processor. Connects to MCP servers once; credentials,
    /// endpoint, and model resolution are deferred to `resolve_client` for
    /// per-event templating.
    async fn init(&self) -> Result<Self::EventHandler, Self::Error> {
        let mcp_tools = connect_mcp_tools(&self.config).await?.map(Arc::new);

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
            mcp_tools,
        })
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
                        error!(error = %e, "Failed to initialize completion processor");
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
                                        error!(error = %e, "Failed to generate completion");
                                        // Upstream 4xx (bad request, auth, quota) won't recover from retry.
                                        if is_upstream_client_error(&e) {
                                            Err(tokio_retry::RetryError::permanent(e))
                                        } else {
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "Completion failed after all retry attempts");
                                // Tag with provider/model so multi-backend proxies show which one failed.
                                let context_msg = event_handler
                                    .render_error_context(&event, &err)
                                    .unwrap_or_else(|| err.to_string());
                                let mut error_event = event.clone();
                                error_event.error = Some(context_msg.clone());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(context_msg);
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

/// Builder for constructing Processor instances with validation.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Processor configuration (required for build).
    config: Option<Arc<super::config::Processor>>,
    /// Event sender for passing events to next task (optional if this is the last task).
    tx: Option<Sender<Event>>,
    /// Event receiver for incoming events (required for build).
    rx: Option<Receiver<Event>>,
    /// Current task identifier for event filtering.
    task_id: usize,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Task type for event categorization and logging.
    task_type: Option<&'static str>,
}

impl ProcessorBuilder {
    pub fn new() -> ProcessorBuilder {
        ProcessorBuilder {
            ..Default::default()
        }
    }

    pub fn config(mut self, config: Arc<super::config::Processor>) -> Self {
        self.config = Some(config);
        self
    }

    pub fn receiver(mut self, receiver: Receiver<Event>) -> Self {
        self.rx = Some(receiver);
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

    #[test]
    fn test_completion_response_serialization() {
        let response = CompletionResponse {
            text: "Hello, world!".to_string(),
            model: "gpt-4".to_string(),
            provider: "OpenAi".to_string(),
            usage: Some(CompletionUsage {
                prompt_tokens: 10,
                completion_tokens: 5,
                total_tokens: 15,
            }),
            tool_calls: Vec::new(),
        };

        let json = serde_json::to_string(&response).unwrap();
        let deserialized: CompletionResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(response.text, deserialized.text);
        assert_eq!(response.model, deserialized.model);
    }

    #[test]
    fn test_completion_chunk_serialization() {
        let chunk = CompletionChunk {
            text: "Hello".to_string(),
            is_final: false,
            index: 0,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: None,
        };

        let json = serde_json::to_string(&chunk).unwrap();
        let deserialized: CompletionChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.text, deserialized.text);
        assert_eq!(chunk.is_final, deserialized.is_final);
        assert_eq!(chunk.index, deserialized.index);
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = Credentials {
            api_key: "sk-test123".to_string(),
            organization_id: Some("org-123".to_string()),
            project_id: None,
            region: None,
        };

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: Credentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }

    // --- CompletionResponse ---

    #[test]
    fn completion_response_without_usage() {
        let resp = CompletionResponse {
            text: "answer".to_string(),
            model: "gpt-4".to_string(),
            provider: "OpenAi".to_string(),
            usage: None,
            tool_calls: Vec::new(),
        };
        let v = serde_json::to_value(&resp).unwrap();
        assert!(!v.as_object().unwrap().contains_key("usage"));
        assert_eq!(v["text"], "answer");
        assert_eq!(v["model"], "gpt-4");
        assert_eq!(v["provider"], "OpenAi");
    }

    #[test]
    fn completion_response_deser_from_json() {
        let json = r#"{
            "text": "response text",
            "model": "claude-3",
            "provider": "Anthropic",
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150
            }
        }"#;
        let resp: CompletionResponse = serde_json::from_str(json).unwrap();
        assert_eq!(resp.text, "response text");
        assert_eq!(resp.model, "claude-3");
        assert_eq!(resp.provider, "Anthropic");
        let usage = resp.usage.unwrap();
        assert_eq!(usage.prompt_tokens, 100);
        assert_eq!(usage.completion_tokens, 50);
        assert_eq!(usage.total_tokens, 150);
    }

    // --- CompletionChunk ---

    #[test]
    fn completion_chunk_intermediate() {
        let chunk = CompletionChunk {
            text: "partial".to_string(),
            is_final: false,
            index: 3,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: None,
        };
        let v = serde_json::to_value(&chunk).unwrap();
        assert_eq!(v["text"], "partial");
        assert_eq!(v["is_final"], false);
        assert_eq!(v["index"], 3);
    }

    #[test]
    fn completion_chunk_final() {
        let chunk = CompletionChunk {
            text: "full accumulated response".to_string(),
            is_final: true,
            index: 10,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: None,
        };
        let v = serde_json::to_value(&chunk).unwrap();
        assert_eq!(v["is_final"], true);
        assert_eq!(v["index"], 10);
    }

    #[test]
    fn completion_chunk_roundtrip() {
        let chunk = CompletionChunk {
            text: "text".to_string(),
            is_final: false,
            index: 0,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: None,
        };
        let json = serde_json::to_string(&chunk).unwrap();
        let back: CompletionChunk = serde_json::from_str(&json).unwrap();
        assert_eq!(chunk.text, back.text);
        assert_eq!(chunk.is_final, back.is_final);
        assert_eq!(chunk.index, back.index);
    }

    // --- CompletionUsage ---

    #[test]
    fn completion_usage_roundtrip() {
        let usage = CompletionUsage {
            prompt_tokens: 200,
            completion_tokens: 100,
            total_tokens: 300,
        };
        let json = serde_json::to_string(&usage).unwrap();
        let back: CompletionUsage = serde_json::from_str(&json).unwrap();
        assert_eq!(usage.prompt_tokens, back.prompt_tokens);
        assert_eq!(usage.completion_tokens, back.completion_tokens);
        assert_eq!(usage.total_tokens, back.total_tokens);
    }

    // --- CompletionChunk.usage on final chunk ---

    #[test]
    fn completion_chunk_final_carries_usage() {
        let chunk = CompletionChunk {
            text: "done".to_string(),
            is_final: true,
            index: 5,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: Some(CompletionUsage {
                prompt_tokens: 100,
                completion_tokens: 50,
                total_tokens: 150,
            }),
        };
        let v = serde_json::to_value(&chunk).unwrap();
        assert_eq!(v["is_final"], true);
        assert_eq!(v["usage"]["prompt_tokens"], 100);
        assert_eq!(v["usage"]["completion_tokens"], 50);
        assert_eq!(v["usage"]["total_tokens"], 150);
    }

    #[test]
    fn completion_chunk_intermediate_omits_usage_on_wire() {
        let chunk = CompletionChunk {
            text: "partial".to_string(),
            is_final: false,
            index: 3,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: None,
        };
        let v = serde_json::to_value(&chunk).unwrap();
        assert!(
            !v.as_object().unwrap().contains_key("usage"),
            "intermediate chunks must not serialise a usage key"
        );
    }

    #[test]
    fn completion_chunk_with_usage_roundtrip() {
        let chunk = CompletionChunk {
            text: "final".to_string(),
            is_final: true,
            index: 1,
            tool_calls: Vec::new(),
            reasoning: None,
            usage: Some(CompletionUsage {
                prompt_tokens: 10,
                completion_tokens: 20,
                total_tokens: 30,
            }),
        };
        let json = serde_json::to_string(&chunk).unwrap();
        let back: CompletionChunk = serde_json::from_str(&json).unwrap();
        let usage = back.usage.expect("usage roundtripped");
        assert_eq!(usage.prompt_tokens, 10);
        assert_eq!(usage.completion_tokens, 20);
        assert_eq!(usage.total_tokens, 30);
    }

    // --- Credentials optional fields ---

    #[test]
    fn credentials_minimal_deser() {
        let json = r#"{"api_key": "sk-abc"}"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.api_key, "sk-abc");
        assert_eq!(creds.organization_id, None);
        assert_eq!(creds.project_id, None);
        assert_eq!(creds.region, None);
    }

    #[test]
    fn credentials_all_fields() {
        let json = r#"{
            "api_key": "sk-123",
            "organization_id": "org-456",
            "project_id": "proj-789",
            "region": "us-east-1"
        }"#;
        let creds: Credentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.api_key, "sk-123");
        assert_eq!(creds.organization_id, Some("org-456".to_string()));
        assert_eq!(creds.project_id, Some("proj-789".to_string()));
        assert_eq!(creds.region, Some("us-east-1".to_string()));
    }

    #[test]
    fn credentials_skip_serializing_none() {
        let creds = Credentials {
            api_key: "sk-x".to_string(),
            organization_id: None,
            project_id: None,
            region: None,
        };
        let v = serde_json::to_value(&creds).unwrap();
        let obj = v.as_object().unwrap();
        assert!(obj.contains_key("api_key"));
        assert!(!obj.contains_key("organization_id"));
        assert!(!obj.contains_key("project_id"));
        assert!(!obj.contains_key("region"));
    }

    #[test]
    fn credentials_missing_api_key_rejected() {
        let json = r#"{"organization_id": "org-1"}"#;
        let result = serde_json::from_str::<Credentials>(json);
        assert!(result.is_err());
    }
}
