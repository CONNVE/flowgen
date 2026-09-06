//! AI gateway role for the generic HTTP server.
//!
//! Implements the OpenAI-compatible chat completions surface: a single
//! `POST <path>/chat/completions` route + a `GET <path>/models` model-listing
//! route. Per-flow routing is driven by the request body's `model` field
//! (`model: "<gateway-name>/<downstream-model>"`); the dispatcher splits on
//! the first `/`, looks the gateway up by name, and forwards the request to
//! the pipeline with the model field rewritten to the downstream portion.
//!
//! The server lifecycle, dispatch table, and hot-reload semantics live in
//! `flowgen_core::http_server`; this module owns only the AI-gateway-specific
//! URL layout and OpenAI translation logic.

use crate::ai_gateway::config::{
    self, ChatCompletionRequest, EventPayload, Message, Protocol, SSE_DONE,
};
use crate::ai_gateway::protocol::completions::OpenAiAdapter;
use crate::ai_gateway::protocol::messages::{
    self as anthropic_msg, MessagesAdapter, MessagesRequest,
};
use crate::ai_gateway::protocol::{
    ProtocolAdapter, StopReason, StreamWriter as ProtocolStreamWriter,
};
use crate::meta;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Router,
};
use base64::Engine;
use flowgen_core::auth::{extract_bearer_token, AuthProvider};
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::{new_completion_channel, Event, EventBuilder, EventData, EventExt};
use flowgen_core::http_server::{
    headers_satisfy, record_headers_rejected_activity, DispatchState, Dispatcher, HasFlowName,
    HttpServer,
};
use flowgen_core::registry::{ProgressEvent, ResponseRegistry, ResponseSender};
use serde::Serialize;
use std::sync::Arc;
use tokio::sync::mpsc::{self, Sender};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::{error, warn, Instrument};

/// Default port for the AI gateway server.
pub const DEFAULT_AI_GATEWAY_PORT: u16 = 3002;

/// Default path prefix for AI gateway routes. Matches the de-facto
/// OpenAI-compatible convention used by vLLM, Ollama, LiteLLM, OpenRouter,
/// and the OpenAI SDKs.
pub const DEFAULT_AI_GATEWAY_PATH: &str = "/v1";

/// Convenience type alias for the AI gateway server.
pub type AiGatewayServer = HttpServer<AiGatewayDispatcher>;

/// Dispatch-table entry describing one registered LLM proxy.
///
/// Stored in the AI gateway server's dispatch table keyed by the proxy's
/// `name` (the `name` field of the `llm_proxy` task config). For OpenAI
/// protocol the dispatcher routes by the request body's `model` field
/// (`"<name>/<downstream-model>"`); the remainder is forwarded as the real
/// model name in the pipeline event.
///
/// `flow_name` lets the server bulk-deregister every proxy owned by a flow
/// when the flow is stopped or hot-reloaded. `protocol` lets the dispatcher
/// reject requests on the wrong URL for this registration's protocol.
#[derive(Clone)]
pub struct LlmProxyRegistration {
    /// Name of the flow that registered this proxy.
    pub flow_name: String,
    /// Human-readable name from the flow's `labels.display_name`, shown in the
    /// model list instead of the bare proxy id. `None` when unset.
    pub display_name: Option<String>,
    /// Short description from the flow's `labels.description`, surfaced
    /// alongside the model list entry. `None` when unset.
    pub description: Option<String>,
    /// Wire protocol exposed for this proxy. Determines which URL handler
    /// is allowed to dispatch to it.
    pub protocol: Protocol,
    /// Full processor configuration. The dispatcher reads `auth`, `ack_timeout`,
    /// and `name` from here.
    pub config: Arc<config::Processor>,
    /// Optional bearer-token credentials loaded from `config.credentials_path`.
    pub credentials: Option<HttpCredentials>,
    /// Optional auth provider for user identity resolution (JWT, OIDC, session).
    pub auth_provider: Option<Arc<dyn AuthProvider>>,
    /// Channel to send the inbound chat-completion event into the flow pipeline.
    pub tx: Sender<Event>,
    /// Task identifier used when constructing pipeline events.
    pub task_id: usize,
    /// Task type label used when constructing pipeline events.
    pub task_type: &'static str,
    /// Shared response registry for streaming chunks back to the OpenAI client
    /// (SSE) or awaiting the final completion (non-streaming).
    pub response_registry: Arc<ResponseRegistry>,
    /// Number of leaf tasks reachable from this gateway source.
    pub leaf_count: usize,
    /// Cancellation token from the owning flow's task tenure.
    pub cancellation_token: CancellationToken,
}

impl HasFlowName for LlmProxyRegistration {
    fn flow_name(&self) -> &str {
        &self.flow_name
    }
}

/// Default AI gateway request body limit. Sized for 1 M-token
/// prompts with tool schemas and multi-turn histories.
pub const DEFAULT_AI_GATEWAY_MAX_BODY_BYTES: usize = 128 * 1024 * 1024;

/// Worker-level dispatcher tuning knobs. Kept small so the framework
/// contract stays generic.
#[derive(Clone, Debug)]
pub struct AiGatewayExtras {
    /// Maximum inbound request body size, in bytes.
    pub max_body_bytes: usize,
}

impl Default for AiGatewayExtras {
    fn default() -> Self {
        Self {
            max_body_bytes: DEFAULT_AI_GATEWAY_MAX_BODY_BYTES,
        }
    }
}

/// Dispatcher for AI gateway traffic.
///
/// Wires `POST <path>/chat/completions` and `GET <path>/models` and routes
/// chat completions by the `model` field of the request body.
pub struct AiGatewayDispatcher;

impl Dispatcher for AiGatewayDispatcher {
    type Registration = LlmProxyRegistration;
    type Extras = AiGatewayExtras;

    fn build_router(state: DispatchState<Self::Registration, Self::Extras>) -> Router {
        let prefix = state.path.trim_end_matches('/').to_string();
        let chat_route = format!("{prefix}/chat/completions");
        let messages_route = format!("{prefix}/messages");
        let models_route = format!("{prefix}/models");
        let body_limit = state.extras.max_body_bytes;
        Router::new()
            .route(&chat_route, post(dispatch_chat_completions))
            .route(&messages_route, post(dispatch_messages))
            .route(&models_route, get(list_models))
            .layer(axum::extract::DefaultBodyLimit::max(body_limit))
            .with_state(state)
    }
}

// ---------------------------------------------------------------------------
// Dispatcher entry points
// ---------------------------------------------------------------------------

/// Compact OpenAI-compatible response for `GET <path>/models`.
#[derive(Serialize)]
struct ModelsResponse {
    object: &'static str,
    data: Vec<ModelEntry>,
}

#[derive(Serialize)]
struct ModelEntry {
    id: String,
    object: &'static str,
    created: i64,
    owned_by: &'static str,
    /// Wire protocol the proxy speaks (`"openai"` / `"anthropic"`). Non-standard
    /// OpenAI field; lets a client hide proxies it can't drive (the built-in
    /// chat speaks OpenAI, so it skips Anthropic proxies).
    protocol: &'static str,
    /// Friendly name from the proxy flow's `labels.display_name`, when set.
    /// Non-standard OpenAI field; clients that don't know it ignore it.
    #[serde(skip_serializing_if = "Option::is_none")]
    display_name: Option<String>,
    /// Short description from the proxy flow's `labels.description`, when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
}

/// Lists advertised models in the OpenAI schema. Each proxy contributes one
/// `<proxy>/<model>` id per declared model; a proxy with no `models` falls
/// back to its bare name so it still appears. Uses the same `headers` check
/// as [`resolve_registration`] so a proxy is never listed without also
/// being reachable.
async fn list_models(
    State(state): State<DispatchState<LlmProxyRegistration, AiGatewayExtras>>,
    headers: HeaderMap,
) -> Response {
    let created = chrono::Utc::now().timestamp();
    let data = state
        .table
        .iter()
        .filter(|e| headers_satisfy(&headers, &e.value().config.headers))
        .flat_map(|e| {
            let proxy = e.key().clone();
            let reg = e.value();
            let display_name = reg.display_name.clone();
            let description = reg.description.clone();
            let protocol = reg.protocol.as_str();
            let entry = |id: String| ModelEntry {
                id,
                object: "model",
                created,
                owned_by: "flowgen",
                protocol,
                display_name: display_name.clone(),
                description: description.clone(),
            };
            match reg.config.models.is_empty() {
                true => vec![entry(proxy)],
                false => reg
                    .config
                    .models
                    .iter()
                    .map(|m| entry(format!("{proxy}/{m}")))
                    .collect(),
            }
        })
        .collect();
    axum::Json(ModelsResponse {
        object: "list",
        data,
    })
    .into_response()
}

/// Handler for `POST <path>/chat/completions`. Peeks `body.model` to
/// find the registration, translates the OpenAI request into the
/// shared `EventPayload`, and hands off to the generic dispatcher.
async fn dispatch_chat_completions(
    State(state): State<DispatchState<LlmProxyRegistration, AiGatewayExtras>>,
    headers: HeaderMap,
    axum::Json(request): axum::Json<ChatCompletionRequest>,
) -> Response {
    let (proxy_name, downstream_model) = match split_model(request.model.as_deref().unwrap_or("")) {
        Ok(parts) => parts,
        Err(e) => return e.into_response(),
    };

    let ctx = match resolve_registration(&state, Protocol::Openai, proxy_name, &headers).await {
        Ok(c) => c,
        Err(resp) => return *resp,
    };

    if let Some(resp) = require_model_choice(&ctx.registration, &downstream_model) {
        return resp;
    }

    let system_prompt = request
        .messages
        .iter()
        .find(|m| m.is_system())
        .and_then(|m| m.content.clone());

    let user_messages: Vec<&Message> = request.messages.iter().filter(|m| !m.is_system()).collect();

    // Skip messages without textual content (assistant tool-call
    // messages, tool-role replies) when synthesising the flat prompt
    // string that non-passthrough flows expect.
    let prompt = user_messages
        .iter()
        .filter_map(|m| m.content.as_deref())
        .collect::<Vec<_>>()
        .join("\n");

    // Attach the full OpenAI-shape payload only when the client
    // actually sent `tools`. Pumping the raw message list into
    // event.data on every request pushes flows over the Rhai
    // template renderer's expression-size limit.
    let client_sent_tools = matches!(&request.tools, Some(t) if !t.is_empty());
    let payload = EventPayload {
        prompt: &prompt,
        system_prompt: system_prompt.as_deref(),
        model: downstream_model.as_deref(),
        temperature: request.temperature,
        max_tokens: request.max_tokens,
        stream: request.stream,
        messages: match client_sent_tools {
            true => Some(request.messages.as_slice()),
            false => None,
        },
        tools: match (client_sent_tools, request.tools.as_deref()) {
            (true, Some(t)) => Some(t),
            _ => None,
        },
        tool_choice: match client_sent_tools {
            true => request.tool_choice.as_ref(),
            false => None,
        },
    };

    let include_usage = request
        .stream_options
        .as_ref()
        .map(|o| o.include_usage)
        .unwrap_or(false);

    dispatch::<OpenAiAdapter>(ctx, &payload, request.stream, include_usage).await
}

/// Handler for `POST <path>/messages`. Parses the Anthropic Messages
/// API request, translates it into the shared `EventPayload`, and
/// hands off to the generic dispatcher. Endpoint-level auth also
/// accepts `x-api-key` (Claude Code's default header) in addition to
/// the standard `Authorization: Bearer` handled by the shared path.
async fn dispatch_messages(
    State(state): State<DispatchState<LlmProxyRegistration, AiGatewayExtras>>,
    mut headers: HeaderMap,
    axum::Json(request): axum::Json<MessagesRequest>,
) -> Response {
    // Claude Code sends the endpoint credential as `x-api-key: <token>`.
    // Fold it into the standard `Authorization: Bearer <token>` header
    // before running the shared auth path so existing credentials JSON
    // files keep working unchanged.
    if !headers.contains_key(header::AUTHORIZATION) {
        if let Some(key) = headers.get("x-api-key").and_then(|v| v.to_str().ok()) {
            if let Ok(value) = format!("Bearer {key}").parse() {
                headers.insert(header::AUTHORIZATION, value);
            }
        }
    }

    let (proxy_name, downstream_model) = match split_model(&request.model) {
        Ok(parts) => parts,
        Err(e) => return e.into_response(),
    };

    let ctx = match resolve_registration(&state, Protocol::Anthropic, proxy_name, &headers).await {
        Ok(c) => c,
        Err(resp) => return *resp,
    };

    if let Some(resp) = require_model_choice(&ctx.registration, &downstream_model) {
        return resp;
    }

    let translated = anthropic_msg::translate_request(&request);

    let payload = EventPayload {
        prompt: &translated.prompt,
        system_prompt: translated.system_prompt.as_deref(),
        model: downstream_model.as_deref(),
        temperature: request.temperature,
        max_tokens: Some(request.max_tokens),
        stream: request.stream,
        messages: translated.messages.as_deref(),
        tools: translated.tools.as_deref(),
        tool_choice: translated.tool_choice.as_ref(),
    };

    // Anthropic has no `stream_options.include_usage` opt-in — usage
    // always rides `message_delta`. Pass `true` so the adapter's
    // writer sees it (the writer ignores the flag).
    dispatch::<MessagesAdapter>(ctx, &payload, request.stream, true).await
}

// ---------------------------------------------------------------------------
// Shared dispatch skeleton
// ---------------------------------------------------------------------------

/// Everything the generic dispatcher needs to know about a resolved
/// request: the target registration and the caller's user identity
/// (when auth is enabled). Built once by `resolve_registration` and
/// consumed by `dispatch::<Adapter>`.
struct DispatchContext {
    registration: LlmProxyRegistration,
    user_context: Option<flowgen_core::auth::UserContext>,
}

/// Per-request state passed from `dispatch<A>` into the blocking /
/// streaming branches. Grouping it into one struct keeps each branch's
/// signature narrow.
struct DispatchRequest<'a> {
    data: serde_json::Value,
    meta: serde_json::Map<String, serde_json::Value>,
    model: String,
    request_id: String,
    created: i64,
    include_usage: bool,
    gateway_ctx: &'a crate::meta::GatewayContext,
}

/// Look up the registration by name, enforce the URL/protocol guard,
/// run endpoint- and user-level auth, and hand back a `DispatchContext`
/// on success. Returns a pre-formatted `Response` on failure so the
/// caller can short-circuit.
async fn resolve_registration(
    state: &DispatchState<LlmProxyRegistration, AiGatewayExtras>,
    expected_protocol: Protocol,
    proxy_name: String,
    headers: &HeaderMap,
) -> Result<DispatchContext, Box<Response>> {
    let registration = match state.table.get(&proxy_name) {
        Some(entry) => entry.clone(),
        None => {
            return Err(Box::new(
                DispatchError::UnknownProxy { name: proxy_name }.into_response(),
            ));
        }
    };

    if !headers_satisfy(headers, &registration.config.headers) {
        record_headers_rejected_activity(
            &registration.flow_name,
            &registration.config.name,
            registration.task_id,
            registration.task_type,
        );
        return Err(Box::new(
            DispatchError::ProxyNotVisible { name: proxy_name }.into_response(),
        ));
    }

    if registration.protocol != expected_protocol {
        return Err(Box::new(
            DispatchError::WrongProtocolForUrl { name: proxy_name }.into_response(),
        ));
    }

    if registration.cancellation_token.is_cancelled() {
        return Err(Box::new(StatusCode::SERVICE_UNAVAILABLE.into_response()));
    }

    if let Err(e) = validate_endpoint_auth(registration.credentials.as_ref(), headers) {
        return Err(Box::new(e.into_response()));
    }

    let user_context = match validate_user_auth(&registration, headers).await {
        Ok(ctx) => ctx,
        Err(e) => return Err(Box::new(e.into_response())),
    };

    Ok(DispatchContext {
        registration,
        user_context,
    })
}

/// Split a request `model` field into `(proxy_name, downstream_model)`.
///
/// `proxy/model` yields the proxy and `Some(model)`. A bare `proxy` (no `/`)
/// yields the proxy and `None` — the client did not pick a model and is
/// leaving it to the proxy's routing script. Whether a bare id is allowed
/// depends on the resolved proxy (see [`require_model_choice`]); the split
/// itself only rejects an empty field or a malformed suffix.
fn split_model(model: &str) -> Result<(String, Option<String>), DispatchError> {
    if model.is_empty() {
        return Err(DispatchError::MissingModelField);
    }
    match model.split_once('/') {
        // Trailing slash / empty model portion is a malformed suffix, not a
        // bare id — reject rather than silently treating it as "no model".
        Some((name, rest)) if !name.is_empty() && !rest.is_empty() => {
            Ok((name.to_string(), Some(rest.to_string())))
        }
        Some(_) => Err(DispatchError::MissingProxyPrefix),
        None => Ok((model.to_string(), None)),
    }
}

/// Enforce the bare-id contract once the proxy is known: a proxy that
/// advertises a `models` menu requires the client to pick one, so a bare id
/// is wrong intent and rejected. A proxy with no `models` accepts a bare id
/// and lets its routing script choose the downstream model. Returns the error
/// response to short-circuit with, or `None` when the request is allowed.
fn require_model_choice(
    registration: &LlmProxyRegistration,
    downstream_model: &Option<String>,
) -> Option<Response> {
    if downstream_model.is_none() && !registration.config.models.is_empty() {
        return Some(
            DispatchError::ModelChoiceRequired {
                name: registration.config.name.clone(),
            }
            .into_response(),
        );
    }
    None
}

/// Generic dispatch entry point. Serialises the `EventPayload` into
/// the pipeline event and forwards to blocking / streaming based on
/// the client's `stream` flag.
async fn dispatch<A: ProtocolAdapter>(
    ctx: DispatchContext,
    payload: &EventPayload<'_>,
    is_stream: bool,
    include_usage: bool,
) -> Response {
    // The model the client picked (`None` for a bare proxy id) — owned here for
    // the observability meta and the response echo.
    let downstream_model = payload.model.map(str::to_string);
    let data = match serde_json::to_value(payload) {
        Ok(v) => v,
        Err(e) => {
            error!(error = %e, "Failed to serialize AI gateway event payload");
            return DispatchError::PayloadSerialization { source: e }.into_response();
        }
    };

    let mut meta = serde_json::Map::new();
    if let Some(ref uctx) = ctx.user_context {
        if let Ok(value) = serde_json::to_value(uctx) {
            meta.insert(flowgen_core::auth::AUTH.to_string(), value);
        }
    }

    // Flatten request-side observability fields onto event.meta so
    // downstream tasks (Rhai budget guards, bigquery_write archiving,
    // NATS billing events) can read them without re-parsing the body.
    let gateway_ctx = crate::meta::GatewayContext {
        protocol: A::PROTOCOL_NAME,
        proxy_name: ctx.registration.config.name.clone(),
        requested_model: downstream_model.clone(),
        stream: is_stream,
        user_id: ctx.user_context.as_ref().map(|u| u.user_id.clone()),
    };
    gateway_ctx.insert_into(&mut meta);

    // Echo the client's model back in the response. For a bare proxy id the
    // client picked none, so fall back to the proxy name until the response
    // reflects the script-resolved model (tracked separately).
    let response_model = downstream_model.unwrap_or_else(|| ctx.registration.config.name.clone());
    let request = DispatchRequest {
        data,
        meta,
        model: response_model,
        request_id: format!("{}-{}", A::REQUEST_ID_PREFIX, uuid::Uuid::now_v7()),
        created: chrono::Utc::now().timestamp(),
        include_usage,
        gateway_ctx: &gateway_ctx,
    };

    let run_span = tracing::info_span!(
        "task.run",
        flow = %ctx.registration.flow_name,
        task = %ctx.registration.config.name,
        task_id = ctx.registration.task_id,
        task_type = %ctx.registration.task_type,
    );
    let handle_span = tracing::info_span!(
        parent: &run_span,
        "task.handle",
        duration_ms = tracing::field::Empty,
    );

    let result = if is_stream {
        dispatch_streaming::<A>(&ctx.registration, request)
            .instrument(handle_span)
            .await
    } else {
        dispatch_blocking::<A>(&ctx.registration, request)
            .instrument(handle_span)
            .await
    };

    match result {
        Ok(response) => response,
        Err(e) => {
            error!(error = %e, "AI gateway dispatch failed");
            e.into_response()
        }
    }
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

/// Validates endpoint-level credentials (bearer/basic) on the request.
fn validate_endpoint_auth(
    credentials: Option<&HttpCredentials>,
    headers: &HeaderMap,
) -> Result<(), DispatchError> {
    let credentials = match credentials {
        Some(creds) => creds,
        None => return Ok(()),
    };

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .ok_or(DispatchError::NoCredentials)?;

    let auth_value = auth_header
        .to_str()
        .map_err(|_| DispatchError::MalformedCredentials)?;

    if let Some(expected_token) = &credentials.bearer_auth {
        match extract_bearer_token(auth_value) {
            Some(token) if token == expected_token => return Ok(()),
            Some(_) => return Err(DispatchError::InvalidCredentials),
            None => {}
        }
    }

    if let Some(basic_auth) = &credentials.basic_auth {
        if let Some(encoded) = auth_value.strip_prefix("Basic ") {
            let decoded_bytes = base64::engine::general_purpose::STANDARD
                .decode(encoded)
                .map_err(|_| DispatchError::MalformedCredentials)?;
            let decoded_str = String::from_utf8(decoded_bytes)
                .map_err(|_| DispatchError::MalformedCredentials)?;
            let expected = format!("{}:{}", basic_auth.username, basic_auth.password);
            return if decoded_str == expected {
                Ok(())
            } else {
                Err(DispatchError::InvalidCredentials)
            };
        }
    }

    Err(DispatchError::InvalidCredentials)
}

/// Validates user-level auth via the worker auth provider when
/// `config.auth.required` is true.
async fn validate_user_auth(
    registration: &LlmProxyRegistration,
    headers: &HeaderMap,
) -> Result<Option<flowgen_core::auth::UserContext>, DispatchError> {
    match &registration.config.auth {
        Some(config) if config.required => {}
        _ => return Ok(None),
    }

    let provider = registration
        .auth_provider
        .as_ref()
        .ok_or(DispatchError::AuthProviderMissing)?;

    let auth_header = headers
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .ok_or(DispatchError::NoCredentials)?;

    let token = extract_bearer_token(auth_header).ok_or(DispatchError::MalformedCredentials)?;

    provider
        .validate(token)
        .await
        .map(Some)
        .map_err(|_| DispatchError::InvalidCredentials)
}

// ---------------------------------------------------------------------------
// Dispatch implementations
// ---------------------------------------------------------------------------

/// Non-streaming dispatch: send event, await leaf completion, hand
/// off to the adapter to shape the wire response.
async fn dispatch_blocking<A: ProtocolAdapter>(
    registration: &LlmProxyRegistration,
    request: DispatchRequest<'_>,
) -> Result<Response, DispatchError> {
    let (completion_state, completion_rx) = new_completion_channel(registration.leaf_count);

    let mut builder = EventBuilder::new()
        .data(EventData::Json(request.data))
        .subject(registration.config.name.to_owned())
        .task_id(registration.task_id)
        .task_type(registration.task_type)
        .completion_tx(completion_state);

    if !request.meta.is_empty() {
        builder = builder.meta(request.meta);
    }

    let e = builder
        .build()
        .map_err(|source| DispatchError::EventBuilder { source })?;

    let logger = e
        .send_with_logging(Some(&registration.tx))
        .context(meta::PROTOCOL, request.gateway_ctx.protocol)
        .context(meta::PROXY_NAME, &request.gateway_ctx.proxy_name)
        .context(meta::STREAM, request.gateway_ctx.stream);
    let logger = match &request.gateway_ctx.requested_model {
        Some(m) => logger.context(meta::REQUESTED_MODEL, m),
        None => logger,
    };
    let logger = match &request.gateway_ctx.user_id {
        Some(id) => logger.context(meta::USER_ID, id),
        None => logger,
    };
    logger
        .await
        .map_err(|source| DispatchError::SendMessage { source })?;

    let result = match registration.config.ack_timeout {
        Some(timeout) => tokio::time::timeout(timeout, completion_rx)
            .await
            .map_err(|_| DispatchError::FlowCompletionFailed)?
            .map_err(|_| DispatchError::FlowCompletionFailed)?,
        None => completion_rx
            .await
            .map_err(|_| DispatchError::FlowCompletionFailed)?,
    };

    let completion_data = result.map_err(|e| DispatchError::FlowError {
        message: e.to_string(),
    })?;

    let (text, tool_calls, usage) = unpack_completion(completion_data.as_ref())?;

    Ok(A::build_blocking_response(
        request.request_id,
        request.model,
        request.created,
        text,
        tool_calls,
        usage,
    ))
}

/// Pull `text`, `tool_calls`, and `usage` out of the leaf's completion
/// event. Wire shape is fixed by `CompletionResponse` and predates the
/// protocol split, so both adapters read the same fields.
fn unpack_completion(
    data: Option<&serde_json::Value>,
) -> Result<
    (
        String,
        Vec<crate::ai_gateway::config::ToolCall>,
        Option<crate::ai_gateway::config::Usage>,
    ),
    DispatchError,
> {
    // Downstream serialises `CompletionResponse` — read `text` for the
    // completion string and `tool_calls` for passthrough. Fall back to
    // `content` for legacy leaf tasks that emit their own shape.
    let text = match data {
        Some(v) => match v.get("text").or_else(|| v.get("content")) {
            Some(c) => match c.as_str() {
                Some(s) => s.to_string(),
                None => String::new(),
            },
            None => String::new(),
        },
        None => String::new(),
    };
    let tool_calls: Vec<crate::ai_gateway::config::ToolCall> =
        match data.and_then(|v| v.get("tool_calls")) {
            Some(v) if !v.is_null() => serde_json::from_value(v.clone())
                .map_err(|source| DispatchError::MalformedToolCalls { source })?,
            _ => Vec::new(),
        };

    // Malformed `usage` is logged and dropped rather than propagated
    // because token accounting is best-effort — a bad shape must not
    // fail an otherwise successful completion.
    let usage = match data.and_then(|v| v.get("usage")).filter(|v| !v.is_null()) {
        None => None,
        Some(v) => match serde_json::from_value::<crate::ai_gateway::config::Usage>(v.clone()) {
            Ok(u) => Some(u),
            Err(e) => {
                warn!(error = %e, "Malformed usage in downstream completion event; dropping.");
                None
            }
        },
    };

    Ok((text, tool_calls, usage))
}

/// Streaming dispatch: open SSE response stream backed by the response
/// registry, driving the adapter's `StreamWriter` from the leaf's
/// progress events and terminal completion. Wire format is entirely
/// owned by the adapter — this function only sequences state.
async fn dispatch_streaming<A: ProtocolAdapter>(
    registration: &LlmProxyRegistration,
    request: DispatchRequest<'_>,
) -> Result<Response, DispatchError> {
    let DispatchRequest {
        data,
        mut meta,
        model,
        request_id,
        created,
        include_usage,
        gateway_ctx,
    } = request;

    let correlation_id = uuid::Uuid::now_v7().to_string();

    let (progress_tx, mut progress_rx) = mpsc::channel::<ProgressEvent>(32);

    registration
        .response_registry
        .insert(
            correlation_id.clone(),
            ResponseSender {
                progress_tx,
                result_tx: None,
            },
        )
        .await;

    meta.insert(
        flowgen_core::registry::CORRELATION_ID.to_string(),
        serde_json::Value::String(correlation_id.clone()),
    );

    let (completion_state_tx, completion_rx) = new_completion_channel(registration.leaf_count);

    let e = EventBuilder::new()
        .data(EventData::Json(data))
        .subject(registration.config.name.to_owned())
        .task_id(registration.task_id)
        .task_type(registration.task_type)
        .meta(meta)
        .completion_tx(completion_state_tx)
        .build()
        .map_err(|source| DispatchError::EventBuilder { source })?;

    let logger = e
        .send_with_logging(Some(&registration.tx))
        .context(meta::PROTOCOL, gateway_ctx.protocol)
        .context(meta::PROXY_NAME, &gateway_ctx.proxy_name)
        .context(meta::STREAM, gateway_ctx.stream);
    let logger = match &gateway_ctx.requested_model {
        Some(m) => logger.context(meta::REQUESTED_MODEL, m),
        None => logger,
    };
    let logger = match &gateway_ctx.user_id {
        Some(id) => logger.context(meta::USER_ID, id),
        None => logger,
    };
    logger
        .await
        .map_err(|source| DispatchError::SendMessage { source })?;

    let registry = Arc::clone(&registration.response_registry);
    let cid = correlation_id.clone();
    let ack_timeout = registration.config.ack_timeout;
    let (sse_tx, sse_rx) = mpsc::channel::<Result<String, std::convert::Infallible>>(32);

    tokio::spawn(async move {
        let mut writer = A::new_stream_writer(request_id, model, created);

        for frame in writer.open() {
            if sse_tx.send(Ok(frame)).await.is_err() {
                return;
            }
        }

        tokio::pin!(completion_rx);

        type CompletionResult = Result<
            Result<Option<serde_json::Value>, Box<dyn std::error::Error + Send + Sync>>,
            tokio::sync::oneshot::error::RecvError,
        >;

        // When the downstream streams, its per-token progress deltas already
        // carry the full text; the final event then repeats it as one chunk.
        // Track whether any delta was sent so the final text isn't streamed
        // twice. Non-streaming downstreams send no progress, so their final
        // text is emitted below.
        let mut streamed_progress = false;

        let result: Option<CompletionResult> = loop {
            tokio::select! {
                progress = progress_rx.recv() => {
                    match progress {
                        Some(evt) => {
                            // A tool step isn't text, so it must not suppress the
                            // final text emission (which is gated on
                            // `streamed_progress`).
                            let frames = match evt.tool {
                                Some(step) => writer.tool_step(step.name, step.arguments),
                                None => {
                                    streamed_progress = true;
                                    writer.text_delta(evt.status.clone())
                                }
                            };
                            for frame in frames {
                                if sse_tx.send(Ok(frame)).await.is_err() {
                                    registry.remove(&cid).await;
                                    return;
                                }
                            }
                        }
                        None => break None,
                    }
                }
                completion = async {
                    match ack_timeout {
                        Some(timeout) => {
                            match tokio::time::timeout(timeout, &mut completion_rx).await {
                                Ok(r) => Some(r),
                                Err(_) => { registry.remove(&cid).await; None }
                            }
                        }
                        None => Some((&mut completion_rx).await),
                    }
                } => {
                    registry.remove(&cid).await;

                    while let Ok(evt) = progress_rx.try_recv() {
                        let frames = match evt.tool {
                            Some(step) => writer.tool_step(step.name, step.arguments),
                            None => {
                                streamed_progress = true;
                                writer.text_delta(evt.status.clone())
                            }
                        };
                        for frame in frames {
                            let _ = sse_tx.send(Ok(frame)).await;
                        }
                    }

                    match completion {
                        Some(r) => break Some(r),
                        None => return,
                    }
                }
            }
        };

        let mut final_usage: Option<crate::ai_gateway::config::Usage> = None;
        let mut stop = StopReason::End;
        match &result {
            Some(Ok(Ok(Some(data)))) => {
                // Legacy shape uses `content`; `CompletionChunk` uses
                // `text`. Read either so old and new emitters coexist.
                let text = match data.get("text").or_else(|| data.get("content")) {
                    Some(c) => match c.as_str() {
                        Some(s) => s.to_string(),
                        None => String::new(),
                    },
                    None => String::new(),
                };
                // Skip when progress deltas already streamed this text, or it
                // would arrive twice. Non-streaming downstreams sent no deltas,
                // so their final text is emitted here.
                if !text.is_empty() && !streamed_progress {
                    for frame in writer.text_delta(text) {
                        let _ = sse_tx.send(Ok(frame)).await;
                    }
                }
                let tool_calls: Vec<crate::ai_gateway::config::ToolCall> =
                    match data.get("tool_calls") {
                        Some(v) if !v.is_null() => match serde_json::from_value(v.clone()) {
                            Ok(list) => list,
                            Err(e) => {
                                error!(error = %e, "Malformed tool_calls in downstream event");
                                Vec::new()
                            }
                        },
                        _ => Vec::new(),
                    };
                if !tool_calls.is_empty() {
                    stop = StopReason::ToolUse;
                    for frame in writer.tool_calls(tool_calls) {
                        let _ = sse_tx.send(Ok(frame)).await;
                    }
                }
                // Usage lives on the completion processor's final chunk.
                // Only newer emitters populate it; older ones fall through.
                // Malformed shapes are logged and dropped rather than
                // aborting the stream — token accounting is best-effort.
                final_usage = match data.get("usage").filter(|v| !v.is_null()) {
                    None => None,
                    Some(v) => {
                        match serde_json::from_value::<crate::ai_gateway::config::Usage>(v.clone())
                        {
                            Ok(u) => Some(u),
                            Err(e) => {
                                warn!(error = %e, "Malformed usage in downstream stream event; dropping.");
                                None
                            }
                        }
                    }
                };
            }
            Some(Ok(Err(e))) => {
                // Surface leaf-task failure as an SSE error frame in the
                // adapter's wire shape so the client sees the cause, not an
                // empty stream.
                let frame = A::error_sse_frame(&e.to_string());
                if !frame.is_empty() {
                    let _ = sse_tx.send(Ok(frame)).await;
                }
            }
            _ => {}
        }

        for frame in writer.close(stop, final_usage, include_usage) {
            let _ = sse_tx.send(Ok(frame)).await;
        }

        if A::EMIT_DONE_SENTINEL {
            let _ = sse_tx.send(Ok(SSE_DONE.to_string())).await;
        }
    });

    let stream = ReceiverStream::new(sse_rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream))
        .map_err(|_| DispatchError::FlowCompletionFailed)
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors raised by the dispatcher path. Implements `IntoResponse` for HTTP
/// status mapping.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
enum DispatchError {
    #[error("Request body is missing the required 'model' field")]
    MissingModelField,
    #[error("Request 'model' field must be of the form '<proxy-name>/<downstream-model>'")]
    MissingProxyPrefix,
    #[error("LLM proxy '{name}' advertises models; the request must pick one as '{name}/<model>'")]
    ModelChoiceRequired { name: String },
    #[error("Unknown LLM proxy '{name}'")]
    UnknownProxy { name: String },
    /// A request missing one of a proxy's required `headers` gets the same
    /// 404 as `UnknownProxy` — the proxy must not be distinguishable from
    /// one that does not exist.
    #[error("Unknown LLM proxy '{name}'")]
    ProxyNotVisible { name: String },
    #[error("LLM proxy '{name}' does not speak the protocol expected at this URL")]
    WrongProtocolForUrl { name: String },
    #[error("No authorization header provided")]
    NoCredentials,
    #[error("Invalid authorization credentials")]
    InvalidCredentials,
    #[error("Malformed authorization header")]
    MalformedCredentials,
    #[error("Auth provider not configured but auth.required is true")]
    AuthProviderMissing,
    #[error("Failed to build pipeline event: {source}")]
    EventBuilder {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Failed to send pipeline event: {source}")]
    SendMessage {
        #[source]
        source: flowgen_core::event::Error,
    },
    #[error("Flow completion failed or timed out")]
    FlowCompletionFailed,
    #[error("Flow error: {message}")]
    FlowError { message: String },
    #[error("Downstream tool_calls payload is malformed: {source}")]
    MalformedToolCalls {
        #[source]
        source: serde_json::Error,
    },
    #[error("Failed to serialize gateway event payload: {source}")]
    PayloadSerialization {
        #[source]
        source: serde_json::Error,
    },
}

/// OpenAI-compatible error response body.
#[derive(serde::Serialize)]
struct OpenAiErrorResponse {
    error: OpenAiErrorDetail,
}

/// Inner detail of an OpenAI-compatible error response.
#[derive(serde::Serialize)]
struct OpenAiErrorDetail {
    message: String,
    #[serde(rename = "type")]
    error_type: &'static str,
}

impl DispatchError {
    /// Returns the OpenAI error type string for this error variant.
    fn error_type(&self) -> &'static str {
        match self {
            DispatchError::MissingModelField
            | DispatchError::MissingProxyPrefix
            | DispatchError::ModelChoiceRequired { .. } => "invalid_request_error",
            DispatchError::UnknownProxy { .. }
            | DispatchError::ProxyNotVisible { .. }
            | DispatchError::WrongProtocolForUrl { .. } => "not_found_error",
            DispatchError::NoCredentials
            | DispatchError::InvalidCredentials
            | DispatchError::MalformedCredentials
            | DispatchError::AuthProviderMissing => "authentication_error",
            DispatchError::EventBuilder { .. }
            | DispatchError::SendMessage { .. }
            | DispatchError::FlowCompletionFailed
            | DispatchError::FlowError { .. }
            | DispatchError::MalformedToolCalls { .. }
            | DispatchError::PayloadSerialization { .. } => "server_error",
        }
    }
}

impl IntoResponse for DispatchError {
    fn into_response(self) -> Response {
        let status = match &self {
            DispatchError::MissingModelField
            | DispatchError::MissingProxyPrefix
            | DispatchError::ModelChoiceRequired { .. } => StatusCode::BAD_REQUEST,
            DispatchError::UnknownProxy { .. }
            | DispatchError::ProxyNotVisible { .. }
            | DispatchError::WrongProtocolForUrl { .. } => StatusCode::NOT_FOUND,
            DispatchError::NoCredentials
            | DispatchError::InvalidCredentials
            | DispatchError::MalformedCredentials
            | DispatchError::AuthProviderMissing => StatusCode::UNAUTHORIZED,
            DispatchError::EventBuilder { .. }
            | DispatchError::SendMessage { .. }
            | DispatchError::FlowCompletionFailed
            | DispatchError::MalformedToolCalls { .. }
            | DispatchError::PayloadSerialization { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            DispatchError::FlowError { .. } => StatusCode::BAD_GATEWAY,
        };
        let body = OpenAiErrorResponse {
            error: OpenAiErrorDetail {
                message: self.to_string(),
                error_type: self.error_type(),
            },
        };
        (status, axum::Json(body)).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_AI_GATEWAY_PORT, 3002);
        assert_eq!(DEFAULT_AI_GATEWAY_PATH, "/v1");
    }

    #[test]
    fn test_dispatch_error_status_codes() {
        assert_eq!(
            DispatchError::MissingModelField.into_response().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            DispatchError::MissingProxyPrefix.into_response().status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            DispatchError::UnknownProxy {
                name: "x".to_string()
            }
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DispatchError::WrongProtocolForUrl {
                name: "x".to_string()
            }
            .into_response()
            .status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            DispatchError::NoCredentials.into_response().status(),
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            DispatchError::FlowCompletionFailed.into_response().status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            DispatchError::ModelChoiceRequired {
                name: "x".to_string()
            }
            .into_response()
            .status(),
            StatusCode::BAD_REQUEST
        );
    }

    #[test]
    fn split_model_parses_suffix_bare_and_rejects_malformed() {
        // `proxy/model` -> proxy + explicit model.
        assert_eq!(
            split_model("flowgen_openai/glm-4.6").unwrap(),
            ("flowgen_openai".to_string(), Some("glm-4.6".to_string()))
        );
        // Nested model path is kept whole after the first `/`.
        assert_eq!(
            split_model("proxy/vendor/model").unwrap(),
            ("proxy".to_string(), Some("vendor/model".to_string()))
        );
        // Bare proxy id -> no model, script decides.
        assert_eq!(
            split_model("flowgen_openai").unwrap(),
            ("flowgen_openai".to_string(), None)
        );
        // Empty field, empty proxy, and trailing-slash suffix are rejected.
        assert!(matches!(
            split_model(""),
            Err(DispatchError::MissingModelField)
        ));
        assert!(matches!(
            split_model("/glm-4.6"),
            Err(DispatchError::MissingProxyPrefix)
        ));
        assert!(matches!(
            split_model("flowgen_openai/"),
            Err(DispatchError::MissingProxyPrefix)
        ));
    }
}
