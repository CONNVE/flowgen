//! Integration tests for the AI-gateway dispatch layer.
//!
//! Boots a real `AiGatewayServer` on an ephemeral TCP port, registers a
//! fake `llm_proxy` whose "downstream" is an inline task that signals
//! `completion_tx` with a synthetic `CompletionResponse` (or
//! `CompletionChunk` for streaming), and drives the server with
//! `reqwest` as an OpenAI-compatible client would. Covers routing paths
//! unit tests do not reach:
//!
//! - `dispatch_blocking`: leaf JSON `usage` → `ChatCompletionResponse.usage`.
//! - `dispatch_streaming` with `stream_options.include_usage: true`:
//!   usage frame emitted before the terminating `[DONE]`.
//! - `dispatch_streaming` without `stream_options`: no usage frame,
//!   byte-for-byte compatible with clients that never opt in.
//! - Malformed leaf `usage`: logged and dropped, stream still completes.
//!
//! Runs in `cargo test` without `#[ignore]` because no external daemon
//! is required.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use flowgen_ai_agent::ai_gateway::config::{Processor as GatewayConfig, Protocol};
use flowgen_ai_agent::ai_gateway::server::{
    AiGatewayExtras, AiGatewayServer, LlmProxyRegistration,
};
use flowgen_core::credentials::HttpCredentials;
use flowgen_core::event::Event;
use flowgen_core::http_server::HttpServer;
use flowgen_core::registry::ResponseRegistry;
use futures_util::StreamExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

/// Spawns an `AiGatewayServer` on an ephemeral port and returns the
/// server handle, base URL for the `/chat/completions` endpoint, and
/// the axum listener JoinHandle. Dropping the JoinHandle tears the
/// server down.
async fn boot_server() -> (Arc<AiGatewayServer>, String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");
    drop(listener);

    let server = Arc::new(HttpServer::new_with_extras(
        "/v1".to_string(),
        AiGatewayExtras::default(),
    ));

    let server_clone = Arc::clone(&server);
    let handle = tokio::spawn(async move {
        let _ = server_clone.start_server(addr.port()).await;
    });

    let base_url = format!("http://{}/v1/chat/completions", addr);
    let client = reqwest::Client::new();
    for _ in 0..50 {
        // We probe with a malformed request so a missing route surfaces
        // as connection error and a live route as a 4xx JSON body.
        let probe = client.post(&base_url).json(&json!({})).send().await;
        if probe.is_ok() {
            return (server, base_url, handle);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("AI gateway did not come up on {addr}");
}

/// Registers a fake `llm_proxy` and spawns a background task that
/// mimics a downstream leaf: whenever the gateway forwards a request
/// event, the closure is called with that event and expected to signal
/// `completion_tx` with the JSON body the client should observe.
fn register_fake_proxy<F>(server: &AiGatewayServer, name: &str, respond: F)
where
    F: FnMut(&Event) -> Value + Send + 'static,
{
    register_fake_proxy_with_protocol(server, name, Protocol::Openai, respond);
}

/// Variant of `register_fake_proxy` that also takes the wire protocol,
/// so tests can register Anthropic-shaped `llm_proxy` backends.
fn register_fake_proxy_with_protocol<F>(
    server: &AiGatewayServer,
    name: &str,
    protocol: Protocol,
    mut respond: F,
) where
    F: FnMut(&Event) -> Value + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Event>(4);

    let config = Arc::new(GatewayConfig {
        name: name.to_string(),
        models: Vec::new(),
        headers: HashMap::new(),
        protocol,
        credentials_path: None,
        auth: None,
        ack_timeout: Some(Duration::from_secs(5)),
        depends_on: None,
        retry: None,
    });

    let registration = LlmProxyRegistration {
        flow_name: "test_flow".to_string(),
        display_name: None,
        description: None,
        protocol,
        config,
        credentials: None,
        auth_provider: None,
        tx,
        task_id: 0,
        task_type: "llm_proxy",
        response_registry: Arc::new(ResponseRegistry::new()),
        leaf_count: 1,
        cancellation_token: CancellationToken::new(),
    };
    server.register(name.to_string(), registration);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let payload = respond(&event);
            if let Some(completion_tx) = event.completion_tx.as_ref() {
                completion_tx.signal_completion(Some(payload));
            }
        }
    });
}

/// Variant of `register_fake_proxy` that advertises a `models` menu, so tests
/// can exercise the "listed models require the client to pick one" contract.
fn register_fake_proxy_with_models<F>(
    server: &AiGatewayServer,
    name: &str,
    models: Vec<String>,
    mut respond: F,
) where
    F: FnMut(&Event) -> Value + Send + 'static,
{
    let (tx, mut rx) = mpsc::channel::<Event>(4);

    let config = Arc::new(GatewayConfig {
        name: name.to_string(),
        models,
        headers: HashMap::new(),
        protocol: Protocol::Openai,
        credentials_path: None,
        auth: None,
        ack_timeout: Some(Duration::from_secs(5)),
        depends_on: None,
        retry: None,
    });

    let registration = LlmProxyRegistration {
        flow_name: "test_flow".to_string(),
        display_name: None,
        description: None,
        protocol: Protocol::Openai,
        config,
        credentials: None,
        auth_provider: None,
        tx,
        task_id: 0,
        task_type: "llm_proxy",
        response_registry: Arc::new(ResponseRegistry::new()),
        leaf_count: 1,
        cancellation_token: CancellationToken::new(),
    };
    server.register(name.to_string(), registration);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            let payload = respond(&event);
            if let Some(completion_tx) = event.completion_tx.as_ref() {
                completion_tx.signal_completion(Some(payload));
            }
        }
    });
}

/// Registers a proxy scoped to a `headers` requirement, so tests can
/// exercise the exclusivity filter on `GET /models` and dispatch.
fn register_fake_proxy_with_headers(
    server: &AiGatewayServer,
    name: &str,
    headers: HashMap<String, String>,
) {
    let (tx, mut rx) = mpsc::channel::<Event>(4);

    let config = Arc::new(GatewayConfig {
        name: name.to_string(),
        models: Vec::new(),
        headers,
        protocol: Protocol::Openai,
        credentials_path: None,
        auth: None,
        ack_timeout: Some(Duration::from_secs(5)),
        depends_on: None,
        retry: None,
    });

    let registration = LlmProxyRegistration {
        flow_name: "test_flow".to_string(),
        display_name: None,
        description: None,
        protocol: Protocol::Openai,
        config,
        credentials: None,
        auth_provider: None,
        tx,
        task_id: 0,
        task_type: "llm_proxy",
        response_registry: Arc::new(ResponseRegistry::new()),
        leaf_count: 1,
        cancellation_token: CancellationToken::new(),
    };
    server.register(name.to_string(), registration);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Some(completion_tx) = event.completion_tx.as_ref() {
                completion_tx.signal_completion(Some(json!({"text": "unused"})));
            }
        }
    });
}

/// Convert the chat-completions URL returned by `boot_server` into the
/// Anthropic messages URL.
fn messages_url(chat_url: &str) -> String {
    chat_url.replace("/chat/completions", "/messages")
}

/// Sends a POST as a real client would and returns the parsed JSON
/// body. Used for non-streaming requests only.
async fn post_json(client: &reqwest::Client, url: &str, body: Value) -> Value {
    let resp = client
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("post to gateway");
    assert_eq!(resp.status(), 200, "gateway returned non-200");
    resp.json::<Value>().await.expect("parse JSON body")
}

/// Consumes an SSE stream and returns each `data: <payload>` line's
/// payload as a String. The terminating `[DONE]` sentinel is included.
async fn collect_sse(url: &str, body: Value) -> Vec<String> {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("open streaming POST");
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    let mut buffer = String::new();
    let mut frames: Vec<String> = Vec::new();
    let deadline = Duration::from_secs(5);

    // Read chunks until we see `[DONE]` or the stream ends.
    loop {
        let chunk = match timeout(deadline, stream.next()).await {
            Ok(Some(Ok(bytes))) => bytes,
            Ok(Some(Err(e))) => panic!("SSE stream error: {e}"),
            Ok(None) => break,
            Err(_) => panic!("SSE stream stalled without [DONE]"),
        };
        buffer.push_str(std::str::from_utf8(&chunk).expect("utf-8 SSE"));

        while let Some(end) = buffer.find("\n\n") {
            let event = buffer[..end].to_string();
            buffer = buffer[end + 2..].to_string();
            for line in event.lines() {
                if let Some(payload) = line.strip_prefix("data: ") {
                    frames.push(payload.to_string());
                }
            }
        }
        if frames.iter().any(|f| f == "[DONE]") {
            break;
        }
    }
    frames
}

// ---------------------------------------------------------------------------
// dispatch_blocking
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_blocking_forwards_usage_from_leaf_to_response() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| {
        json!({
            "text": "hello",
            "usage": {
                "prompt_tokens": 42,
                "completion_tokens": 7,
                "total_tokens": 49,
            }
        })
    });

    let resp = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}]
        }),
    )
    .await;

    assert_eq!(resp["usage"]["prompt_tokens"], 42);
    assert_eq!(resp["usage"]["completion_tokens"], 7);
    assert_eq!(resp["usage"]["total_tokens"], 49);
    assert_eq!(resp["choices"][0]["message"]["content"], "hello");
}

#[tokio::test]
async fn dispatch_blocking_omits_usage_when_leaf_does_not_report_it() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| json!({"text":"hello"}));

    let resp = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}]
        }),
    )
    .await;

    assert!(
        resp.get("usage").map(|v| v.is_null()).unwrap_or(true),
        "response must omit `usage` when leaf did not populate it, got {resp}"
    );
}

#[tokio::test]
async fn dispatch_blocking_drops_malformed_usage_and_still_returns_response() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| {
        json!({
            "text": "hello",
            "usage": {"prompt_tokens": "not-a-number"}
        })
    });

    let resp = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}]
        }),
    )
    .await;

    assert_eq!(resp["choices"][0]["message"]["content"], "hello");
    assert!(
        resp.get("usage").map(|v| v.is_null()).unwrap_or(true),
        "malformed usage must be dropped, not propagated as null-shape"
    );
}

// ---------------------------------------------------------------------------
// dispatch_streaming
// ---------------------------------------------------------------------------

#[tokio::test]
async fn dispatch_streaming_emits_usage_frame_when_client_opts_in() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| {
        json!({
            "text": "streamed",
            "is_final": true,
            "index": 0,
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 200,
                "total_tokens": 300,
            }
        })
    });

    let frames = collect_sse(
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}],
            "stream": true,
            "stream_options": {"include_usage": true}
        }),
    )
    .await;

    let done_idx = frames
        .iter()
        .position(|f| f == "[DONE]")
        .expect("DONE present");
    let usage_frame = frames[..done_idx]
        .iter()
        .rev()
        .find_map(|f| serde_json::from_str::<Value>(f).ok())
        .filter(|v| v.get("usage").is_some())
        .expect("usage frame emitted before [DONE]");
    assert!(
        usage_frame["choices"].as_array().unwrap().is_empty(),
        "usage frame carries empty choices per OpenAI wire spec"
    );
    assert_eq!(usage_frame["usage"]["prompt_tokens"], 100);
    assert_eq!(usage_frame["usage"]["completion_tokens"], 200);
    assert_eq!(usage_frame["usage"]["total_tokens"], 300);
}

#[tokio::test]
async fn dispatch_streaming_omits_usage_frame_when_client_does_not_opt_in() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| {
        json!({
            "text": "streamed",
            "is_final": true,
            "index": 0,
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 200,
                "total_tokens": 300,
            }
        })
    });

    let frames = collect_sse(
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}],
            "stream": true
        }),
    )
    .await;

    assert!(
        frames.iter().position(|f| f == "[DONE]").is_some(),
        "stream must terminate with [DONE]"
    );
    // No frame before [DONE] should carry a `usage` key — byte-compat
    // with clients that never expect the extra frame.
    for frame in frames.iter().filter(|f| f.as_str() != "[DONE]") {
        let v: Value = serde_json::from_str(frame).unwrap();
        assert!(
            v.get("usage").map(|u| u.is_null()).unwrap_or(true),
            "no chunk may carry usage without opt-in, got {v}"
        );
    }
}

#[tokio::test]
async fn dispatch_streaming_drops_malformed_usage_and_still_terminates() {
    let (server, url, _handle) = boot_server().await;

    register_fake_proxy(&server, "gw", |_event| {
        json!({
            "text": "streamed",
            "is_final": true,
            "index": 0,
            "usage": {"prompt_tokens": "not-a-number"}
        })
    });

    let frames = collect_sse(
        &url,
        json!({
            "model": "gw/gpt-4",
            "messages": [{"role":"user","content":"hi"}],
            "stream": true,
            "stream_options": {"include_usage": true}
        }),
    )
    .await;

    assert!(
        frames.last().map(|f| f == "[DONE]").unwrap_or(false),
        "stream must still terminate with [DONE] even with malformed usage"
    );
    // No usage frame should have been emitted — malformed input silently
    // downgrades the response rather than aborting.
    for frame in frames.iter().filter(|f| f.as_str() != "[DONE]") {
        let v: Value = serde_json::from_str(frame).unwrap();
        assert!(
            v.get("usage").map(|u| u.is_null()).unwrap_or(true),
            "no usage frame must be emitted for malformed leaf shape, got {v}"
        );
    }
}

// ---------------------------------------------------------------------------
// Anthropic Messages API (`POST /v1/messages`)
// ---------------------------------------------------------------------------

/// Anthropic streams close on `event: message_stop` instead of `[DONE]`,
/// so this helper collects `event:` / `data:` pairs until it sees
/// `message_stop`.
async fn collect_anthropic_sse(url: &str, body: Value) -> Vec<(String, Value)> {
    let resp = reqwest::Client::new()
        .post(url)
        .json(&body)
        .send()
        .await
        .expect("open streaming POST");
    assert_eq!(resp.status(), 200);

    let mut stream = resp.bytes_stream();
    let mut buffer = String::new();
    let mut frames: Vec<(String, Value)> = Vec::new();
    let deadline = Duration::from_secs(5);

    loop {
        let chunk = match timeout(deadline, stream.next()).await {
            Ok(Some(Ok(bytes))) => bytes,
            Ok(Some(Err(e))) => panic!("SSE stream error: {e}"),
            Ok(None) => break,
            Err(_) => panic!("SSE stream stalled without message_stop"),
        };
        buffer.push_str(std::str::from_utf8(&chunk).expect("utf-8 SSE"));

        while let Some(end) = buffer.find("\n\n") {
            let event_block = buffer[..end].to_string();
            buffer = buffer[end + 2..].to_string();
            let mut event_type: Option<String> = None;
            let mut data_line: Option<String> = None;
            for line in event_block.lines() {
                if let Some(rest) = line.strip_prefix("event: ") {
                    event_type = Some(rest.to_string());
                } else if let Some(rest) = line.strip_prefix("data: ") {
                    data_line = Some(rest.to_string());
                }
            }
            if let (Some(ty), Some(data)) = (event_type, data_line) {
                let v: Value =
                    serde_json::from_str(&data).expect("Anthropic SSE data is valid JSON");
                frames.push((ty, v));
            }
        }
        if frames.iter().any(|(ty, _)| ty == "message_stop") {
            break;
        }
    }
    frames
}

#[tokio::test]
async fn dispatch_messages_blocking_returns_anthropic_shape() {
    let (server, chat_url, _handle) = boot_server().await;
    register_fake_proxy_with_protocol(&server, "claude", Protocol::Anthropic, |_event| {
        json!({
            "text": "hello",
            "usage": {
                "prompt_tokens": 12,
                "completion_tokens": 3,
                "total_tokens": 15,
            }
        })
    });

    let url = messages_url(&chat_url);
    let resp = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "claude/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 128
        }),
    )
    .await;

    assert_eq!(resp["type"], "message");
    assert_eq!(resp["role"], "assistant");
    assert_eq!(resp["stop_reason"], "end_turn");
    assert_eq!(resp["content"][0]["type"], "text");
    assert_eq!(resp["content"][0]["text"], "hello");
    assert_eq!(resp["usage"]["input_tokens"], 12);
    assert_eq!(resp["usage"]["output_tokens"], 3);
    assert!(resp["usage"].get("total_tokens").is_none());
    assert!(resp["usage"].get("prompt_tokens").is_none());
}

#[tokio::test]
async fn dispatch_messages_blocking_tool_use_stop_reason() {
    let (server, chat_url, _handle) = boot_server().await;
    register_fake_proxy_with_protocol(&server, "claude", Protocol::Anthropic, |_event| {
        json!({
            "text": "let me check",
            "tool_calls": [{
                "id": "call_1",
                "type": "function",
                "function": {"name": "bash", "arguments": "{\"cmd\":\"ls\"}"}
            }]
        })
    });

    let url = messages_url(&chat_url);
    let resp = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "claude/kimi",
            "messages": [{"role": "user", "content": "list files"}],
            "max_tokens": 128,
            "tools": [{
                "name": "bash",
                "description": "run a shell command",
                "input_schema": {"type": "object", "properties": {"cmd": {"type": "string"}}}
            }]
        }),
    )
    .await;

    assert_eq!(resp["stop_reason"], "tool_use");
    assert_eq!(resp["content"][0]["type"], "text");
    assert_eq!(resp["content"][1]["type"], "tool_use");
    assert_eq!(resp["content"][1]["id"], "call_1");
    assert_eq!(resp["content"][1]["name"], "bash");
    assert_eq!(resp["content"][1]["input"], json!({"cmd": "ls"}));
}

#[tokio::test]
async fn dispatch_messages_streaming_emits_message_stop_and_no_done_sentinel() {
    let (server, chat_url, _handle) = boot_server().await;
    register_fake_proxy_with_protocol(&server, "claude", Protocol::Anthropic, |_event| {
        json!({
            "text": "streamed",
            "is_final": true,
            "index": 0,
            "usage": {"prompt_tokens": 5, "completion_tokens": 2, "total_tokens": 7}
        })
    });

    let url = messages_url(&chat_url);
    let frames = collect_anthropic_sse(
        &url,
        json!({
            "model": "claude/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 128,
            "stream": true
        }),
    )
    .await;

    let event_types: Vec<&str> = frames.iter().map(|(t, _)| t.as_str()).collect();
    // Fixed sequence for a plain-text stream: opening envelope, then
    // one text block open/delta/stop, then the terminal delta+stop pair.
    assert_eq!(event_types.first().copied(), Some("message_start"));
    assert!(event_types.contains(&"content_block_start"));
    assert!(event_types.contains(&"content_block_delta"));
    assert!(event_types.contains(&"content_block_stop"));
    assert_eq!(event_types[event_types.len() - 2], "message_delta");
    assert_eq!(event_types.last().copied(), Some("message_stop"));

    // No `[DONE]` sentinel is emitted for Anthropic streams.
    for (ty, _) in &frames {
        assert_ne!(ty, "[DONE]");
    }

    // Terminal `message_delta` carries stop_reason + usage.
    let (_, delta) = frames
        .iter()
        .find(|(t, _)| t == "message_delta")
        .expect("message_delta present");
    assert_eq!(delta["delta"]["stop_reason"], "end_turn");
    assert_eq!(delta["usage"]["input_tokens"], 5);
    assert_eq!(delta["usage"]["output_tokens"], 2);
}

#[tokio::test]
async fn wrong_protocol_for_url_returns_not_found() {
    let (server, chat_url, _handle) = boot_server().await;
    // Register an Anthropic proxy and try to reach it via `/chat/completions`.
    register_fake_proxy_with_protocol(
        &server,
        "claude",
        Protocol::Anthropic,
        |_event| json!({"text": "should not be reached"}),
    );

    let resp = reqwest::Client::new()
        .post(&chat_url)
        .json(&json!({
            "model": "claude/kimi",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn openai_registration_on_messages_url_returns_not_found() {
    let (server, chat_url, _handle) = boot_server().await;
    // Default OpenAI proxy reached via Anthropic URL — protocol guard rejects.
    register_fake_proxy(&server, "gw", |_event| json!({"text": "unreachable"}));

    let url = messages_url(&chat_url);
    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "gw/gpt-4",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 8
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 404);
}

// ---------------------------------------------------------------------------
// Model listing (`GET /models`)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_models_returns_registered_proxies_in_openai_shape() {
    let (server, chat_url, _handle) = boot_server().await;
    register_fake_proxy(&server, "openai_gw", |_event| json!({"text": "unused"}));
    register_fake_proxy_with_protocol(
        &server,
        "anthropic_gw",
        Protocol::Anthropic,
        |_e| json!({"text": "unused"}),
    );

    let models_url = chat_url.replace("/chat/completions", "/models");
    let resp = reqwest::Client::new()
        .get(&models_url)
        .send()
        .await
        .expect("GET /models");
    assert_eq!(resp.status(), 200);

    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["object"], "list");
    let ids: Vec<&str> = body["data"]
        .as_array()
        .expect("data array")
        .iter()
        .map(|m| m["id"].as_str().expect("id string"))
        .collect();
    assert!(ids.contains(&"openai_gw"));
    assert!(ids.contains(&"anthropic_gw"));
    // Each entry follows OpenAI's `object: "model"` shape and carries its
    // wire protocol so clients can hide proxies they can't drive.
    for entry in body["data"].as_array().unwrap() {
        assert_eq!(entry["object"], "model");
        assert_eq!(entry["owned_by"], "flowgen");
    }
    let proto_of = |id: &str| {
        body["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["id"] == id)
            .and_then(|m| m["protocol"].as_str())
            .unwrap()
            .to_string()
    };
    assert_eq!(proto_of("openai_gw"), "openai");
    assert_eq!(proto_of("anthropic_gw"), "anthropic");
}

#[tokio::test]
async fn list_models_headers_filter_scopes_by_matching_header() {
    // `public` (empty headers) is reachable by everyone; `scoped` requires
    // `X-Flowgen-Client: flowgen-ui`. Everyone sees `public`; only a request
    // carrying the matching header also sees `scoped`.
    let (server, chat_url, _handle) = boot_server().await;
    register_fake_proxy_with_headers(&server, "public", HashMap::new());
    register_fake_proxy_with_headers(
        &server,
        "scoped",
        HashMap::from([("X-Flowgen-Client".to_string(), "flowgen-ui".to_string())]),
    );
    let models_url = chat_url.replace("/chat/completions", "/models");

    let ids_for = |client: Option<&'static str>| {
        let url = models_url.clone();
        async move {
            let mut req = reqwest::Client::new().get(&url);
            if let Some(c) = client {
                req = req.header("X-Flowgen-Client", c);
            }
            let body: Value = req
                .send()
                .await
                .expect("GET /models")
                .json()
                .await
                .expect("json");
            body["data"]
                .as_array()
                .expect("data array")
                .iter()
                .map(|m| m["id"].as_str().expect("id").to_string())
                .collect::<Vec<_>>()
        }
    };

    // Named client: sees both — public is visible to everyone, and its
    // header satisfies the scoped proxy's requirement.
    let ui = ids_for(Some("flowgen-ui")).await;
    assert!(
        ui.contains(&"scoped".to_string()),
        "flowgen-ui must see scoped"
    );
    assert!(
        ui.contains(&"public".to_string()),
        "flowgen-ui must see public too"
    );

    // Anonymous: only the public proxy — scoped requires the header.
    let anon = ids_for(None).await;
    assert!(anon.contains(&"public".to_string()), "anon must see public");
    assert!(
        !anon.contains(&"scoped".to_string()),
        "anon must NOT see scoped, got {anon:?}"
    );

    // Dispatch is blocked, not just hidden from listing: an anonymous
    // request to the scoped proxy gets the same 404 as an unknown proxy.
    let resp = reqwest::Client::new()
        .post(&chat_url)
        .json(&json!({
            "model": "scoped/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(
        resp.status(),
        404,
        "anon must not be able to dispatch to scoped"
    );
}

// ---------------------------------------------------------------------------
// Routing failure paths (`UnknownProxy`, `MissingModelField`, `MissingProxyPrefix`)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_proxy_returns_404() {
    let (_server, url, _handle) = boot_server().await;

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "does_not_exist/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 404);
}

#[tokio::test]
async fn missing_model_field_returns_400() {
    let (_server, url, _handle) = boot_server().await;

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn bare_proxy_id_routes_when_proxy_advertises_no_models() {
    // A proxy with no `models` menu accepts a bare id (no `<model>`): the
    // client left the choice to the routing script, so this is a 200, and the
    // event the leaf sees carries no `model` field.
    let (server, url, _handle) = boot_server().await;
    let seen = Arc::new(std::sync::Mutex::new(None));
    let seen_w = Arc::clone(&seen);
    register_fake_proxy(&server, "gw", move |event| {
        let data = event.data_as_json().expect("event data as json");
        *seen_w.lock().unwrap() = Some(data.get("model").cloned());
        json!({"text": "ok"})
    });

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "gw",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    // Bare id => `event.data.model` absent, so the script branches on its
    // absence rather than a sentinel value.
    let model_field = seen.lock().unwrap().clone().expect("leaf saw an event");
    assert!(
        model_field.is_none() || model_field == Some(Value::Null),
        "bare id must not set event.data.model, got {model_field:?}"
    );
}

#[tokio::test]
async fn bare_proxy_id_rejected_when_proxy_advertises_models() {
    // A proxy that advertises a `models` menu requires the client to pick one;
    // a bare id is wrong intent and rejected with 400.
    let (server, url, _handle) = boot_server().await;
    register_fake_proxy_with_models(
        &server,
        "gw",
        vec!["glm-4.6".into()],
        |_event| json!({"text": "unused"}),
    );

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "gw",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
}

#[tokio::test]
async fn malformed_model_suffix_returns_400() {
    // A trailing slash is a malformed suffix, not a bare id.
    let (server, url, _handle) = boot_server().await;
    register_fake_proxy(&server, "gw", |_event| json!({"text": "unused"}));

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "gw/",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 400);
}

// ---------------------------------------------------------------------------
// Endpoint auth (`credentials.bearer_auth`)
// ---------------------------------------------------------------------------

/// Registers a proxy with a bearer-token credentials guard so the
/// endpoint auth path (rather than the flowless `None` variant) is
/// exercised. Returns the token clients must present in `Authorization`.
fn register_proxy_with_bearer_token(
    server: &AiGatewayServer,
    name: &str,
    protocol: Protocol,
    token: &str,
) {
    let (tx, mut rx) = mpsc::channel::<Event>(4);

    let config = Arc::new(GatewayConfig {
        name: name.to_string(),
        models: Vec::new(),
        headers: HashMap::new(),
        protocol,
        credentials_path: None,
        auth: None,
        ack_timeout: Some(Duration::from_secs(5)),
        depends_on: None,
        retry: None,
    });

    let registration = LlmProxyRegistration {
        flow_name: "test_flow".to_string(),
        display_name: None,
        description: None,
        protocol,
        config,
        credentials: Some(HttpCredentials::bearer(token)),
        auth_provider: None,
        tx,
        task_id: 0,
        task_type: "llm_proxy",
        response_registry: Arc::new(ResponseRegistry::new()),
        leaf_count: 1,
        cancellation_token: CancellationToken::new(),
    };
    server.register(name.to_string(), registration);

    tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            if let Some(completion_tx) = event.completion_tx.as_ref() {
                completion_tx.signal_completion(Some(json!({"text": "authed"})));
            }
        }
    });
}

#[tokio::test]
async fn endpoint_auth_rejects_missing_authorization() {
    let (server, url, _handle) = boot_server().await;
    register_proxy_with_bearer_token(&server, "gw", Protocol::Openai, "secret-token");

    let resp = reqwest::Client::new()
        .post(&url)
        .json(&json!({
            "model": "gw/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn endpoint_auth_rejects_wrong_bearer_token() {
    let (server, url, _handle) = boot_server().await;
    register_proxy_with_bearer_token(&server, "gw", Protocol::Openai, "secret-token");

    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth("wrong-token")
        .json(&json!({
            "model": "gw/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 401);
}

#[tokio::test]
async fn endpoint_auth_accepts_correct_bearer_token() {
    let (server, url, _handle) = boot_server().await;
    register_proxy_with_bearer_token(&server, "gw", Protocol::Openai, "secret-token");

    let resp = reqwest::Client::new()
        .post(&url)
        .bearer_auth("secret-token")
        .json(&json!({
            "model": "gw/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(resp.status(), 200);
    let body: Value = resp.json().await.expect("json");
    assert_eq!(body["choices"][0]["message"]["content"], "authed");
}

// ---------------------------------------------------------------------------
// `x-api-key` folding on the Anthropic route
// ---------------------------------------------------------------------------

#[tokio::test]
async fn anthropic_x_api_key_folds_into_bearer_auth() {
    let (server, chat_url, _handle) = boot_server().await;
    // Same guard as the OpenAI auth test, but on the Anthropic protocol.
    register_proxy_with_bearer_token(&server, "claude", Protocol::Anthropic, "sk-anthropic-xxx");

    let url = messages_url(&chat_url);
    let resp = reqwest::Client::new()
        .post(&url)
        .header("x-api-key", "sk-anthropic-xxx")
        .json(&json!({
            "model": "claude/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 16
        }))
        .send()
        .await
        .expect("post");
    assert_eq!(
        resp.status(),
        200,
        "x-api-key must fold into Authorization before endpoint auth runs"
    );
}

// ---------------------------------------------------------------------------
// GatewayContext meta plumbing
// ---------------------------------------------------------------------------

/// Registers a proxy that captures the first inbound event's `meta`
/// object into the returned shared slot so the test can assert on
/// what the gateway wrote into `event.meta` before the pipeline ran.
fn register_meta_capturing_proxy(
    server: &AiGatewayServer,
    name: &str,
    protocol: Protocol,
) -> Arc<std::sync::Mutex<Option<serde_json::Map<String, Value>>>> {
    let captured: Arc<std::sync::Mutex<Option<serde_json::Map<String, Value>>>> =
        Arc::new(std::sync::Mutex::new(None));
    let captured_clone = Arc::clone(&captured);
    register_fake_proxy_with_protocol(server, name, protocol, move |event| {
        if let Some(meta) = event.meta.as_ref() {
            let mut guard = captured_clone.lock().unwrap();
            if guard.is_none() {
                *guard = Some(meta.clone());
            }
        }
        json!({"text": "ok"})
    });
    captured
}

#[tokio::test]
async fn gateway_writes_context_fields_to_event_meta_openai_path() {
    let (server, url, _handle) = boot_server().await;
    let captured = register_meta_capturing_proxy(&server, "flowgen_openai", Protocol::Openai);

    let _ = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "flowgen_openai/gpt-4",
            "messages": [{"role": "user", "content": "hi"}]
        }),
    )
    .await;

    let meta = captured
        .lock()
        .unwrap()
        .clone()
        .expect("gateway must populate event.meta before dispatch");
    assert_eq!(meta.get("protocol"), Some(&Value::String("openai".into())));
    assert_eq!(
        meta.get("proxy_name"),
        Some(&Value::String("flowgen_openai".into()))
    );
    assert_eq!(
        meta.get("requested_model"),
        Some(&Value::String("gpt-4".into()))
    );
    assert_eq!(meta.get("stream"), Some(&Value::Bool(false)));
    // user_id absent when auth is off — downstream reads as missing.
    assert!(!meta.contains_key("user_id"));
}

#[tokio::test]
async fn gateway_writes_context_fields_to_event_meta_anthropic_path() {
    let (server, chat_url, _handle) = boot_server().await;
    let captured = register_meta_capturing_proxy(&server, "flowgen_anthropic", Protocol::Anthropic);

    let url = messages_url(&chat_url);
    let _ = post_json(
        &reqwest::Client::new(),
        &url,
        json!({
            "model": "flowgen_anthropic/kimi",
            "messages": [{"role": "user", "content": "hi"}],
            "max_tokens": 128
        }),
    )
    .await;

    let meta = captured
        .lock()
        .unwrap()
        .clone()
        .expect("gateway must populate event.meta before dispatch");
    assert_eq!(
        meta.get("protocol"),
        Some(&Value::String("anthropic".into()))
    );
    assert_eq!(
        meta.get("proxy_name"),
        Some(&Value::String("flowgen_anthropic".into()))
    );
    assert_eq!(
        meta.get("requested_model"),
        Some(&Value::String("kimi".into()))
    );
    assert_eq!(meta.get("stream"), Some(&Value::Bool(false)));
}

#[tokio::test]
async fn gateway_meta_carries_stream_true_on_streaming_request() {
    let (server, url, _handle) = boot_server().await;
    let captured = register_meta_capturing_proxy(&server, "flowgen_openai", Protocol::Openai);

    // Streaming request — leaf still gets one inbound event; we only
    // care that the gateway flagged `stream: true` on it.
    let _frames = collect_sse(
        &url,
        json!({
            "model": "flowgen_openai/gpt-4",
            "messages": [{"role": "user", "content": "hi"}],
            "stream": true
        }),
    )
    .await;

    let meta = captured
        .lock()
        .unwrap()
        .clone()
        .expect("gateway must populate event.meta before dispatch");
    assert_eq!(meta.get("stream"), Some(&Value::Bool(true)));
}
