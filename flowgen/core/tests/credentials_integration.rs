//! Integration tests for OAuth 2.0 client credentials flow in
//! `HttpCredentials`.
//!
//! Spins up an in-process HTTP server (via `tokio::net::TcpListener` +
//! `axum`) that implements a mock OAuth 2.0 token endpoint, then verifies
//! that `authorization_header_async` fetches, caches, and refreshes
//! access tokens correctly. No external service required — runs in the
//! normal `cargo test` job without `#[ignore]`.

use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use axum::http::StatusCode;
use axum::routing::post;
use axum::Router;
use flowgen_core::credentials::{HttpCredentials, OAuth2Error};

struct MockTokenState {
    call_count: AtomicU32,
    response_access_token: String,
    response_expires_in: Option<u64>,
    response_status: u16,
    response_delay: Option<std::time::Duration>,
}

async fn boot_token_server(state: Arc<MockTokenState>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind ephemeral port");
    let addr = listener.local_addr().expect("read local addr");

    let app = Router::new().route(
        "/token",
        post(move || {
            let state = Arc::clone(&state);
            async move {
                state.call_count.fetch_add(1, Ordering::SeqCst);
                if let Some(delay) = state.response_delay {
                    tokio::time::sleep(delay).await;
                }
                let status =
                    StatusCode::from_u16(state.response_status).expect("valid status code");
                if status.is_success() {
                    let mut body = serde_json::json!({
                        "access_token": state.response_access_token,
                        "token_type": "Bearer",
                    });
                    if let Some(expires_in) = state.response_expires_in {
                        body["expires_in"] = expires_in.into();
                    }
                    (status, axum::Json(body))
                } else {
                    let body = serde_json::json!({
                        "error": "invalid_client",
                        "error_description": "simulated failure",
                    });
                    (status, axum::Json(body))
                }
            }
        }),
    );

    let handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server runs");
    });

    (format!("http://{addr}/token"), handle)
}

fn make_creds(token_url: &str) -> HttpCredentials {
    HttpCredentials::oauth2(
        token_url,
        "test-client",
        "test-secret",
        Some("api".to_string()),
    )
}

#[tokio::test]
async fn oauth2_fetches_token_on_first_call() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "access-123".to_string(),
        response_expires_in: Some(3600),
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let header = creds.authorization_header_async().await.unwrap();
    assert_eq!(header.as_deref(), Some("Bearer access-123"));
    assert_eq!(state.call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oauth2_caches_token_and_skips_refetch() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "cached-token".to_string(),
        response_expires_in: Some(3600),
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let h1 = creds.authorization_header_async().await.unwrap();
    let h2 = creds.authorization_header_async().await.unwrap();
    let h3 = creds.authorization_header_async().await.unwrap();

    assert_eq!(h1.as_deref(), Some("Bearer cached-token"));
    assert_eq!(h2.as_deref(), Some("Bearer cached-token"));
    assert_eq!(h3.as_deref(), Some("Bearer cached-token"));
    assert_eq!(state.call_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn oauth2_refreshes_when_token_expired() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "refreshed-token".to_string(),
        response_expires_in: Some(0),
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let h1 = creds.authorization_header_async().await.unwrap();
    assert_eq!(h1.as_deref(), Some("Bearer refreshed-token"));
    assert_eq!(state.call_count.load(Ordering::SeqCst), 1);

    let h2 = creds.authorization_header_async().await.unwrap();
    assert_eq!(h2.as_deref(), Some("Bearer refreshed-token"));
    assert_eq!(state.call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn oauth2_errors_on_non_200_response() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "unused".to_string(),
        response_expires_in: Some(3600),
        response_status: 401,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let err = creds.authorization_header_async().await.unwrap_err();
    match err {
        OAuth2Error::TokenResponse { status, body, .. } => {
            assert_eq!(status, 401);
            assert!(body.contains("invalid_client"));
        }
        other => panic!("expected TokenResponse, got {other:?}"),
    }
}

#[tokio::test]
async fn oauth2_errors_on_malformed_token_body() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");

    let app = Router::new().route("/token", post(|| async { "this is not json" }));

    let _handle = tokio::spawn(async move {
        axum::serve(listener, app).await.expect("server runs");
    });

    let token_url = format!("http://{addr}/token");
    let creds = make_creds(&token_url);

    let err = creds.authorization_header_async().await.unwrap_err();
    assert!(matches!(err, OAuth2Error::InvalidTokenResponse { .. }));
}

#[tokio::test]
async fn oauth2_precedence_over_static_bearer() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "oauth-token".to_string(),
        response_expires_in: Some(3600),
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let mut creds = make_creds(&token_url);
    creds.bearer_auth = Some("static-fallback".to_string());

    let header = creds.authorization_header_async().await.unwrap();
    assert_eq!(header.as_deref(), Some("Bearer oauth-token"));
}

#[tokio::test]
async fn oauth2_accepts_token_response_without_expires_in() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "no-expiry-token".to_string(),
        response_expires_in: None,
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let h1 = creds.authorization_header_async().await.unwrap();
    assert_eq!(h1.as_deref(), Some("Bearer no-expiry-token"));

    let h2 = creds.authorization_header_async().await.unwrap();
    assert_eq!(h2.as_deref(), Some("Bearer no-expiry-token"));
    assert_eq!(
        state.call_count.load(Ordering::SeqCst),
        1,
        "a response without `expires_in` must fall back to the default \
         lifetime and still cache, not refetch on every call"
    );
}

#[tokio::test]
async fn oauth2_concurrent_callers_issue_a_single_token_request() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "shared-token".to_string(),
        response_expires_in: Some(3600),
        response_status: 200,
        response_delay: Some(std::time::Duration::from_millis(200)),
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = Arc::new(make_creds(&token_url));

    let mut set = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let creds = Arc::clone(&creds);
        set.spawn(async move { creds.authorization_header_async().await });
    }

    while let Some(result) = set.join_next().await {
        let header = result.expect("task joins").expect("token fetch succeeds");
        assert_eq!(header.as_deref(), Some("Bearer shared-token"));
    }

    assert_eq!(
        state.call_count.load(Ordering::SeqCst),
        1,
        "8 concurrent callers on a cold cache must collapse into one token \
         request; token endpoints are rate-limited"
    );
}

#[tokio::test]
async fn oauth2_clone_resets_cache_and_refetches() {
    let state = Arc::new(MockTokenState {
        call_count: AtomicU32::new(0),
        response_access_token: "after-clone".to_string(),
        response_expires_in: Some(3600),
        response_status: 200,
        response_delay: None,
    });
    let (token_url, _handle) = boot_token_server(state.clone()).await;

    let creds = make_creds(&token_url);

    let _ = creds.authorization_header_async().await.unwrap();
    assert_eq!(state.call_count.load(Ordering::SeqCst), 1);

    let cloned = creds.clone();
    let header = cloned.authorization_header_async().await.unwrap();
    assert_eq!(header.as_deref(), Some("Bearer after-clone"));
    assert_eq!(state.call_count.load(Ordering::SeqCst), 2);
}
