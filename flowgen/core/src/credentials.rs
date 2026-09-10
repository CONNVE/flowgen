//! Shared credential types for authenticating with external services.
//!
//! Provides a unified credentials format that can be loaded from JSON files
//! and reused across HTTP requests, MCP connections, and other integrations.
//!
//! Three auth modes are supported:
//!
//! - **Bearer** (`bearer_auth`) — a static token in `Authorization: Bearer <token>`.
//! - **Basic** (`basic_auth`) — username/password in `Authorization: Basic <base64>`.
//! - **OAuth 2.0 client credentials** (`oauth2_client_credentials`) — performs
//!   the [client credentials flow](https://datatracker.ietf.org/doc/html/rfc6749#section-4.4)
//!   against a token endpoint, caches the resulting access token, and
//!   auto-refreshes when it expires.

use serde::{Deserialize, Serialize};
use std::path::Path;
use tokio::sync::Mutex;

/// Errors that can occur during credential loading.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum Error {
    #[error("Failed to read credentials file at {path}: {source}")]
    ReadFile {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("Failed to parse credentials file at {path}: {source}")]
    ParseFile {
        path: String,
        #[source]
        source: serde_json::Error,
    },
}

/// Errors that can occur during OAuth 2.0 token fetch.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum OAuth2Error {
    #[error("Failed to send token request to {token_url}: {source}")]
    TokenRequest {
        token_url: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Token endpoint at {token_url} returned status {status}: {body}")]
    TokenResponse {
        token_url: String,
        status: u16,
        body: String,
    },
    #[error(
        "Token response from {token_url} is missing `access_token`, or body is not valid JSON"
    )]
    InvalidTokenResponse {
        token_url: String,
        #[source]
        source: serde_json::Error,
    },
}

/// HTTP authentication credentials loaded from a JSON file.
///
/// Bearer wins over Basic when both are present, matching HTTP semantics
/// (a single request carries one `Authorization` header). OAuth 2.0 client
/// credentials take precedence over both — the token is fetched and cached
/// on first use, then auto-refreshed when it expires.
///
/// The JSON file format:
///
/// ```json
/// {
///   "bearer_auth": "my-secret-token",
///   "basic_auth": {
///     "username": "user",
///     "password": "pass"
///   },
///   "oauth2_client_credentials": {
///     "token_url": "https://auth.example.com/oauth2/token",
///     "client_id": "my-client-id",
///     "client_secret": "my-client-secret",
///     "scope": "api"
///   }
/// }
/// ```
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct HttpCredentials {
    /// Bearer token for `Authorization: Bearer <token>` header.
    #[serde(default)]
    pub bearer_auth: Option<String>,
    /// Basic authentication credentials.
    #[serde(default)]
    pub basic_auth: Option<BasicAuth>,
    /// OAuth 2.0 client credentials flow configuration. When set, takes
    /// precedence over `bearer_auth` and `basic_auth`.
    #[serde(default)]
    pub oauth2_client_credentials: Option<OAuth2ClientCredentials>,
    /// Cached OAuth 2.0 token state. Populated lazily on first
    /// `authorization_header_async` call; not serialized or deserialized.
    #[serde(skip)]
    oauth2_cache: Mutex<Option<CachedToken>>,
}

impl PartialEq for HttpCredentials {
    fn eq(&self, other: &Self) -> bool {
        self.bearer_auth == other.bearer_auth
            && self.basic_auth == other.basic_auth
            && self.oauth2_client_credentials == other.oauth2_client_credentials
    }
}

impl Clone for HttpCredentials {
    fn clone(&self) -> Self {
        Self {
            bearer_auth: self.bearer_auth.clone(),
            basic_auth: self.basic_auth.clone(),
            oauth2_client_credentials: self.oauth2_client_credentials.clone(),
            oauth2_cache: Mutex::new(None),
        }
    }
}

/// Basic authentication username and password.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct BasicAuth {
    /// Username for basic authentication.
    pub username: String,
    /// Password for basic authentication.
    pub password: String,
}

/// OAuth 2.0 client credentials flow configuration.
///
/// Performs the [client credentials flow] to obtain an access token from
/// the token endpoint. The token is cached and auto-refreshed when it
/// expires. Takes precedence over `bearer_auth` and `basic_auth` when
/// present in the credentials file.
///
/// [client credentials flow]: https://datatracker.ietf.org/doc/html/rfc6749#section-4.4
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
pub struct OAuth2ClientCredentials {
    /// Token endpoint URL (e.g. `https://auth.example.com/oauth2/token`).
    pub token_url: String,
    /// OAuth 2.0 client ID.
    pub client_id: String,
    /// OAuth 2.0 client secret.
    pub client_secret: String,
    /// Optional space-delimited scope(s) to request.
    #[serde(default)]
    pub scope: Option<String>,
}

/// A cached OAuth 2.0 token with its expiry time.
#[derive(Clone, Debug)]
struct CachedToken {
    token: String,
    expires_at: std::time::Instant,
}

impl CachedToken {
    /// Returns `true` if the token is still valid, keeping
    /// [`TOKEN_EXPIRY_BUFFER_SECS`] of headroom before its stated expiry.
    fn is_valid(&self) -> bool {
        self.expires_at
            .checked_duration_since(std::time::Instant::now())
            .is_some_and(|remaining| remaining.as_secs() > TOKEN_EXPIRY_BUFFER_SECS)
    }
}

/// Response body from a standard OAuth 2.0 token endpoint.
#[derive(Deserialize)]
struct TokenResponse {
    access_token: String,
    /// RFC 6749 marks this RECOMMENDED, not required, so issuers may omit it.
    #[serde(default)]
    expires_in: Option<u64>,
}

/// Assumed token lifetime when the endpoint omits `expires_in`.
const DEFAULT_TOKEN_LIFETIME_SECS: u64 = 300;

/// Refresh this long before stated expiry, so a token never lapses mid-flight.
const TOKEN_EXPIRY_BUFFER_SECS: u64 = 60;

impl HttpCredentials {
    /// Creates credentials with a static bearer token.
    pub fn bearer(token: impl Into<String>) -> Self {
        Self {
            bearer_auth: Some(token.into()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Creates credentials with basic authentication.
    pub fn basic(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: username.into(),
                password: password.into(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Creates credentials with OAuth 2.0 client credentials flow.
    pub fn oauth2(
        token_url: impl Into<String>,
        client_id: impl Into<String>,
        client_secret: impl Into<String>,
        scope: Option<String>,
    ) -> Self {
        Self {
            bearer_auth: None,
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: token_url.into(),
                client_id: client_id.into(),
                client_secret: client_secret.into(),
                scope,
            }),
            oauth2_cache: Mutex::new(None),
        }
    }

    /// Returns the value for an outgoing `Authorization` header synchronously.
    ///
    /// Only handles static `bearer_auth` and `basic_auth`. For OAuth 2.0
    /// client credentials, use [`authorization_header_async`](Self::authorization_header_async)
    /// which can perform network I/O to fetch/refresh the token.
    ///
    /// Bearer wins over Basic when both are present, matching HTTP semantics
    /// (a single request carries one `Authorization` header).
    ///
    /// Used by transports that expose raw header injection rather than a
    /// reqwest `RequestBuilder`, so basic-auth encoding is not re-implemented
    /// per call site.
    pub fn authorization_header(&self) -> Option<String> {
        use base64::Engine;
        match (&self.bearer_auth, &self.basic_auth) {
            (Some(token), _) => Some(format!("Bearer {token}")),
            (None, Some(basic)) => {
                let raw = format!("{}:{}", basic.username, basic.password);
                let encoded = base64::engine::general_purpose::STANDARD.encode(raw);
                Some(format!("Basic {encoded}"))
            }
            (None, None) => None,
        }
    }

    /// Returns the value for an outgoing `Authorization` header, fetching
    /// or refreshing an OAuth 2.0 token when `oauth2_client_credentials` is
    /// configured.
    ///
    /// Precedence: OAuth 2.0 client credentials > Bearer > Basic > None.
    pub async fn authorization_header_async(&self) -> Result<Option<String>, OAuth2Error> {
        match &self.oauth2_client_credentials {
            Some(oauth2) => {
                let token = self.get_or_refresh_token(oauth2).await?;
                Ok(Some(format!("Bearer {token}")))
            }
            None => Ok(self.authorization_header()),
        }
    }

    /// Returns a cached token if still valid, otherwise fetches a new one
    /// from the token endpoint.
    async fn get_or_refresh_token(
        &self,
        oauth2: &OAuth2ClientCredentials,
    ) -> Result<String, OAuth2Error> {
        // Held across the fetch so concurrent callers on a cold or expired
        // cache issue one token request between them, not one each — token
        // endpoints are commonly rate-limited.
        let mut guard = self.oauth2_cache.lock().await;

        if let Some(cached) = guard.as_ref() {
            if cached.is_valid() {
                return Ok(cached.token.clone());
            }
        }

        let token = self.fetch_token(oauth2).await?;
        *guard = Some(token.clone());

        Ok(token.token)
    }

    /// Fetches a new access token from the OAuth 2.0 token endpoint using
    /// the client credentials grant type.
    async fn fetch_token(
        &self,
        oauth2: &OAuth2ClientCredentials,
    ) -> Result<CachedToken, OAuth2Error> {
        let client = reqwest::Client::new();
        let mut form = vec![
            ("grant_type".to_string(), "client_credentials".to_string()),
            ("client_id".to_string(), oauth2.client_id.clone()),
            ("client_secret".to_string(), oauth2.client_secret.clone()),
        ];
        if let Some(scope) = &oauth2.scope {
            form.push(("scope".to_string(), scope.clone()));
        }

        let response = client
            .post(&oauth2.token_url)
            .form(&form)
            .send()
            .await
            .map_err(|source| OAuth2Error::TokenRequest {
                token_url: oauth2.token_url.clone(),
                source,
            })?;

        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        if status != 200 {
            return Err(OAuth2Error::TokenResponse {
                token_url: oauth2.token_url.clone(),
                status,
                body,
            });
        }

        let token_response: TokenResponse =
            serde_json::from_str(&body).map_err(|source| OAuth2Error::InvalidTokenResponse {
                token_url: oauth2.token_url.clone(),
                source,
            })?;

        let lifetime = token_response
            .expires_in
            .unwrap_or(DEFAULT_TOKEN_LIFETIME_SECS);
        let expires_at = std::time::Instant::now() + std::time::Duration::from_secs(lifetime);

        Ok(CachedToken {
            token: token_response.access_token,
            expires_at,
        })
    }
}

/// Loads and parses HTTP credentials from a JSON file.
pub async fn load_http_credentials(path: &Path) -> Result<HttpCredentials, Error> {
    let content = tokio::fs::read_to_string(path)
        .await
        .map_err(|source| Error::ReadFile {
            path: path.display().to_string(),
            source,
        })?;
    serde_json::from_str(&content).map_err(|source| Error::ParseFile {
        path: path.display().to_string(),
        source,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authorization_header_bearer() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_basic() {
        let creds = HttpCredentials {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: "Aladdin".to_string(),
                password: "open sesame".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(
            creds.authorization_header().as_deref(),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        );
    }

    #[test]
    fn authorization_header_bearer_wins() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: Some(BasicAuth {
                username: "u".to_string(),
                password: "p".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds.authorization_header().as_deref(), Some("Bearer tok"));
    }

    #[test]
    fn authorization_header_none() {
        let creds = HttpCredentials::default();
        assert_eq!(creds.authorization_header(), None);
    }

    #[tokio::test]
    async fn authorization_header_async_bearer_no_oauth2() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        let result = creds.authorization_header_async().await;
        assert_eq!(result.unwrap().as_deref(), Some("Bearer tok"));
    }

    #[tokio::test]
    async fn authorization_header_async_basic_no_oauth2() {
        let creds = HttpCredentials {
            bearer_auth: None,
            basic_auth: Some(BasicAuth {
                username: "Aladdin".to_string(),
                password: "open sesame".to_string(),
            }),
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        let result = creds.authorization_header_async().await;
        assert_eq!(
            result.unwrap().as_deref(),
            Some("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ=="),
        );
    }

    #[tokio::test]
    async fn authorization_header_async_none() {
        let creds = HttpCredentials::default();
        let result = creds.authorization_header_async().await;
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn cached_token_is_valid_with_future_expiry() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
        };
        assert!(token.is_valid());
    }

    #[test]
    fn cached_token_is_invalid_when_past_expiry() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now(),
        };
        assert!(!token.is_valid());
    }

    #[test]
    fn cached_token_is_invalid_when_within_buffer() {
        let token = CachedToken {
            token: "abc".to_string(),
            expires_at: std::time::Instant::now() + std::time::Duration::from_secs(30),
        };
        assert!(!token.is_valid());
    }

    #[test]
    fn http_credentials_partial_eq_ignores_cache() {
        let creds_a = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let creds_b = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(None),
        };
        assert_eq!(creds_a, creds_b);
    }

    #[test]
    fn http_credentials_clone_resets_cache() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let mut cloned = creds.clone();
        assert!(cloned.oauth2_cache.get_mut().is_none());
    }

    #[test]
    fn oauth2_client_credentials_deserialize() {
        let json = r#"{
            "token_url": "https://auth.example.com/oauth2/token",
            "client_id": "my-client-id",
            "client_secret": "my-client-secret",
            "scope": "api"
        }"#;
        let oauth2: OAuth2ClientCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(oauth2.token_url, "https://auth.example.com/oauth2/token");
        assert_eq!(oauth2.client_id, "my-client-id");
        assert_eq!(oauth2.client_secret, "my-client-secret");
        assert_eq!(oauth2.scope.as_deref(), Some("api"));
    }

    #[test]
    fn oauth2_client_credentials_deserialize_without_scope() {
        let json = r#"{
            "token_url": "https://example.com/token",
            "client_id": "id",
            "client_secret": "secret"
        }"#;
        let oauth2: OAuth2ClientCredentials = serde_json::from_str(json).unwrap();
        assert!(oauth2.scope.is_none());
    }

    #[test]
    fn http_credentials_deserialize_with_oauth2() {
        let json = r#"{
            "oauth2_client_credentials": {
                "token_url": "https://example.com/token",
                "client_id": "id",
                "client_secret": "secret"
            }
        }"#;
        let creds: HttpCredentials = serde_json::from_str(json).unwrap();
        assert!(creds.bearer_auth.is_none());
        assert!(creds.basic_auth.is_none());
        assert!(creds.oauth2_client_credentials.is_some());
    }

    #[test]
    fn http_credentials_deserialize_with_bearer_only() {
        let json = r#"{
            "bearer_auth": "my-token"
        }"#;
        let creds: HttpCredentials = serde_json::from_str(json).unwrap();
        assert_eq!(creds.bearer_auth.as_deref(), Some("my-token"));
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[test]
    fn http_credentials_serde_roundtrip_preserves_oauth2() {
        let original = HttpCredentials {
            bearer_auth: None,
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "https://example.com/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: Some("api".to_string()),
            }),
            oauth2_cache: Mutex::new(None),
        };
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: HttpCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(original, deserialized);
    }

    #[test]
    fn http_credentials_serde_skips_cache_field() {
        let creds = HttpCredentials {
            bearer_auth: Some("tok".to_string()),
            basic_auth: None,
            oauth2_client_credentials: None,
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let json = serde_json::to_string(&creds).unwrap();
        assert!(!json.contains("oauth2_cache"));
    }

    #[test]
    fn bearer_constructor_sets_only_bearer() {
        let creds = HttpCredentials::bearer("my-token");
        assert_eq!(creds.bearer_auth.as_deref(), Some("my-token"));
        assert!(creds.basic_auth.is_none());
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[test]
    fn basic_constructor_sets_only_basic() {
        let creds = HttpCredentials::basic("user", "pass");
        assert!(creds.bearer_auth.is_none());
        let basic = creds.basic_auth.unwrap();
        assert_eq!(basic.username, "user");
        assert_eq!(basic.password, "pass");
        assert!(creds.oauth2_client_credentials.is_none());
    }

    #[tokio::test]
    async fn authorization_header_async_falls_back_to_bearer_when_no_oauth2() {
        let creds = HttpCredentials::bearer("static-tok");
        let result = creds.authorization_header_async().await.unwrap();
        assert_eq!(result.as_deref(), Some("Bearer static-tok"));
    }

    #[tokio::test]
    async fn authorization_header_async_skips_oauth_when_cache_pre_populated() {
        let creds = HttpCredentials {
            bearer_auth: Some("fallback".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://should-not-be-called.test/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "cached-token".to_string(),
                expires_at: std::time::Instant::now() + std::time::Duration::from_secs(300),
            })),
        };
        let result = creds.authorization_header_async().await.unwrap();
        assert_eq!(result.as_deref(), Some("Bearer cached-token"));
    }

    #[tokio::test]
    async fn authorization_header_async_returns_oauth_error_when_token_expired_and_endpoint_unreachable(
    ) {
        let creds = HttpCredentials {
            bearer_auth: Some("fallback".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://127.0.0.1:1/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(Some(CachedToken {
                token: "expired".to_string(),
                expires_at: std::time::Instant::now(),
            })),
        };
        let result = creds.authorization_header_async().await;
        assert!(result.is_err());
    }

    #[test]
    fn oauth2_precedence_over_bearer_in_doc() {
        let creds = HttpCredentials {
            bearer_auth: Some("static".to_string()),
            basic_auth: None,
            oauth2_client_credentials: Some(OAuth2ClientCredentials {
                token_url: "http://example.com/token".to_string(),
                client_id: "id".to_string(),
                client_secret: "secret".to_string(),
                scope: None,
            }),
            oauth2_cache: Mutex::new(None),
        };
        assert!(creds.oauth2_client_credentials.is_some());
    }
}
