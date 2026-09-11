//! HTTP request processor for making outbound HTTP calls.
//!
//! Handles HTTP request processing with authentication, headers,
//! and various payload formats. Processes events by making HTTP requests
//! and publishing the responses as new events.

use flowgen_core::credentials::HttpCredentials;
use flowgen_core::{
    config::ConfigExt,
    event::{Event, EventBuilder, EventData, EventExt},
};
use futures_util::future;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio::{
    fs,
    sync::mpsc::{Receiver, Sender},
};
use tracing::{error, Instrument};

/// Formats an error and every entry in its `source()` chain into a single
/// colon-separated string.
///
/// Reqwest's top-level `Display` for builder errors is the literal string
/// `"builder error"`; the actual cause (`url::ParseError`,
/// `serde_urlencoded::ser::Error`, `InvalidHeaderValue`, etc.) lives only
/// in the source chain. Walking it surfaces the underlying cause to
/// operators without having to inspect logs at debug level.
fn error_chain<E: std::error::Error + ?Sized>(err: &E) -> String {
    let mut out = err.to_string();
    let mut current: Option<&dyn std::error::Error> = err.source();
    while let Some(cause) = current {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        current = cause.source();
    }
    out
}

/// Errors that can occur during HTTP request processing.
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
    #[error("Error reading credentials file at {path}: {source}")]
    ReadHttpCredentials {
        path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("JSON error: {source}")]
    SerdeJson {
        #[source]
        source: serde_json::Error,
    },
    #[error("OAuth 2.0 token fetch failed: {source}")]
    OAuth2TokenFetch {
        #[source]
        source: flowgen_core::credentials::OAuth2Error,
    },
    #[error("Config template rendering error: {source}")]
    ConfigRender {
        #[source]
        source: flowgen_core::config::Error,
    },
    #[error("HTTP client init failed: {}", error_chain(.source))]
    ClientInit {
        #[source]
        source: reqwest::Error,
    },
    #[error(
        "HTTP request error for {method} {endpoint}: {}",
        error_chain(.source)
    )]
    Reqwest {
        endpoint: String,
        method: String,
        #[source]
        source: reqwest::Error,
    },
    #[error("Invalid HTTP header name: {source}")]
    ReqwestInvalidHeaderName {
        #[source]
        source: reqwest::header::InvalidHeaderName,
    },
    #[error("Invalid HTTP header value: {source}")]
    ReqwestInvalidHeaderValue {
        #[source]
        source: reqwest::header::InvalidHeaderValue,
    },
    #[error("Either payload json or payload input is required")]
    PayloadConfig,
    #[error("Event data was not found on the event payload")]
    MissingEventData,
    #[error("Missing required builder attribute: {}", _0)]
    MissingBuilderAttribute(String),
    #[error("Task failed after all retry attempts: {source}")]
    RetryExhausted {
        #[source]
        source: Box<Error>,
    },
    #[error("HTTP request to {method} {endpoint} failed with status {status}: {body}")]
    HttpError {
        endpoint: String,
        method: String,
        status: u16,
        body: String,
    },
}

/// Event handler for processing HTTP requests.
#[derive(Debug)]
pub struct EventHandler {
    /// HTTP client instance.
    client: Arc<reqwest::Client>,
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Credentials loaded once at init from `credentials_path`.
    credentials: Option<HttpCredentials>,
    /// Event sender channel.
    tx: Option<Sender<Event>>,
    /// Current task identifier.
    task_id: usize,
    /// Task type for event categorization and logging.
    task_type: &'static str,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Arc<flowgen_core::task::context::TaskContext>,
}

impl EventHandler {
    /// Processes an event by making an HTTP request.
    #[tracing::instrument(skip(self, event), name = "task.handle", fields(duration_ms = tracing::field::Empty))]
    async fn handle(&self, event: Event) -> Result<(), Error> {
        if self.task_context.cancellation_token.is_cancelled() {
            return Ok(());
        }

        let event = Arc::new(event);
        let completion_tx_arc = Arc::clone(&event).completion_tx.clone();

        flowgen_core::event::with_event_context(&Arc::clone(&event), async move {
            // Capture processing start time via EventBuilder (sets timestamp to now).
            let event_builder = EventBuilder::new();

            // Render config to support templates inside configuration.
            let event_value = serde_json::value::Value::try_from(event.as_ref())
                .map_err(|source| Error::EventBuilder { source })?;
            let config = self
                .config
                .render(&event_value)
                .map_err(|source| Error::ConfigRender { source })?;

            // Capture the rendered endpoint and method as plain strings so
            // every error and the success log carry enough context to identify
            // which request was being attempted. Reqwest builder errors only
            // surface the underlying cause via the source chain, never the
            // request that triggered them.
            let endpoint = config.endpoint.clone();
            let method = format!("{:?}", config.method).to_uppercase();

            let mut client = match config.method {
                crate::config::Method::Get => self.client.get(config.endpoint),
                crate::config::Method::Post => self.client.post(config.endpoint),
                crate::config::Method::Put => self.client.put(config.endpoint),
                crate::config::Method::Delete => self.client.delete(config.endpoint),
                crate::config::Method::Patch => self.client.patch(config.endpoint),
                crate::config::Method::Head => self.client.head(config.endpoint),
            };

            if let Some(headers) = config.headers.to_owned() {
                let mut header_map = HeaderMap::new();
                for (key, value) in headers {
                    let header_name = HeaderName::try_from(key)
                        .map_err(|source| Error::ReqwestInvalidHeaderName { source })?;
                    let header_value = HeaderValue::try_from(value)
                        .map_err(|source| Error::ReqwestInvalidHeaderValue { source })?;
                    header_map.insert(header_name, header_value);
                }
                client = client.headers(header_map);
            }

            if let Some(payload) = &config.payload {
                let event_data = if payload.from_event {
                    event_value
                        .get("event")
                        .and_then(|e| e.get("data"))
                        .ok_or_else(|| Error::MissingEventData)?
                } else {
                    &match &payload.object {
                        Some(obj) => Value::Object(obj.to_owned()),
                        None => match &payload.input {
                            Some(input) => {
                                serde_json::from_str::<serde_json::Value>(input.as_str())
                                    .map_err(|source| Error::SerdeJson { source })?
                            }
                            None => return Err(Error::PayloadConfig),
                        },
                    }
                };

                client = match payload.send_as {
                    crate::config::PayloadSendAs::Json => client.json(&event_data),
                    crate::config::PayloadSendAs::UrlEncoded => client.form(&event_data),
                    crate::config::PayloadSendAs::QueryParams => client.query(&event_data),
                };
            }

            if let Some(credentials) = &self.credentials {
                let header_value = credentials
                    .authorization_header_async()
                    .await
                    .map_err(|source| Error::OAuth2TokenFetch { source })?;
                if let Some(header_value) = header_value {
                    client = client.header(reqwest::header::AUTHORIZATION, header_value);
                }
            }

            let response = client.send().await.map_err(|source| Error::Reqwest {
                endpoint: endpoint.clone(),
                method: method.clone(),
                source,
            })?;

            let status = response.status();

            let event_data = match self.config.response_type {
                crate::config::ResponseType::Bytes => {
                    let raw = response.bytes().await.map_err(|source| Error::Reqwest {
                        endpoint: endpoint.clone(),
                        method: method.clone(),
                        source,
                    })?;
                    if status.is_client_error() || status.is_server_error() {
                        return Err(Error::HttpError {
                            endpoint: endpoint.clone(),
                            method: method.clone(),
                            status: status.as_u16(),
                            body: String::from_utf8_lossy(&raw).to_string(),
                        });
                    }
                    EventData::Bytes(raw)
                }
                crate::config::ResponseType::Text => {
                    let body = response.text().await.map_err(|source| Error::Reqwest {
                        endpoint: endpoint.clone(),
                        method: method.clone(),
                        source,
                    })?;
                    if status.is_client_error() || status.is_server_error() {
                        return Err(Error::HttpError {
                            endpoint: endpoint.clone(),
                            method: method.clone(),
                            status: status.as_u16(),
                            body,
                        });
                    }
                    EventData::Json(json!(body))
                }
                crate::config::ResponseType::Json => {
                    let body = response.text().await.map_err(|source| Error::Reqwest {
                        endpoint: endpoint.clone(),
                        method: method.clone(),
                        source,
                    })?;
                    if status.is_client_error() || status.is_server_error() {
                        return Err(Error::HttpError {
                            endpoint: endpoint.clone(),
                            method: method.clone(),
                            status: status.as_u16(),
                            body,
                        });
                    }
                    let data = serde_json::from_str::<Value>(&body).unwrap_or_else(|_| json!(body));
                    EventData::Json(data)
                }
            };

            let mut e = event_builder
                .data(event_data)
                .subject(self.config.name.to_owned())
                .task_id(self.task_id)
                .task_type(self.task_type)
                .build()
                .map_err(|source| Error::EventBuilder { source })?;

            // Signal completion or pass through to next task.
            match self.tx {
                None => {
                    // Leaf task: signal completion.
                    if let Some(arc) = completion_tx_arc.as_ref() {
                        arc.signal_completion(e.data_as_json().ok());
                    }
                }
                Some(_) => {
                    // Pass through completion_tx to next task.
                    e.completion_tx = completion_tx_arc.clone();
                }
            }

            e.send_with_logging(self.tx.as_ref())
                .context("endpoint", &endpoint)
                .context("method", &method)
                .context("status", status.as_u16())
                .await
                .map_err(|source| Error::SendMessage { source })?;
            Ok(())
        })
        .await
    }
}

/// HTTP request processor.
#[derive(Debug)]
pub struct Processor {
    /// Processor configuration.
    config: Arc<super::config::Processor>,
    /// Event sender channel.
    tx: Option<Sender<Event>>,
    /// Event receiver channel.
    rx: Receiver<Event>,
    /// Current task identifier.
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

    async fn init(&self) -> Result<EventHandler, Error> {
        let mut builder = reqwest::ClientBuilder::new()
            .https_only(true)
            .gzip(true)
            .brotli(true)
            .deflate(true);
        if let Some(timeout) = self.config.timeout {
            builder = builder.timeout(timeout);
        }
        if let Some(connect_timeout) = self.config.connect_timeout {
            builder = builder.connect_timeout(connect_timeout);
        }
        let client = Arc::new(
            builder
                .build()
                .map_err(|source| Error::ClientInit { source })?,
        );

        let credentials = match &self.config.credentials_path {
            Some(path) => {
                let content = fs::read_to_string(path).await.map_err(|source| {
                    Error::ReadHttpCredentials {
                        path: path.clone(),
                        source,
                    }
                })?;
                let creds: HttpCredentials =
                    serde_json::from_str(&content).map_err(|source| Error::SerdeJson { source })?;
                Some(creds)
            }
            None => None,
        };

        Ok(EventHandler {
            config: Arc::clone(&self.config),
            task_id: self.task_id,
            tx: self.tx.clone(),
            client,
            credentials,
            task_type: self.task_type,
            task_context: Arc::clone(&self.task_context),
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
                        error!(error = %e, "Failed to initialize request processor");
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
                                        // 4xx client errors (except 429) are permanent — the
                                        // request data is invalid and retrying will not help.
                                        let is_permanent = matches!(
                                            &e,
                                            Error::HttpError { status, .. }
                                                if (400..500).contains(status) && *status != 429
                                        );

                                        if is_permanent {
                                            error!(error = %e, "HTTP request failed with client error, skipping retries");
                                            Err(tokio_retry::RetryError::permanent(e))
                                        } else {
                                            error!(error = %e, "Failed to process HTTP request");
                                            Err(tokio_retry::RetryError::transient(e))
                                        }
                                    }
                                }
                            })
                            .await;

                            if let Err(err) = result {
                                error!(error = %err, "HTTP request failed after all retry attempts");
                                let mut error_event = event.clone();
                                error_event.error = Some(err.to_string());
                                if let Some(ref tx) = event_handler.tx {
                                    tx.send(error_event).await.ok();
                                } else if let Some(arc) = event.completion_tx.as_ref() {
                                    arc.signal_completion_with_error(err.to_string());
                                }
                            }
                        }
                        .instrument(tracing::Span::current()),
                    );
                    handlers.push(handle);
                }
                None => {
                    // Channel closed, wait for all spawned handlers to complete.
                    future::join_all(handlers).await;
                    return Ok(());
                }
            }
        }
    }
}

/// Builder for HTTP request processor.
#[derive(Debug, Default)]
pub struct ProcessorBuilder {
    /// Optional processor configuration.
    config: Option<Arc<super::config::Processor>>,
    /// Optional event sender.
    tx: Option<Sender<Event>>,
    /// Optional event receiver.
    rx: Option<Receiver<Event>>,
    /// Task execution context providing metadata and runtime configuration.
    task_context: Option<Arc<flowgen_core::task::context::TaskContext>>,
    /// Current task identifier.
    task_id: usize,
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
    use flowgen_core::credentials::BasicAuth;
    use serde_json::Map;
    use tokio::sync::mpsc;

    /// Creates a mock TaskContext for testing.
    fn create_mock_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
        let mut labels = Map::new();
        labels.insert(
            "description".to_string(),
            Value::String("Clone Test".to_string()),
        );
        let task_manager = Arc::new(
            flowgen_core::task::manager::TaskManagerBuilder::new()
                .build()
                .unwrap(),
        );
        let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
            as Arc<dyn flowgen_core::cache::Cache>;
        Arc::new(
            flowgen_core::task::context::TaskContextBuilder::new()
                .flow_name("test-flow".to_string())
                .flow_labels(Some(labels))
                .task_manager(task_manager)
                .cache(cache)
                .build()
                .unwrap(),
        )
    }

    #[test]
    fn test_credentials_default() {
        let creds = HttpCredentials::default();
        assert_eq!(creds.bearer_auth, None);
        assert_eq!(creds.basic_auth, None);
    }

    #[test]
    fn test_credentials_creation() {
        let creds = HttpCredentials::bearer("bearer_token_123");

        assert_eq!(creds.bearer_auth, Some("bearer_token_123".to_string()));
        assert!(creds.basic_auth.is_none());
    }

    #[test]
    fn test_credentials_serialization() {
        let creds = HttpCredentials::bearer("token");

        let json = serde_json::to_string(&creds).unwrap();
        let deserialized: HttpCredentials = serde_json::from_str(&json).unwrap();
        assert_eq!(creds, deserialized);
    }

    #[test]
    fn test_basic_auth_default() {
        let basic_auth = BasicAuth::default();
        assert_eq!(basic_auth.username, "");
        assert_eq!(basic_auth.password, "");
    }

    #[test]
    fn test_basic_auth_creation() {
        let basic_auth = BasicAuth {
            username: "admin".to_string(),
            password: "secret123".to_string(),
        };

        assert_eq!(basic_auth.username, "admin");
        assert_eq!(basic_auth.password, "secret123");
    }

    #[test]
    fn test_basic_auth_serialization() {
        let basic_auth = BasicAuth {
            username: "test_user".to_string(),
            password: "test_password".to_string(),
        };

        let json = serde_json::to_string(&basic_auth).unwrap();
        let deserialized: BasicAuth = serde_json::from_str(&json).unwrap();
        assert_eq!(basic_auth, deserialized);
    }

    #[test]
    fn test_error_read_credentials_structure() {
        use std::path::PathBuf;
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let error = Error::ReadHttpCredentials {
            path: PathBuf::from("/test/credentials.json"),
            source: io_error,
        };
        assert!(matches!(error, Error::ReadHttpCredentials { .. }));
    }

    #[test]
    fn test_error_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error = Error::SerdeJson { source: json_error };
        assert!(matches!(error, Error::SerdeJson { .. }));
    }

    #[tokio::test]
    async fn test_processor_builder() {
        let config = Arc::new(crate::config::Processor {
            name: "test_processor".to_string(),
            endpoint: "https://test.com".to_string(),
            method: crate::config::Method::Get,
            payload: None,
            headers: None,
            credentials_path: None,
            ack_timeout: None,
            timeout: crate::config::default_request_timeout(),
            connect_timeout: crate::config::default_connect_timeout(),
            max_body_bytes: crate::config::default_max_body_bytes(),
            stream: false,
            response_type: crate::config::ResponseType::default(),
            auth: None,
            depends_on: None,
            retry: None,
        });
        let (tx, rx) = mpsc::channel(100);

        // Success case.
        let processor = ProcessorBuilder::new()
            .config(config.clone())
            .sender(tx.clone())
            .receiver(rx)
            .task_id(1)
            .task_type("test")
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(processor.is_ok());

        // Error case - missing config.
        let (tx2, rx2) = mpsc::channel(100);
        let result = ProcessorBuilder::new()
            .sender(tx2)
            .receiver(rx2)
            .task_context(create_mock_task_context())
            .build()
            .await;
        assert!(matches!(
            result.unwrap_err(),
            Error::MissingBuilderAttribute(_)
        ));
    }
}
