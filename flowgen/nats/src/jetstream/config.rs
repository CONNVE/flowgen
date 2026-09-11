use flowgen_core::config::ConfigExt;
use serde::{Deserialize, Deserializer, Serialize};
use std::{path::PathBuf, time::Duration};

/// Errors that can occur during configuration parsing.
#[derive(thiserror::Error, Debug)]
#[non_exhaustive]
pub enum ConfigError {
    #[error("Failed to parse backoff duration '{value}': {source}")]
    BackoffDurationParse {
        value: String,
        #[source]
        source: humantime::DurationError,
    },
}

/// Deserializes a list of human-readable duration strings into a vector of durations.
/// Accepts formats like ["1s", "5s", "30s", "1m", "5m"] for NATS consumer backoff schedules.
fn deserialize_backoff<'de, D>(deserializer: D) -> Result<Vec<Duration>, D::Error>
where
    D: Deserializer<'de>,
{
    let strings: Vec<String> = Vec::deserialize(deserializer)?;
    strings
        .into_iter()
        .map(|s| {
            humantime::parse_duration(&s).map_err(|source| {
                serde::de::Error::custom(ConfigError::BackoffDurationParse { value: s, source })
            })
        })
        .collect()
}

/// Serializes a vector of durations as human-readable strings.
/// This ensures ConfigExt::render JSON round-trip compatibility with deserialize_backoff.
fn serialize_backoff<S>(durations: &Vec<Duration>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(durations.len()))?;
    for d in durations {
        seq.serialize_element(&humantime::format_duration(*d).to_string())?;
    }
    seq.end()
}

/// Default NATS server URL function for serde.
fn default_nats_url() -> String {
    crate::client::DEFAULT_NATS_URL.to_string()
}

/// Default number of messages the JetStream pull consumer asks for in each
/// batch request. The continuous stream keeps a single batch in flight; when
/// it is consumed or expires, the next batch is requested automatically.
const DEFAULT_MAX_MESSAGES_PER_BATCH: usize = 200;

/// Default batch request expiry for the JetStream pull consumer stream.
///
/// This is what prevents the subscriber from spinning: instead of returning
/// immediately when no messages are available, the server holds the request
/// open for this duration and either delivers messages or a timeout status.
const DEFAULT_BATCH_EXPIRES: Duration = Duration::from_secs(30);

/// Default value for [`Config::max_messages_per_batch`].
fn default_max_messages_per_batch() -> usize {
    DEFAULT_MAX_MESSAGES_PER_BATCH
}

/// Default value for [`Config::batch_expires`].
fn default_batch_expires() -> Duration {
    DEFAULT_BATCH_EXPIRES
}

/// Unified configuration for both NATS JetStream publisher and subscriber tasks.
#[derive(PartialEq, Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    /// The unique name / identifier of the task.
    pub name: String,
    /// Optional path to credentials file containing NATS
    /// authentication details.
    #[serde(default)]
    pub credentials_path: Option<PathBuf>,
    /// NATS server URL (e.g., "nats://localhost:4222"). Defaults to "localhost:4222".
    #[serde(default = "default_nats_url")]
    pub url: String,
    /// Subject name for publishing or subscribing to messages.
    pub subject: String,
    /// Optional stream configuration.
    /// For publishers: if provided, ensures the stream exists.
    /// For subscribers: required to specify the stream to consume from.
    pub stream: Option<StreamOptions>,
    /// Durable consumer name (subscriber only).
    pub durable_name: Option<String>,
    /// Maximum number of messages per batch request for the continuous pull
    /// stream (subscriber only). Higher values improve throughput; lower
    /// values reduce the number of messages buffered in memory and the time
    /// before a shutdown cleanly finishes the current batch.
    #[serde(default = "default_max_messages_per_batch")]
    pub max_messages_per_batch: usize,
    /// How long each batch request waits on the server before returning empty
    /// (subscriber only). This prevents empty fetch loops by asking NATS to
    /// hold the request open until messages arrive or the timeout elapses.
    /// Accepts duration strings: "5s", "30s", "1m", etc.
    #[serde(default = "default_batch_expires", with = "humantime_serde")]
    pub batch_expires: Duration,
    /// Maximum number of unacknowledged messages allowed (subscriber only).
    /// Limits concurrent in-flight messages across all consumer instances.
    /// Set to 1 to prevent duplicate processing in multi-pod deployments.
    /// Default: None (uses NATS server default, typically 1000).
    pub max_ack_pending: Option<i64>,
    /// Maximum number of waiting pull requests allowed (subscriber only).
    /// Limits queued fetch requests when no messages are available.
    /// Prevents resource exhaustion with many idle consumers.
    /// Default: None (uses NATS server default, typically 512).
    pub max_waiting: Option<i64>,
    /// Delay between message batch fetches (subscriber only).
    ///
    /// Deprecated: the subscriber now uses a continuous pull stream with a
    /// server-side batch expiry, so there are no empty fetch loops to slow
    /// down. Use `throttle` to rate-limit individual messages instead.
    /// Kept for backward compatibility and ignored by the handler.
    /// Accepts duration strings: "100ms", "1s", "5m", etc.
    #[serde(default, with = "humantime_serde")]
    pub delay: Option<Duration>,
    /// Throttle individual message processing (subscriber only).
    /// Enforces a fixed delay between each message.
    /// Accepts duration strings: "100ms", "1s", etc.
    #[serde(default, with = "humantime_serde")]
    pub throttle: Option<Duration>,
    /// Acknowledgment timeout used for both publisher and subscriber contexts.
    /// For publishers: timeout for receiving publish acknowledgments from the server.
    /// For subscribers: sets the NATS consumer Ack Wait duration and the internal
    /// flow completion timeout. If not specified, uses the NATS default (30s).
    /// Accepts duration strings: "5s", "30s", "1m", "2m", etc.
    #[serde(default, with = "humantime_serde")]
    pub ack_timeout: Option<Duration>,
    /// Maximum number of times a message will be delivered before being discarded
    /// or sent to a dead letter subject (subscriber only).
    /// Set to -1 for unlimited redelivery (NATS default).
    /// Requires a dead letter configuration on the NATS server side to avoid data loss.
    pub max_deliver: Option<i64>,
    /// Redelivery backoff schedule (subscriber only).
    /// Defines delays between redelivery attempts (e.g., ["1s", "5s", "30s", "1m", "5m"]).
    /// If fewer entries than max_deliver, the last entry repeats for subsequent attempts.
    /// NATS natively supports this via consumer backoff config.
    #[serde(
        default,
        deserialize_with = "deserialize_backoff",
        serialize_with = "serialize_backoff",
        skip_serializing_if = "Vec::is_empty"
    )]
    pub backoff: Vec<Duration>,
    /// Optional list of upstream task names this task depends on.
    /// When set, this task only receives events from the named tasks.
    /// When not set, the task receives from the previous task in the list (linear chain).
    #[serde(default)]
    pub depends_on: Option<Vec<String>>,
    /// Optional NATS message ID for server-side deduplication.
    /// Can be a static string or templated from event data (e.g.
    /// `"{{event.data.record_id}}"`, `"fixed-key"`).
    /// When set, overrides `event.id` as the `Nats-Msg-Id` header on publish.
    /// Requires `duplicate_window` on the stream to take effect.
    #[serde(default)]
    pub msg_id: Option<String>,
    /// Optional retry configuration (overrides app-level retry config).
    #[serde(default)]
    pub retry: Option<flowgen_core::retry::RetryConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            name: String::new(),
            credentials_path: None,
            url: default_nats_url(),
            subject: String::new(),
            stream: None,
            durable_name: None,
            max_messages_per_batch: default_max_messages_per_batch(),
            max_ack_pending: None,
            max_waiting: None,
            batch_expires: default_batch_expires(),
            delay: None,
            throttle: None,
            ack_timeout: None,
            max_deliver: None,
            backoff: Vec::new(),
            depends_on: None,
            msg_id: None,
            retry: None,
        }
    }
}

/// Type alias for backward compatibility with publisher code.
pub type Publisher = Config;

/// Type alias for backward compatibility with subscriber code.
pub type Subscriber = Config;

impl ConfigExt for Config {}

#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct StreamOptions {
    /// Stream name.
    pub name: String,
    /// Stream description.
    pub description: Option<String>,
    /// Subject patterns for the stream (can include wildcards).
    /// Required for publishers when creating a stream.
    pub subjects: Vec<String>,
    /// Maximum age of messages (e.g., "30d", "24h", "7200s").
    #[serde(default, with = "humantime_serde")]
    pub max_age: Option<Duration>,
    /// Maximum number of messages per subject.
    pub max_messages_per_subject: Option<i64>,
    /// Maximum number of messages in the stream.
    pub max_messages: Option<i64>,
    /// Maximum bytes the stream can store.
    pub max_bytes: Option<i64>,
    /// Maximum message size in bytes.
    pub max_message_size: Option<i32>,
    /// Maximum number of consumers allowed.
    pub max_consumers: Option<i32>,
    /// Whether to create or update the stream if it doesn't exist or differs.
    pub create_or_update: bool,
    /// Retention policy for the stream.
    /// If None during update, keeps the existing value.
    pub retention: Option<RetentionPolicy>,
    /// Discard policy for when stream limits are reached.
    /// If None during update, keeps the existing value.
    pub discard: Option<DiscardPolicy>,
    /// When `true`, prevents a message from being added to the stream
    /// if the `max_messages_per_subject` limit for the subject has been reached.
    /// Requires `discard: New` to be set.
    pub discard_new_per_subject: Option<bool>,
    /// Duplicate window (e.g., "120s", "2m", "1h").
    /// Prevents duplicate messages within this time window.
    #[serde(default, with = "humantime_serde")]
    pub duplicate_window: Option<Duration>,
    /// Allows publishing multiple messages in a single batch operation.
    pub allow_batch_publish: Option<bool>,
    /// Allows direct access to retrieve messages without using the consumer API.
    pub allow_direct: Option<bool>,
    /// Allows rollup headers for message compaction and consolidation.
    pub allow_rollup: Option<bool>,
    /// Disables the ability to delete individual messages from the stream.
    pub deny_delete: Option<bool>,
    /// Disables the ability to purge all messages from the stream.
    pub deny_purge: Option<bool>,
}

/// NATS JetStream retention policies.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RetentionPolicy {
    /// Limits retention based on stream limits (messages, bytes, age).
    #[default]
    Limits,
    /// Retains messages based on consumer interest.
    Interest,
    /// Retains messages until explicitly deleted.
    WorkQueue,
}

/// NATS JetStream discard policies.
#[derive(PartialEq, Clone, Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum DiscardPolicy {
    /// Discard old messages when stream is full.
    #[default]
    Old,
    /// Discard new messages when stream is full.
    New,
}

#[cfg(test)]
mod tests {

    use super::*;
    use std::path::PathBuf;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.name, String::new());
        assert_eq!(config.credentials_path, None);
        assert_eq!(config.subject, String::new());
        assert_eq!(config.stream, None);
        assert_eq!(config.durable_name, None);
        assert_eq!(
            config.max_messages_per_batch,
            DEFAULT_MAX_MESSAGES_PER_BATCH
        );
        assert_eq!(config.batch_expires, DEFAULT_BATCH_EXPIRES);
        assert_eq!(config.delay, None);
    }

    #[test]
    fn test_subscriber_creation() {
        let subscriber = Subscriber {
            name: "test_subscriber".to_string(),
            credentials_path: Some(PathBuf::from("/path/to/nats.creds")),
            subject: "test.subject".to_string(),
            stream: Some(StreamOptions {
                name: "test_stream".to_string(),
                subjects: vec!["test.>".to_string()],
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("test_consumer".to_string()),
            max_messages_per_batch: 100,
            delay: Some(Duration::from_secs(5)),
            ..Default::default()
        };

        assert_eq!(subscriber.name, "test_subscriber");
        assert_eq!(
            subscriber.credentials_path,
            Some(PathBuf::from("/path/to/nats.creds"))
        );
        assert_eq!(subscriber.subject, "test.subject");
        assert!(subscriber.stream.is_some());
        assert_eq!(subscriber.durable_name, Some("test_consumer".to_string()));
        assert_eq!(subscriber.max_messages_per_batch, 100);
        assert_eq!(subscriber.delay, Some(Duration::from_secs(5)));
    }

    #[test]
    fn test_subscriber_serialization() {
        let subscriber = Subscriber {
            name: "serial_sub".to_string(),
            credentials_path: Some(PathBuf::from("/credentials/nats.jwt")),
            subject: "my.subject.*".to_string(),
            stream: Some(StreamOptions {
                name: "my_stream".to_string(),
                subjects: vec!["my.>".to_string()],
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("my_durable".to_string()),
            max_messages_per_batch: 50,
            delay: Some(Duration::from_secs(10)),
            ..Default::default()
        };

        let json = serde_json::to_string(&subscriber).unwrap();
        let deserialized: Subscriber = serde_json::from_str(&json).unwrap();
        assert_eq!(subscriber, deserialized);
    }

    #[test]
    fn test_subscriber_clone() {
        let subscriber = Subscriber {
            name: "clone_sub".to_string(),
            credentials_path: Some(PathBuf::from("/test/creds.jwt")),
            subject: "clone.subject".to_string(),
            stream: Some(StreamOptions {
                name: "clone_stream".to_string(),
                subjects: vec!["clone.>".to_string()],
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("clone_consumer".to_string()),
            max_messages_per_batch: 25,
            delay: None,
            ..Default::default()
        };

        let cloned = subscriber.clone();
        assert_eq!(subscriber, cloned);
    }

    #[test]
    fn test_publisher_default() {
        let publisher = Publisher::default();
        assert_eq!(publisher.name, String::new());
        assert_eq!(publisher.credentials_path, None);
        assert_eq!(publisher.subject, String::new());
        assert_eq!(publisher.stream, None);
        assert_eq!(publisher.durable_name, None);
        assert_eq!(
            publisher.max_messages_per_batch,
            DEFAULT_MAX_MESSAGES_PER_BATCH
        );
        assert_eq!(publisher.delay, None);
    }

    #[test]
    fn test_publisher_creation() {
        let stream_opts = StreamOptions {
            name: "pub_stream".to_string(),
            description: Some("Test publisher stream".to_string()),
            subjects: vec!["pub.subject.1".to_string(), "pub.subject.2".to_string()],
            max_age: Some(Duration::from_secs(3600)),
            max_messages_per_subject: Some(100),
            create_or_update: true,
            retention: Some(RetentionPolicy::Limits),
            discard: Some(DiscardPolicy::Old),
            ..Default::default()
        };

        let publisher = Publisher {
            name: "test_publisher".to_string(),
            credentials_path: Some(PathBuf::from("/path/to/pub.creds")),
            subject: "pub.subject.1".to_string(),
            stream: Some(stream_opts.clone()),
            durable_name: None,
            delay: None,
            ..Default::default()
        };

        assert_eq!(publisher.name, "test_publisher");
        assert_eq!(
            publisher.credentials_path,
            Some(PathBuf::from("/path/to/pub.creds"))
        );
        assert_eq!(publisher.subject, "pub.subject.1");
        assert!(publisher.stream.is_some());
        if let Some(stream) = publisher.stream {
            assert_eq!(stream.name, "pub_stream");
            assert_eq!(stream.subjects, vec!["pub.subject.1", "pub.subject.2"]);
            assert_eq!(stream.max_age, Some(Duration::from_secs(3600)));
            assert_eq!(stream.max_messages_per_subject, Some(100));
        }
    }

    #[test]
    fn test_publisher_serialization() {
        let publisher = Publisher {
            name: "serial_pub".to_string(),
            credentials_path: Some(PathBuf::from("/creds/publisher.jwt")),
            subject: "test.serialize".to_string(),
            stream: Some(StreamOptions {
                name: "serialization_stream".to_string(),
                description: Some("Serialization test".to_string()),
                subjects: vec!["test.serialize".to_string()],
                max_age: Some(Duration::from_secs(7200)),
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: None,
            delay: None,
            ..Default::default()
        };

        let json = serde_json::to_string(&publisher).unwrap();
        let deserialized: Publisher = serde_json::from_str(&json).unwrap();
        assert_eq!(publisher, deserialized);
    }

    #[test]
    fn test_publisher_clone() {
        let publisher = Publisher {
            name: "clone_pub".to_string(),
            credentials_path: Some(PathBuf::from("/test/pub.creds")),
            subject: "clone.subject".to_string(),
            stream: Some(StreamOptions {
                name: "clone_stream".to_string(),
                subjects: vec!["clone.subject".to_string()],
                max_age: Some(Duration::from_secs(1800)),
                max_messages_per_subject: Some(1),
                create_or_update: true,
                retention: Some(RetentionPolicy::WorkQueue),
                discard: Some(DiscardPolicy::New),
                ..Default::default()
            }),
            durable_name: None,
            delay: None,
            ..Default::default()
        };

        let cloned = publisher.clone();
        assert_eq!(publisher, cloned);
    }

    #[test]
    fn test_publisher_without_stream() {
        let publisher = Publisher {
            name: "no_stream_pub".to_string(),
            credentials_path: Some(PathBuf::from("/no/stream.creds")),
            subject: "simple.subject".to_string(),
            stream: None,
            durable_name: None,
            delay: None,
            ..Default::default()
        };

        assert_eq!(publisher.subject, "simple.subject");
        assert!(publisher.stream.is_none());
    }

    #[test]
    fn test_stream_with_multiple_subjects() {
        let stream_opts = StreamOptions {
            name: "multi_stream".to_string(),
            subjects: vec![
                "subject.1".to_string(),
                "subject.2".to_string(),
                "subject.3".to_string(),
            ],
            max_age: Some(Duration::from_secs(86400)),
            create_or_update: true,
            retention: Some(RetentionPolicy::Interest),
            discard: Some(DiscardPolicy::Old),
            ..Default::default()
        };

        let publisher = Publisher {
            name: "multi_pub".to_string(),
            credentials_path: Some(PathBuf::from("/multi/subjects.creds")),
            subject: "subject.1".to_string(),
            stream: Some(stream_opts),
            durable_name: None,
            delay: None,
            ..Default::default()
        };

        assert!(publisher.stream.is_some());
        if let Some(stream) = publisher.stream {
            assert_eq!(stream.subjects.len(), 3);
            assert!(stream.subjects.contains(&"subject.2".to_string()));
        }
    }

    #[test]
    fn test_config_equality() {
        let sub1 = Subscriber {
            name: "eq_sub".to_string(),
            credentials_path: Some(PathBuf::from("/path/creds.jwt")),
            subject: "subject1".to_string(),
            stream: Some(StreamOptions {
                name: "stream1".to_string(),
                subjects: vec!["subject1".to_string()],
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("consumer1".to_string()),
            max_messages_per_batch: 10,
            delay: Some(Duration::from_secs(1)),
            ..Default::default()
        };

        let sub2 = Subscriber {
            name: "eq_sub".to_string(),
            credentials_path: Some(PathBuf::from("/path/creds.jwt")),
            subject: "subject1".to_string(),
            stream: Some(StreamOptions {
                name: "stream1".to_string(),
                subjects: vec!["subject1".to_string()],
                create_or_update: true,
                retention: Some(RetentionPolicy::Limits),
                discard: Some(DiscardPolicy::Old),
                ..Default::default()
            }),
            durable_name: Some("consumer1".to_string()),
            max_messages_per_batch: 10,
            delay: Some(Duration::from_secs(1)),
            ..Default::default()
        };

        assert_eq!(sub1, sub2);
    }

    #[test]
    fn test_config_default_max_deliver_and_backoff() {
        let config = Config::default();
        assert_eq!(config.max_deliver, None);
        assert!(config.backoff.is_empty());
    }

    #[test]
    fn test_subscriber_with_max_deliver_and_backoff() {
        let json = r#"{
            "name": "test_sub",
            "credentials_path": "/path/to/creds.jwt",
            "subject": "test.subject",
            "max_deliver": 5,
            "backoff": ["1s", "5s", "30s", "1m", "5m"]
        }"#;
        let config: Config = serde_json::from_str(json).expect("Failed to parse config.");
        assert_eq!(config.max_deliver, Some(5));
        assert_eq!(config.backoff.len(), 5);
        assert_eq!(config.backoff[0], Duration::from_secs(1));
        assert_eq!(config.backoff[1], Duration::from_secs(5));
        assert_eq!(config.backoff[2], Duration::from_secs(30));
        assert_eq!(config.backoff[3], Duration::from_secs(60));
        assert_eq!(config.backoff[4], Duration::from_secs(300));
    }

    #[test]
    fn test_backoff_empty_when_omitted() {
        let json = r#"{
            "name": "test_sub",
            "credentials_path": "/path/to/creds.jwt",
            "subject": "test.subject"
        }"#;
        let config: Config = serde_json::from_str(json).expect("Failed to parse config.");
        assert!(config.backoff.is_empty());
    }

    #[test]
    fn test_backoff_invalid_duration_returns_error() {
        let json = r#"{
            "name": "test_sub",
            "credentials_path": "/path/to/creds.jwt",
            "subject": "test.subject",
            "backoff": ["1s", "invalid", "5s"]
        }"#;
        let result: Result<Config, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_backoff_skipped_in_serialization_when_empty() {
        let config = Config {
            name: "test".to_string(),
            credentials_path: Some(PathBuf::from("/path/to/creds.jwt")),
            subject: "test.subject".to_string(),
            ..Default::default()
        };
        let json = serde_json::to_string(&config).expect("Failed to serialize config.");
        assert!(!json.contains("backoff"));
    }
}
