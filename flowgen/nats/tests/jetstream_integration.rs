//! Integration tests for the NATS JetStream Publisher / Subscriber
//! processors against a real NATS server in a Docker container.
//!
//! Each test starts a fresh container so streams never collide.
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips them; CI runs the ignored set explicitly:
//!
//!     cargo test -p flowgen_nats --test jetstream_integration -- --ignored --nocapture

use flowgen_core::event::{EventBuilder, EventData};
use flowgen_nats::jetstream::config::{
    Config as JsConfig, DiscardPolicy, RetentionPolicy, StreamOptions,
};
use flowgen_nats::jetstream::publisher::PublisherBuilder;
use flowgen_nats::jetstream::subscriber::SubscriberBuilder;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::sync::mpsc;

/// Starts a NATS 2.11.8 container with JetStream enabled and returns
/// its connection URL.
async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
    let (container, url, _) = start_nats_monitored().await;
    (container, url)
}

/// Same server, with the HTTP monitoring endpoint enabled. Returns the
/// client URL and the `/varz` monitoring URL, which reports server-side
/// counters no client-side assertion can reach.
async fn start_nats_monitored() -> (ContainerAsync<GenericImage>, String, String) {
    let container = GenericImage::new("nats", "2.11.8-alpine")
        .with_exposed_port(4222.tcp())
        .with_exposed_port(8222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js", "-m", "8222"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("map nats port");
    let monitor_port = container
        .get_host_port_ipv4(8222)
        .await
        .expect("map nats monitoring port");
    (
        container,
        format!("nats://127.0.0.1:{port}"),
        format!("http://127.0.0.1:{monitor_port}/varz"),
    )
}

/// Total messages the server has received, from `/varz`. Pull requests count
/// here, so the delta over an idle window is how often the subscriber asked
/// for work.
async fn server_in_msgs(varz_url: &str) -> u64 {
    let body = reqwest::get(varz_url)
        .await
        .expect("fetch varz")
        .text()
        .await
        .expect("varz body");
    let varz: serde_json::Value = serde_json::from_str(&body).expect("varz json");
    varz.get("in_msgs")
        .and_then(|v| v.as_u64())
        .expect("varz in_msgs")
}

/// Builds a TaskContext with an in-memory cache — enough for the
/// publisher/subscriber builders to initialise.
fn test_task_context() -> Arc<flowgen_core::task::context::TaskContext> {
    let task_manager = Arc::new(
        flowgen_core::task::manager::TaskManagerBuilder::new()
            .build()
            .expect("build TaskManager"),
    );
    let cache = Arc::new(flowgen_core::cache::memory::MemoryCache::new())
        as Arc<dyn flowgen_core::cache::Cache>;
    Arc::new(
        flowgen_core::task::context::TaskContextBuilder::new()
            .flow_name("test_flow".to_string())
            .task_manager(task_manager)
            .cache(cache)
            .build()
            .expect("build TaskContext"),
    )
}

/// Convenience: WorkQueue retention, `discard: Old`, no per-subject limit.
fn stream_options(name: &str, subject: &str) -> StreamOptions {
    StreamOptions {
        name: name.to_string(),
        subjects: vec![subject.to_string()],
        create_or_update: true,
        retention: Some(RetentionPolicy::WorkQueue),
        discard: Some(DiscardPolicy::Old),
        ..Default::default()
    }
}

/// Spawns a publisher from the given config and returns the input
/// sender, output receiver, and the join handle.
async fn spawn_publisher(
    config: Arc<JsConfig>,
) -> (
    mpsc::Sender<flowgen_core::event::Event>,
    mpsc::Receiver<flowgen_core::event::Event>,
    tokio::task::JoinHandle<()>,
) {
    let (in_tx, in_rx) = mpsc::channel(4);
    let (out_tx, out_rx) = mpsc::channel(4);
    let publisher = PublisherBuilder::new()
        .config(config)
        .receiver(in_rx)
        .sender(out_tx)
        .task_id(0)
        .task_type("nats_jetstream_publisher")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build publisher");
    let handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = publisher.run().await;
    });
    (in_tx, out_rx, handle)
}

/// Connects directly to NATS and fetches stream info — used to verify
/// server-side state independently of the publisher.
async fn stream_info(url: &str, stream_name: &str) -> async_nats::jetstream::stream::Info {
    let client = async_nats::connect(url).await.expect("connect");
    let js = async_nats::jetstream::new(client);
    let mut stream = js.get_stream(stream_name).await.expect("get stream");
    stream.info().await.expect("stream info").clone()
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn publisher_writes_event_to_stream() {
    let (_nats, url) = start_nats().await;

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "pub.only".to_string(),
        stream: Some(stream_options("pub_only_stream", "pub.only")),
        ..Default::default()
    });
    let (in_tx, mut out_rx, handle) = spawn_publisher(pub_config).await;

    let event = EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"hello": "world"})))
        .subject("pub.only".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    in_tx.send(event).await.expect("send event");

    let ack_event = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("ack event within timeout")
        .expect("channel open");
    assert!(
        ack_event.error.is_none(),
        "unexpected error: {:?}",
        ack_event.error
    );

    let ack = ack_event.data_as_json().expect("ack data as json");
    assert_eq!(
        ack.get("stream").and_then(|s| s.as_str()),
        Some("pub_only_stream")
    );
    assert!(ack.get("sequence").and_then(|s| s.as_u64()).is_some());

    drop(in_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn publisher_and_subscriber_round_trip_delivers_the_message() {
    let (_nats, url) = start_nats().await;
    let stream = stream_options("rt_stream", "rt.subject");

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "rt.subject".to_string(),
        stream: Some(stream.clone()),
        ..Default::default()
    });
    let (pub_tx, _pub_out_rx, pub_handle) = spawn_publisher(pub_config).await;

    let sub_config = Arc::new(JsConfig {
        name: "subscriber".to_string(),
        url: url.clone(),
        subject: "rt.subject".to_string(),
        stream: Some(stream),
        durable_name: Some("rt_consumer".to_string()),
        max_messages_per_batch: 10,
        ack_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    });
    let (sub_out_tx, mut sub_out_rx) = mpsc::channel(4);
    let subscriber = SubscriberBuilder::new()
        .config(sub_config)
        .sender(sub_out_tx)
        .task_id(1)
        .task_type("nats_jetstream_subscriber")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build subscriber");
    let sub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = subscriber.run().await;
    });

    tokio::time::sleep(Duration::from_millis(500)).await;

    let event = EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"payload": 42})))
        .subject("rt.subject".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    pub_tx.send(event).await.expect("publish event");

    let delivered = tokio::time::timeout(Duration::from_secs(10), sub_out_rx.recv())
        .await
        .expect("subscriber must deliver the message")
        .expect("channel open");

    if let Some(arc) = delivered.completion_tx.as_ref() {
        arc.signal_completion(None);
    }
    assert_eq!(delivered.subject, "rt.subject");

    drop(pub_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    sub_handle.abort();
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn discard_new_per_subject_rejects_second_publish_to_same_subject() {
    let (_nats, url) = start_nats().await;

    let stream = StreamOptions {
        name: "dedup_per_subject".to_string(),
        subjects: vec!["dedup.>".to_string()],
        create_or_update: true,
        retention: Some(RetentionPolicy::Limits),
        discard: Some(DiscardPolicy::New),
        discard_new_per_subject: Some(true),
        max_messages_per_subject: Some(1),
        ..Default::default()
    };

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "dedup.record-123".to_string(),
        stream: Some(stream),
        retry: Some(flowgen_core::retry::RetryConfig {
            max_attempts: Some(1),
            ..Default::default()
        }),
        ..Default::default()
    });
    let (in_tx, mut out_rx, handle) = spawn_publisher(pub_config).await;

    let make_event = || {
        EventBuilder::new()
            .data(EventData::Json(
                serde_json::json!({"record_id": "record-123"}),
            ))
            .subject("dedup.record-123".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .expect("build event")
    };

    in_tx.send(make_event()).await.expect("send first event");
    let first = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("first ack within timeout")
        .expect("channel open");
    assert!(
        first.error.is_none(),
        "first publish to an empty subject must not error, got: {:?}",
        first.error
    );

    let info = stream_info(&url, "dedup_per_subject").await;
    assert_eq!(
        info.config.discard,
        async_nats::jetstream::stream::DiscardPolicy::New
    );
    assert!(info.config.discard_new_per_subject);
    assert_eq!(info.config.max_messages_per_subject, 1);
    assert_eq!(info.state.messages, 1);

    in_tx
        .send(make_event())
        .await
        .expect("send duplicate event");
    let second = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("duplicate publish must produce an event within timeout")
        .expect("channel open");
    assert!(
        second.error.is_some(),
        "second publish to a full subject must surface as an error event, got ack: {:?}",
        second.data_as_json()
    );

    let info = stream_info(&url, "dedup_per_subject").await;
    assert_eq!(info.state.messages, 1, "stream must still have 1 message");
    assert_eq!(
        info.state.first_sequence, 1,
        "first message must not be evicted"
    );
    assert_eq!(info.state.last_sequence, 1);

    drop(in_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn msg_id_template_deduplicates_within_duplicate_window() {
    let (_nats, url) = start_nats().await;

    let stream = StreamOptions {
        name: "msg_id_dedup".to_string(),
        subjects: vec!["msgid.>".to_string()],
        create_or_update: true,
        retention: Some(RetentionPolicy::Limits),
        discard: Some(DiscardPolicy::Old),
        duplicate_window: Some(Duration::from_secs(60)),
        ..Default::default()
    };

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "msgid.record-123".to_string(),
        msg_id: Some("{{event.data.record_id}}".to_string()),
        stream: Some(stream),
        ..Default::default()
    });
    let (in_tx, mut out_rx, handle) = spawn_publisher(pub_config).await;

    let event_a = EventBuilder::new()
        .data(EventData::Json(
            serde_json::json!({"record_id": "rec-1", "value": "a"}),
        ))
        .subject("msgid.record-123".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    in_tx.send(event_a).await.expect("send first event");

    let first = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("first ack within timeout")
        .expect("channel open");
    assert!(
        first.error.is_none(),
        "first publish must succeed: {:?}",
        first.error
    );
    let first_ack = first.data_as_json().expect("ack data as json");
    assert_eq!(
        first_ack.get("duplicate").and_then(|d| d.as_bool()),
        Some(false)
    );

    let event_b = EventBuilder::new()
        .data(EventData::Json(
            serde_json::json!({"record_id": "rec-1", "value": "b"}),
        ))
        .subject("msgid.record-123".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    in_tx.send(event_b).await.expect("send duplicate event");

    let second = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("second ack within timeout")
        .expect("channel open");
    assert!(
        second.error.is_none(),
        "duplicate publish must not error: {:?}",
        second.error
    );
    let second_ack = second.data_as_json().expect("ack data as json");
    assert_eq!(
        second_ack.get("duplicate").and_then(|d| d.as_bool()),
        Some(true),
        "second publish with same msg_id must be flagged as duplicate"
    );

    let info = stream_info(&url, "msg_id_dedup").await;
    assert_eq!(info.state.messages, 1, "stream must have 1 message, not 2");

    drop(in_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn msg_id_template_different_keys_are_not_deduplicated() {
    let (_nats, url) = start_nats().await;

    let stream = StreamOptions {
        name: "msg_id_distinct".to_string(),
        subjects: vec!["msgid.>".to_string()],
        create_or_update: true,
        retention: Some(RetentionPolicy::Limits),
        discard: Some(DiscardPolicy::Old),
        duplicate_window: Some(Duration::from_secs(60)),
        ..Default::default()
    };

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "msgid.record".to_string(),
        msg_id: Some("{{event.data.record_id}}".to_string()),
        stream: Some(stream),
        ..Default::default()
    });
    let (in_tx, mut out_rx, handle) = spawn_publisher(pub_config).await;

    for id in ["rec-1", "rec-2"] {
        let event = EventBuilder::new()
            .data(EventData::Json(serde_json::json!({"record_id": id})))
            .subject("msgid.record".to_string())
            .task_id(0)
            .task_type("test")
            .build()
            .expect("build event");
        in_tx.send(event).await.expect("send event");

        let ack_event = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
            .await
            .expect("ack within timeout")
            .expect("channel open");
        assert!(
            ack_event.error.is_none(),
            "publish must succeed: {:?}",
            ack_event.error
        );

        let ack = ack_event.data_as_json().expect("ack data as json");
        assert_eq!(
            ack.get("duplicate").and_then(|d| d.as_bool()),
            Some(false),
            "publish with a new msg_id must not be flagged as duplicate"
        );
    }

    let info = stream_info(&url, "msg_id_distinct").await;
    assert_eq!(
        info.state.messages, 2,
        "stream must have 2 distinct messages"
    );

    drop(in_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
}

/// An idle subscriber must wait on the server rather than poll it.
///
/// This is the regression behind the `fetch()` -> continuous-stream switch:
/// `fetch()` returned immediately when the queue was empty, so an idle
/// consumer re-requested in a tight loop and burned CPU and round-trips. The
/// pull stream instead parks a batch request until it fills or
/// `batch_expires` elapses.
///
/// The assertion is on request *volume*, taken from the server's own
/// `in_msgs` counter, because that is what the bug actually was. A parked
/// consumer re-requests only once per expiry; a spinning one issues hundreds
/// over the same window, so the threshold sits far below the old behaviour
/// and far above the new one.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn idle_subscriber_does_not_poll_the_server() {
    let (_nats, url, varz) = start_nats_monitored().await;
    let stream = stream_options("idle_stream", "idle.subject");

    let idle_window = Duration::from_secs(5);
    let batch_expires = Duration::from_secs(30);

    let sub_config = Arc::new(JsConfig {
        name: "subscriber".to_string(),
        url: url.clone(),
        subject: "idle.subject".to_string(),
        stream: Some(stream),
        durable_name: Some("idle_consumer".to_string()),
        max_messages_per_batch: 10,
        // Longer than the idle window, so a correct subscriber renews its
        // request at most once while being observed.
        batch_expires,
        ..Default::default()
    });
    let (sub_out_tx, mut sub_out_rx) = mpsc::channel(4);
    let subscriber = SubscriberBuilder::new()
        .config(sub_config)
        .sender(sub_out_tx)
        .task_id(1)
        .task_type("nats_jetstream_subscriber")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build subscriber");
    let sub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = subscriber.run().await;
    });

    // Let connection setup and the first batch request settle, so they are
    // not counted against the idle window.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let before = server_in_msgs(&varz).await;
    tokio::time::sleep(idle_window).await;
    let after = server_in_msgs(&varz).await;

    let requests = after - before;
    assert!(
        requests < 20,
        "an idle subscriber sent {requests} messages in {}s; a parked pull \
         request renews about once per {}s expiry, so this many means it is \
         polling the server rather than waiting on it",
        idle_window.as_secs(),
        batch_expires.as_secs(),
    );

    assert!(
        sub_out_rx.try_recv().is_err(),
        "an idle subscriber must not emit events when nothing was published"
    );

    sub_handle.abort();
}

/// The parked request must not stall delivery: a message published after the
/// consumer has gone idle still arrives on the already-open batch request.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn idle_subscriber_still_delivers_a_message_published_later() {
    let (_nats, url) = start_nats().await;
    let stream = stream_options("wake_stream", "wake.subject");

    let sub_config = Arc::new(JsConfig {
        name: "subscriber".to_string(),
        url: url.clone(),
        subject: "wake.subject".to_string(),
        stream: Some(stream.clone()),
        durable_name: Some("wake_consumer".to_string()),
        max_messages_per_batch: 10,
        batch_expires: Duration::from_secs(30),
        ack_timeout: Some(Duration::from_secs(5)),
        ..Default::default()
    });
    let (sub_out_tx, mut sub_out_rx) = mpsc::channel(4);
    let subscriber = SubscriberBuilder::new()
        .config(sub_config)
        .sender(sub_out_tx)
        .task_id(1)
        .task_type("nats_jetstream_subscriber")
        .task_context(test_task_context())
        .build()
        .await
        .expect("build subscriber");
    let sub_handle = tokio::spawn(async move {
        use flowgen_core::task::runner::Runner;
        let _ = subscriber.run().await;
    });

    // Go idle first, so the message lands on a request that is already parked
    // rather than on a fresh one.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let pub_config = Arc::new(JsConfig {
        name: "publisher".to_string(),
        url: url.clone(),
        subject: "wake.subject".to_string(),
        stream: Some(stream),
        ..Default::default()
    });
    let (pub_tx, _pub_out_rx, pub_handle) = spawn_publisher(pub_config).await;

    let event = EventBuilder::new()
        .data(EventData::Json(serde_json::json!({"payload": 7})))
        .subject("wake.subject".to_string())
        .task_id(0)
        .task_type("test")
        .build()
        .expect("build event");
    pub_tx.send(event).await.expect("publish event");

    let delivered = tokio::time::timeout(Duration::from_secs(10), sub_out_rx.recv())
        .await
        .expect("a parked batch request must still deliver a later message")
        .expect("channel open");

    if let Some(arc) = delivered.completion_tx.as_ref() {
        arc.signal_completion(None);
    }
    assert_eq!(delivered.subject, "wake.subject");

    drop(pub_tx);
    let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
    sub_handle.abort();
}
