//! Integration tests for `App::init_cache` against a real NATS server.
//!
//! Covers:
//!
//! - Both cache buckets (`flowgen_cache` runtime + `flowgen_system` loader)
//!   accept per-message TTL puts. This is the regression from the incident
//!   where the two `Cache::init` call sites had diverged on the `history`
//!   and `tombstone_ttl` overrides — the system bucket ended up with
//!   `AllowMsgTTL=false` and every per-message TTL put on it failed with
//!   `per-message TTL is disabled (code 400, error code 10166)`. The
//!   unified `App::init_cache(config, Option<&str>)` now flows both bucket
//!   names through the same builder.
//!
//! - Disabling the cache falls back to the in-memory backend without
//!   touching NATS.
//!
//! Requires a running Docker daemon. Marked `#[ignore]` so a default
//! `cargo test` skips it; CI runs the ignored set explicitly.

use bytes::Bytes;
use flowgen::app::App;
use flowgen::config::{
    AppConfig, BucketOptions, CacheOptions, CacheType, FlowOptions, SystemBucketOptions,
};
use flowgen_core::cache::Cache;
use std::sync::Arc;
use std::time::Duration;
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

async fn start_nats() -> (ContainerAsync<GenericImage>, String) {
    let container = GenericImage::new("nats", "2.11.8-alpine")
        .with_exposed_port(4222.tcp())
        .with_wait_for(WaitFor::message_on_stderr("Server is ready"))
        .with_cmd(["-js"])
        .start()
        .await
        .expect("start nats container");
    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("map nats port");
    (container, format!("nats://127.0.0.1:{port}"))
}

fn app_config_with_cache(url: String) -> AppConfig {
    AppConfig {
        cache: Some(CacheOptions {
            enabled: true,
            cache_type: CacheType::Nats,
            credentials_path: None,
            url,
            db_name: None,
            runtime: BucketOptions::default(),
            system: SystemBucketOptions::default(),
            history: Some(64),
            tombstone_ttl: Some(Duration::from_secs(3600)),
        }),
        flows: FlowOptions {
            path: None,
            cache: None,
        },
        resources: None,
        http_server: None,
        mcp_server: None,
        ai_gateway: None,
        web: None,
        health: Default::default(),
        retry: None,
        event_buffer_size: None,
        telemetry: None,
    }
}

async fn probe_bucket_supports_ttl(cache: Arc<dyn Cache>, label: &str) {
    // Per-message TTL puts fail with `per-message TTL is disabled`
    // when the underlying stream was created without
    // `AllowMsgTTL=true`. If our tombstone_ttl config did not
    // propagate through `App::init_cache`, this is where it would
    // manifest.
    cache
        .put(
            &format!("probe.{label}"),
            Bytes::from_static(b"ok"),
            Some(60),
        )
        .await
        .unwrap_or_else(|e| panic!("{label} bucket must accept per-message TTL puts: {e}"));
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn init_cache_propagates_tombstone_ttl_to_every_bucket_it_creates() {
    let (_nats, url) = start_nats().await;
    let config = app_config_with_cache(url);

    // Two independent calls, two different bucket names. Both must
    // land on the same code path that reads the app's cache config.
    let runtime = App::init_cache(&config, None)
        .await
        .expect("runtime cache init");
    let flows = App::init_cache(&config, Some("flowgen_system"))
        .await
        .expect("flows cache init");
    let resources = App::init_cache(&config, Some("flowgen_resources"))
        .await
        .expect("resources cache init");

    probe_bucket_supports_ttl(runtime, "runtime").await;
    probe_bucket_supports_ttl(flows, "flows").await;
    probe_bucket_supports_ttl(resources, "resources").await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn init_cache_falls_back_to_memory_when_cache_is_disabled() {
    // Sanity check: when `cache.enabled: false`, `init_cache` must
    // return the in-memory backend even if a NATS URL is
    // configured, so the app boots offline.
    let mut config = app_config_with_cache("nats://unreachable.example:4222".to_string());
    if let Some(cache) = config.cache.as_mut() {
        cache.enabled = false;
    }

    let cache = App::init_cache(&config, None)
        .await
        .expect("memory fallback init");
    cache
        .put("probe", Bytes::from_static(b"ok"), None)
        .await
        .expect("memory backend must accept puts");
    assert_eq!(
        &cache.get("probe").await.expect("get").expect("present")[..],
        b"ok"
    );
}
