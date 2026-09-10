//! Integration tests for `PeerRegistry` across every `Cache` backend it can
//! run on.
//!
//! `peer_registry_behaves_correctly` is the shared assertion body — new
//! backends (Redis, redb, ...) get a new backend and a thin `#[tokio::test]`
//! wrapper, not a new copy of the test logic. Coverage that only one backend
//! can exercise (TTL-based dead-peer expiry — `MemoryCache` does not
//! implement TTL at all) lives in its own backend-specific test.

use flowgen_core::cache::Cache;
use flowgen_core::identity::FlowIdentity;
use flowgen_core::peer::PeerRegistry;
use std::sync::Arc;
use std::time::Duration;

async fn peer_registry_behaves_correctly(cache: Arc<dyn Cache>) {
    let pod_a = Arc::new(PeerRegistry::new(cache.clone(), "pod-a".to_string()));
    let pod_b = Arc::new(PeerRegistry::new(cache.clone(), "pod-b".to_string()));
    pod_a.register().await.expect("pod-a registers");
    pod_b.register().await.expect("pod-b registers");

    let peers = pod_a.list_peers().await.expect("list_peers");
    assert_eq!(peers, vec!["pod-a".to_string(), "pod-b".to_string()]);

    let flow = FlowIdentity::new("integration-test-flow");
    let a_owner = pod_a.is_preferred_owner(&flow).await.expect("a check");
    let b_owner = pod_b.is_preferred_owner(&flow).await.expect("b check");
    assert_ne!(
        a_owner, b_owner,
        "exactly one of the two registered peers must be preferred"
    );

    pod_b.deregister().await.expect("pod-b deregisters");
    let peers = pod_a
        .list_peers()
        .await
        .expect("list_peers after deregister");
    assert_eq!(peers, vec!["pod-a".to_string()]);
    assert!(
        pod_a
            .is_preferred_owner(&flow)
            .await
            .expect("a check after deregister"),
        "sole remaining peer must be preferred for every flow"
    );
}

#[tokio::test]
async fn peer_registry_behaves_correctly_on_memory_cache() {
    let cache: Arc<dyn Cache> = Arc::new(flowgen_core::cache::memory::MemoryCache::new());
    peer_registry_behaves_correctly(cache).await;
}

#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn peer_registry_behaves_correctly_on_nats() {
    let (_nats, url) = start_nats().await;
    let config = app_config_with_cache(url);
    let cache = flowgen::app::App::init_cache(&config, None)
        .await
        .expect("nats cache init");
    peer_registry_behaves_correctly(cache).await;
}

/// The system bucket must exist whenever the cache does, even with flows
/// loaded from the filesystem (`flows.cache: None` — the shape every
/// git_sync deployment uses). It used to be created only as a side effect of
/// cache-backed flow loading, so those deployments silently kept leases and
/// peer keys in the runtime bucket, within reach of `ctx.cache`.
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn system_bucket_is_separate_from_runtime_without_cache_backed_flows() {
    let (_nats, url) = start_nats().await;
    let config = app_config_with_cache(url);
    assert!(
        config.flows.cache.is_none(),
        "this test only means something with flows loaded from the filesystem"
    );

    let system = flowgen::app::App::init_cache(
        &config,
        Some(&config.cache.as_ref().unwrap().system.db_name),
    )
    .await
    .expect("system cache init");
    let runtime = flowgen::app::App::init_cache(&config, None)
        .await
        .expect("runtime cache init");

    let registry = PeerRegistry::new(Arc::clone(&system), "pod-a".to_string());
    registry.register().await.expect("pod-a registers");

    assert_eq!(
        registry.list_peers().await.expect("list_peers"),
        vec!["pod-a".to_string()]
    );
    assert!(
        runtime
            .list_keys("peers.")
            .await
            .expect("runtime list_keys")
            .is_empty(),
        "peer keys must not be reachable from the runtime bucket that ctx.cache exposes"
    );
}

/// `MemoryCache` does not implement TTL at all (`put`'s `ttl_secs` argument
/// is ignored), so a crashed pod's registration never expires there and this
/// scenario can only be exercised against a real backend.
///
/// Uses a 1s `tombstone_ttl` (== `limit_markers`), not the 3600s production
/// default — that value sets the server's sweep cadence for expired
/// per-message TTL entries, separately from the per-message TTL itself,
/// so a 3600s tombstone_ttl would make a 1s-TTL peer registration linger
/// for up to an hour (see `flowgen/nats/tests/cache_integration.rs`'s
/// `put_with_ttl_expires_the_value`).
#[tokio::test]
#[ignore = "requires Docker daemon; run in CI via `cargo test -- --ignored`"]
async fn dead_peer_drops_out_after_ttl_expiry_on_nats() {
    let (_nats, url) = start_nats().await;
    let mut config = app_config_with_cache(url);
    if let Some(cache_config) = config.cache.as_mut() {
        cache_config.tombstone_ttl = Some(Duration::from_secs(1));
    }
    let cache = flowgen::app::App::init_cache(&config, None)
        .await
        .expect("nats cache init");

    let live = PeerRegistry::new(cache.clone(), "pod-live".to_string());
    live.register().await.expect("live pod registers");

    let hard_killed_peer_key = "peers.pod-crashed";
    cache
        .put(
            hard_killed_peer_key,
            bytes::Bytes::from_static(b"pod-crashed"),
            Some(1),
        )
        .await
        .expect("crashed pod registers with a short TTL and is never renewed or deregistered");

    let peers = live.list_peers().await.expect("list_peers before expiry");
    assert_eq!(
        peers,
        vec!["pod-crashed".to_string(), "pod-live".to_string()]
    );

    let mut peers_after = peers;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        peers_after = live.list_peers().await.expect("list_peers while polling");
        if peers_after == vec!["pod-live".to_string()] {
            break;
        }
    }
    assert_eq!(
        peers_after,
        vec!["pod-live".to_string()],
        "crashed pod's registration must expire and drop out of the peer list"
    );
}

async fn start_nats() -> (
    testcontainers::ContainerAsync<testcontainers::GenericImage>,
    String,
) {
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::AsyncRunner;
    use testcontainers::{GenericImage, ImageExt};

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

fn app_config_with_cache(url: String) -> flowgen::config::AppConfig {
    flowgen::config::AppConfig {
        cache: Some(flowgen::config::CacheOptions {
            enabled: true,
            cache_type: flowgen::config::CacheType::Nats,
            credentials_path: None,
            url,
            db_name: None,
            runtime: flowgen::config::BucketOptions::default(),
            system: flowgen::config::SystemBucketOptions::default(),
            history: Some(64),
            tombstone_ttl: Some(Duration::from_secs(3600)),
        }),
        flows: flowgen::config::FlowOptions {
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
