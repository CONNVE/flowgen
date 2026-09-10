//! Peer discovery and flow distribution via consistent hashing.
//!
//! Each worker pod registers itself in the shared cache under a TTL-backed key
//! (`peers.{identity}`). Before racing for a flow lease, the task manager
//! queries the peer list, sorts it, and uses `hash(flow_name) % peer_count` to
//! determine the preferred owner. Non-preferred pods defer their acquisition
//! attempt, giving the preferred pod a head start. If the preferred pod fails
//! to acquire within the deferral window, all pods fall back to normal racing.

use crate::identity::FlowIdentity;
use bytes::Bytes;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Prefix for all peer registration keys in the cache.
const PEER_KEY_PREFIX: &str = "peers.";

/// Default TTL for peer registrations (30 seconds).
/// Must be longer than the renewal interval to survive one missed renewal.
const DEFAULT_PEER_TTL_SECS: u64 = 30;

/// Default renewal interval for peer heartbeats (10 seconds).
const DEFAULT_PEER_RENEWAL_SECS: u64 = 10;

/// How long a non-preferred pod waits before falling back to normal lease
/// acquisition (seconds). Gives the preferred pod time to win the race.
const DEFAULT_DEFERRAL_SECS: u64 = 5;

/// Peer registry for automatic pod discovery and flow distribution.
#[derive(Clone)]
pub struct PeerRegistry {
    cache: Arc<dyn crate::cache::Cache>,
    identity: String,
    ttl_secs: u64,
    renewal_secs: u64,
    deferral_secs: u64,
}

impl std::fmt::Debug for PeerRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerRegistry")
            .field("identity", &self.identity)
            .field("ttl_secs", &self.ttl_secs)
            .field("renewal_secs", &self.renewal_secs)
            .finish()
    }
}

impl PeerRegistry {
    /// Creates a new peer registry.
    ///
    /// `identity` should match the executor's `holder_identity` (typically
    /// `$POD_NAME` or `$HOSTNAME`).
    pub fn new(cache: Arc<dyn crate::cache::Cache>, identity: String) -> Self {
        Self {
            cache,
            identity,
            ttl_secs: DEFAULT_PEER_TTL_SECS,
            renewal_secs: DEFAULT_PEER_RENEWAL_SECS,
            deferral_secs: DEFAULT_DEFERRAL_SECS,
        }
    }

    /// Overrides the deferral window. Used by tests so the deferral-vs-race
    /// behavior can be exercised without waiting out the production 5s
    /// default.
    #[cfg(test)]
    pub(crate) fn with_deferral_secs(mut self, deferral_secs: u64) -> Self {
        self.deferral_secs = deferral_secs;
        self
    }

    /// Returns the deferral duration non-preferred pods should wait before
    /// falling back to normal lease acquisition.
    pub fn deferral_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.deferral_secs)
    }

    /// Registers this pod in the cache. Call once at startup.
    pub async fn register(&self) -> Result<(), crate::cache::Error> {
        self.renew_internal().await?;
        info!(identity = %self.identity, "Registered peer");
        Ok(())
    }

    /// Renews this pod's registration in the cache without logging at INFO
    /// level, so background heartbeats don't spam the logs.
    async fn renew(&self) -> Result<(), crate::cache::Error> {
        self.renew_internal().await?;
        debug!(identity = %self.identity, "Renewed peer registration");
        Ok(())
    }

    async fn renew_internal(&self) -> Result<(), crate::cache::Error> {
        let key = format!("{PEER_KEY_PREFIX}{}", self.identity);
        self.cache
            .put(
                &key,
                Bytes::from(self.identity.clone()),
                Some(self.ttl_secs),
            )
            .await
    }

    /// Spawns a background task that renews the peer registration at the
    /// configured interval. Stops when the cancellation token is cancelled.
    pub fn spawn_renewal(&self, cancel: CancellationToken) -> tokio::task::JoinHandle<()> {
        let registry = self.clone();
        tokio::spawn(async move {
            let renewal_duration = std::time::Duration::from_secs(registry.renewal_secs);
            let start = tokio::time::Instant::now() + renewal_duration;
            let mut interval = tokio::time::interval_at(start, renewal_duration);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!(identity = %registry.identity, "Peer renewal cancelled");
                        break;
                    }
                    _ = interval.tick() => {
                        if let Err(e) = registry.renew().await {
                            warn!(error = %e, "Failed to renew peer registration");
                        }
                    }
                }
            }
        })
    }

    /// Deregisters this pod from the cache. Call on graceful shutdown.
    pub async fn deregister(&self) -> Result<(), crate::cache::Error> {
        let key = format!("{PEER_KEY_PREFIX}{}", self.identity);
        self.cache.delete(&key).await?;
        info!(identity = %self.identity, "Deregistered peer");
        Ok(())
    }

    /// Returns the sorted list of currently registered peer identities.
    pub async fn list_peers(&self) -> Result<Vec<String>, crate::cache::Error> {
        let keys = self.cache.list_keys(PEER_KEY_PREFIX).await?;
        let mut peers: Vec<String> = keys
            .into_iter()
            .map(|k| k.strip_prefix(PEER_KEY_PREFIX).unwrap_or(&k).to_string())
            .collect();
        peers.sort();
        Ok(peers)
    }

    /// Returns `true` if this pod is the preferred owner for the given flow.
    ///
    /// Uses consistent hashing: `sha256(flow key)` mapped to a peer index
    /// in the sorted peer list. When only one peer is registered, it always
    /// returns `true`.
    ///
    /// The hash input is the identity's key-safe form so it stays consistent
    /// with the lease key derived from the same identity.
    pub async fn is_preferred_owner(
        &self,
        flow_name: &FlowIdentity,
    ) -> Result<bool, crate::cache::Error> {
        let peers = self.list_peers().await?;
        if peers.is_empty() || peers.len() == 1 {
            return Ok(true);
        }
        let preferred = preferred_peer(&flow_name.as_key(), &peers);
        Ok(preferred == self.identity)
    }
}

/// Deterministic mapping from flow name to preferred peer.
///
/// Hashes the flow name with SHA-256 and maps the first 8 bytes to an index
/// in the sorted peer list. The mapping is stable as long as the peer list
/// does not change.
pub fn preferred_peer<'a>(flow_name: &str, sorted_peers: &'a [String]) -> &'a str {
    debug_assert!(!sorted_peers.is_empty(), "sorted_peers must not be empty");
    let mut hasher = Sha256::new();
    hasher.update(flow_name.as_bytes());
    let hash = hasher.finalize();
    let hash_bytes: [u8; 8] = match hash[..8].try_into() {
        Ok(b) => b,
        Err(_) => return &sorted_peers[0],
    };
    let hash_value = u64::from_be_bytes(hash_bytes);
    let index = (hash_value % sorted_peers.len() as u64) as usize;
    &sorted_peers[index]
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::memory::MemoryCache;

    fn make_registry(identity: &str) -> PeerRegistry {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        PeerRegistry::new(cache, identity.to_string())
    }

    fn make_shared_registry(cache: Arc<dyn crate::cache::Cache>, identity: &str) -> PeerRegistry {
        PeerRegistry::new(cache, identity.to_string())
    }

    #[tokio::test]
    async fn test_register_and_list() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");
        let r3 = make_shared_registry(cache.clone(), "pod-c");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r3.register().await.unwrap();

        let peers = r1.list_peers().await.unwrap();
        assert_eq!(peers, vec!["pod-a", "pod-b", "pod-c"]);
    }

    #[tokio::test]
    async fn test_deregister() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r1.deregister().await.unwrap();

        let peers = r2.list_peers().await.unwrap();
        assert_eq!(peers, vec!["pod-b"]);
    }

    #[tokio::test]
    async fn test_single_peer_always_preferred() {
        let r = make_registry("only-pod");
        r.register().await.unwrap();

        assert!(r
            .is_preferred_owner(&FlowIdentity::new("any-flow"))
            .await
            .unwrap());
        assert!(r
            .is_preferred_owner(&FlowIdentity::new("another-flow"))
            .await
            .unwrap());
    }

    #[test]
    fn test_preferred_peer_deterministic() {
        let peers = vec!["pod-a".into(), "pod-b".into(), "pod-c".into()];
        let p1 = preferred_peer("my-flow", &peers);
        let p2 = preferred_peer("my-flow", &peers);
        assert_eq!(p1, p2, "same input must produce same output");
    }

    #[test]
    fn test_preferred_peer_distributes() {
        let peers: Vec<String> = (0..3).map(|i| format!("pod-{i}")).collect();
        let mut counts = std::collections::HashMap::new();
        for i in 0..100 {
            let flow = format!("flow-{i}");
            let p = preferred_peer(&flow, &peers);
            *counts.entry(p.to_string()).or_insert(0u32) += 1;
        }
        for (pod, count) in &counts {
            assert!(
                *count > 5,
                "pod {pod} only got {count} flows out of 100 — distribution too uneven"
            );
        }
    }

    #[tokio::test]
    async fn test_preferred_owner_among_multiple_peers() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let r1 = make_shared_registry(cache.clone(), "pod-a");
        let r2 = make_shared_registry(cache.clone(), "pod-b");
        let r3 = make_shared_registry(cache.clone(), "pod-c");

        r1.register().await.unwrap();
        r2.register().await.unwrap();
        r3.register().await.unwrap();

        let flow = FlowIdentity::new("test-flow");
        let owners: Vec<bool> = futures_util::future::join_all(vec![
            r1.is_preferred_owner(&flow),
            r2.is_preferred_owner(&flow),
            r3.is_preferred_owner(&flow),
        ])
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect();

        let preferred_count = owners.iter().filter(|&&o| o).count();
        assert_eq!(
            preferred_count, 1,
            "exactly one peer should be preferred for a flow"
        );
    }

    #[tokio::test]
    async fn test_empty_peers_returns_preferred() {
        let r = make_registry("pod-a");
        assert!(r
            .is_preferred_owner(&FlowIdentity::new("flow"))
            .await
            .unwrap());
    }
}
