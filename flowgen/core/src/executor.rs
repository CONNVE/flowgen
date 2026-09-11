//! Executor manages distributed coordination via cache-based leases.
//!
//! Provides a universal lease management abstraction that works with any Cache
//! implementation (NATS KV, Redis, etc.) using atomic operations for coordination.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

/// Prefix for all lease keys in the cache for better organization.
pub const LEASE_KEY_PREFIX: &str = "lease.";

/// Default lease duration in seconds (60 seconds).
const DEFAULT_LEASE_DURATION_SECS: u64 = 60;

/// Default renewal interval in seconds (10 seconds).
const DEFAULT_RENEWAL_INTERVAL_SECS: u64 = 10;

/// Maximum lease duration in seconds (1 hour).
const MAX_LEASE_DURATION_SECS: u64 = 3600;

/// Lease configuration validation errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum LeaseConfigError {
    /// Lease duration must be greater than zero.
    #[error("Lease duration must be greater than 0")]
    ZeroLeaseDuration,
    /// Lease duration exceeds maximum allowed value.
    #[error("Lease duration {0:?} exceeds maximum of 1 hour")]
    ExcessiveLeaseDuration(Duration),
    /// Renewal interval must be less than lease duration.
    #[error("Renewal interval {renewal:?} must be less than lease duration {lease:?}")]
    InvalidRenewalInterval { renewal: Duration, lease: Duration },
}

/// Lease metadata stored in cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaseMetadata {
    /// Identity of the lease holder (hostname, instance ID, etc.).
    pub holder_identity: String,
    /// Unix timestamp when lease was acquired/renewed (seconds).
    pub renewed_at: u64,
    /// Lease duration in seconds.
    pub lease_duration_secs: u64,
    /// Generation counter to track lease lineage.
    /// Incremented on each acquisition, preserved on renewal.
    #[serde(default)]
    pub generation: u64,
}

/// Configuration for lease management.
#[derive(Debug, Clone)]
pub struct LeaseConfig {
    /// How long a lease remains valid without renewal (default: 60 seconds).
    pub lease_duration: Duration,
    /// How often to renew the lease (default: 10 seconds).
    pub renewal_interval: Duration,
    /// Identity of this executor (hostname, instance ID, etc.).
    pub holder_identity: String,
    /// Retry configuration for lease acquisition with exponential backoff and jitter.
    pub retry_config: crate::retry::RetryConfig,
}

/// Resolves this pod's holder identity from the environment.
///
/// Priority: `POD_NAME` > `HOSTNAME` > a generated ID (PID + timestamp).
/// The generated fallback is only unique per call — callers that need one
/// consistent identity across multiple `LeaseConfig`/`Executor` instances
/// (e.g. one per flow) must resolve it once and share the value, rather
/// than relying on `LeaseConfig::default()` to re-resolve it each time.
pub fn resolve_holder_identity() -> String {
    std::env::var("POD_NAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| {
            // Generate a unique identifier using process ID and timestamp.
            // This ensures uniqueness even without external dependencies.
            let pid = std::process::id();
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_nanos();
            format!("executor-{pid}-{timestamp}")
        })
}

impl Default for LeaseConfig {
    fn default() -> Self {
        Self {
            lease_duration: Duration::from_secs(DEFAULT_LEASE_DURATION_SECS),
            renewal_interval: Duration::from_secs(DEFAULT_RENEWAL_INTERVAL_SECS),
            holder_identity: resolve_holder_identity(),
            retry_config: crate::retry::RetryConfig::default(),
        }
    }
}

impl LeaseConfig {
    /// Validates the lease configuration for reasonable values.
    ///
    /// # Errors
    /// Returns `LeaseConfigError` if any configuration values are invalid.
    pub fn validate(&self) -> Result<(), LeaseConfigError> {
        if self.lease_duration.as_secs() == 0 {
            return Err(LeaseConfigError::ZeroLeaseDuration);
        }
        if self.lease_duration > Duration::from_secs(MAX_LEASE_DURATION_SECS) {
            return Err(LeaseConfigError::ExcessiveLeaseDuration(
                self.lease_duration,
            ));
        }
        if self.renewal_interval >= self.lease_duration {
            return Err(LeaseConfigError::InvalidRenewalInterval {
                renewal: self.renewal_interval,
                lease: self.lease_duration,
            });
        }
        Ok(())
    }
}

/// Result of lease acquisition attempt.
#[derive(Debug, Clone, PartialEq)]
pub enum LeaseResult {
    /// This executor acquired the lease. `generation` is carried back so
    /// renewals can preserve it without re-reading the lease.
    Acquired { revision: u64, generation: u64 },
    /// Another executor holds the lease.
    HeldByOther { holder: String },
    /// Lease expired and was taken over.
    TakenOver { revision: u64, generation: u64 },
}

/// Result of lease renewal attempt.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum RenewalResult {
    /// Lease renewed successfully with new revision number.
    Renewed { revision: u64 },
    /// Lost lease ownership to another executor.
    LostOwnership { holder: String },
    /// Lease was deleted during renewal attempt.
    LeaseDeleted,
}

/// Executor manages distributed coordination via cache-based leases.
///
/// Provides a universal lease management abstraction that works with any Cache
/// implementation (NATS KV, Redis, etc.) using atomic operations for
/// compare-and-swap semantics.
#[derive(Debug, Clone)]
pub struct Executor {
    cache: Arc<dyn crate::cache::Cache>,
    pub(crate) config: LeaseConfig,
}

impl Executor {
    /// Creates a new executor with the given cache backend.
    ///
    /// # Errors
    /// Returns `LeaseConfigError` if the configuration is invalid.
    pub fn new(
        cache: Arc<dyn crate::cache::Cache>,
        config: LeaseConfig,
    ) -> Result<Self, LeaseConfigError> {
        // Validate config before creating executor.
        config.validate()?;
        Ok(Self { cache, config })
    }

    /// Attempts to acquire a lease for distributed coordination.
    ///
    /// This method implements a three-phase lease acquisition protocol:
    /// 1. Optimistically try to create a new lease (succeeds if lease doesn't exist).
    /// 2. If the lease exists, check if it has expired beyond its duration.
    /// 3. For expired leases, attempt atomic takeover using revision number for consistency.
    ///
    /// The generation counter in LeaseMetadata ensures proper lease lineage tracking
    /// and prevents stale processes from incorrectly reclaiming leases.
    pub async fn acquire_lease(
        &self,
        lease_name: &str,
    ) -> Result<LeaseResult, crate::cache::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                tracing::warn!(error = %e, "System clock went backwards");
                crate::cache::CacheError::CreateFailed(Box::new(e))
            })?
            .as_secs();

        let metadata = LeaseMetadata {
            holder_identity: self.config.holder_identity.clone(),
            renewed_at: now,
            lease_duration_secs: self.config.lease_duration.as_secs(),
            generation: 1,
        };

        let value = serde_json::to_vec(&metadata)
            .map_err(|e| crate::cache::CacheError::CreateFailed(Box::new(e)))?;

        // Try to create lease (atomic - only succeeds if key doesn't exist).
        match self
            .cache
            .create(
                lease_name,
                Bytes::from(value.clone()),
                Some(self.config.lease_duration.as_secs()),
            )
            .await
        {
            Ok(revision) => {
                tracing::debug!(
                    lease_name = %lease_name,
                    holder = %self.config.holder_identity,
                    revision = %revision,
                    "Acquired new lease"
                );
                return Ok(LeaseResult::Acquired {
                    revision,
                    generation: metadata.generation,
                });
            }
            Err(crate::cache::CacheError::AlreadyExists) => {
                // Lease exists - check if we can take it over.
            }
            Err(e) => return Err(e),
        }

        // Lease exists - get current state with revision.
        let (current_value, current_revision) =
            match self.cache.get_with_revision(lease_name).await? {
                Some(entry) => entry,
                None => {
                    // No value but key exists - this is a DELETE tombstone.
                    // Get the tombstone's revision and overwrite it.
                    if let Some(tombstone_revision) = self.cache.get_revision(lease_name).await? {
                        tracing::debug!(
                            lease_name = %lease_name,
                            tombstone_revision = %tombstone_revision,
                            "Overwriting DELETE tombstone with new lease"
                        );
                        let new_revision = self
                            .cache
                            .update(
                                lease_name,
                                Bytes::from(value),
                                tombstone_revision,
                                Some(self.config.lease_duration.as_secs()),
                            )
                            .await?;
                        return Ok(LeaseResult::Acquired {
                            revision: new_revision,
                            generation: metadata.generation,
                        });
                    } else {
                        // Race condition: create() saw a key (tombstone or value), but now
                        // it's completely gone. Another pod likely deleted or TTL expired it.
                        // Retry from the beginning to attempt fresh acquisition.
                        tracing::debug!(
                            lease_name = %lease_name,
                            "Key disappeared between create and get, retrying acquisition"
                        );
                        return Err(crate::cache::CacheError::NotFound);
                    }
                }
            };

        let current_metadata: LeaseMetadata = serde_json::from_slice(&current_value)
            .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

        // Check if current holder is self. This can happen during process restarts where
        // the same instance reclaims its lease before it expired.
        if current_metadata.holder_identity == self.config.holder_identity {
            tracing::debug!(
                lease_name = %lease_name,
                holder = %self.config.holder_identity,
                "Already own this lease, renewing"
            );
            match self
                .renew_lease(lease_name, current_revision, current_metadata.generation)
                .await?
            {
                RenewalResult::Renewed { revision } => {
                    return Ok(LeaseResult::Acquired {
                        revision,
                        generation: current_metadata.generation,
                    })
                }
                RenewalResult::LostOwnership { holder } => {
                    return Ok(LeaseResult::HeldByOther { holder })
                }
                RenewalResult::LeaseDeleted => {
                    // Lease was deleted, we can try to acquire it fresh.
                    tracing::debug!(
                        lease_name = %lease_name,
                        "Lease was deleted during renewal, attempting fresh acquisition"
                    );
                    // Fall through to continue with normal acquisition flow.
                    // The lease no longer exists, so we'll create it fresh.
                    return Err(crate::cache::CacheError::NotFound);
                }
            }
        }

        // Check if lease is expired.
        let elapsed = now.saturating_sub(current_metadata.renewed_at);
        if elapsed > current_metadata.lease_duration_secs {
            // Lease expired - attempt takeover (atomic update with revision check).
            tracing::info!(
                lease_name = %lease_name,
                current_holder = %current_metadata.holder_identity,
                elapsed = %elapsed,
                lease_duration = %current_metadata.lease_duration_secs,
                "Lease expired, attempting takeover"
            );

            // Create new metadata with incremented generation for takeover.
            let takeover_metadata = LeaseMetadata {
                holder_identity: self.config.holder_identity.clone(),
                renewed_at: now,
                lease_duration_secs: self.config.lease_duration.as_secs(),
                generation: current_metadata.generation.saturating_add(1),
            };

            let takeover_value = serde_json::to_vec(&takeover_metadata)
                .map_err(|e| crate::cache::CacheError::UpdateFailed(Box::new(e)))?;

            match self
                .cache
                .update(
                    lease_name,
                    Bytes::from(takeover_value),
                    current_revision,
                    Some(self.config.lease_duration.as_secs()),
                )
                .await
            {
                Ok(new_revision) => {
                    tracing::info!(
                        lease_name = %lease_name,
                        holder = %self.config.holder_identity,
                        revision = %new_revision,
                        "Successfully took over expired lease"
                    );
                    Ok(LeaseResult::TakenOver {
                        revision: new_revision,
                        generation: takeover_metadata.generation,
                    })
                }
                Err(crate::cache::CacheError::RevisionMismatch { .. }) => {
                    // Another executor took over in the meantime - get new state.
                    let (value, _) = self
                        .cache
                        .get_with_revision(lease_name)
                        .await?
                        .ok_or(crate::cache::CacheError::NotFound)?;
                    let metadata: LeaseMetadata = serde_json::from_slice(&value)
                        .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

                    tracing::debug!(
                        lease_name = %lease_name,
                        holder = %metadata.holder_identity,
                        "Lost takeover race to another executor"
                    );
                    Ok(LeaseResult::HeldByOther {
                        holder: metadata.holder_identity,
                    })
                }
                Err(e) => Err(e),
            }
        } else {
            // Lease is still valid and owned by another executor.
            tracing::debug!(
                lease_name = %lease_name,
                holder = %current_metadata.holder_identity,
                time_remaining = %(current_metadata.lease_duration_secs - elapsed),
                "Lease held by another executor"
            );
            Ok(LeaseResult::HeldByOther {
                holder: current_metadata.holder_identity,
            })
        }
    }

    /// Renews an existing lease that this executor owns.
    ///
    /// This method performs an atomic compare-and-swap operation to extend the lease duration.
    /// The revision number ensures consistency - if another process has taken over the lease,
    /// the update will fail with a revision mismatch.
    ///
    /// `generation` is supplied by the caller rather than re-read here: the
    /// holder learned it when it acquired the lease, and a renewal every few
    /// seconds per flow makes the extra round-trip the dominant cost on an
    /// otherwise idle worker. The revision check below still rejects a
    /// renewal that races a takeover.
    pub async fn renew_lease(
        &self,
        lease_name: &str,
        current_revision: u64,
        generation: u64,
    ) -> Result<RenewalResult, crate::cache::Error> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| {
                tracing::warn!(error = %e, "System clock went backwards");
                crate::cache::CacheError::UpdateFailed(Box::new(e))
            })?
            .as_secs();

        let metadata = LeaseMetadata {
            holder_identity: self.config.holder_identity.clone(),
            renewed_at: now,
            lease_duration_secs: self.config.lease_duration.as_secs(),
            generation,
        };

        let value = serde_json::to_vec(&metadata)
            .map_err(|e| crate::cache::CacheError::UpdateFailed(Box::new(e)))?;

        // Atomic update - only succeeds if revision matches (prevents split-brain).
        match self
            .cache
            .update(
                lease_name,
                Bytes::from(value),
                current_revision,
                Some(self.config.lease_duration.as_secs()),
            )
            .await
        {
            Ok(new_revision) => {
                tracing::trace!(
                    lease_name = %lease_name,
                    revision = %new_revision,
                    "Successfully renewed lease"
                );
                Ok(RenewalResult::Renewed {
                    revision: new_revision,
                })
            }
            Err(crate::cache::CacheError::RevisionMismatch { .. })
            | Err(crate::cache::CacheError::NotFound) => {
                // Lease was taken over or deleted - get current holder.
                match self.cache.get_with_revision(lease_name).await? {
                    Some((value, _)) => {
                        let metadata: LeaseMetadata = serde_json::from_slice(&value)
                            .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;

                        tracing::warn!(
                            lease_name = %lease_name,
                            current_holder = %metadata.holder_identity,
                            "Lost lease ownership during renewal"
                        );
                        Ok(RenewalResult::LostOwnership {
                            holder: metadata.holder_identity,
                        })
                    }
                    None => {
                        tracing::warn!(
                            lease_name = %lease_name,
                            "Lease was deleted during renewal"
                        );
                        Ok(RenewalResult::LeaseDeleted)
                    }
                }
            }
            Err(e) => Err(e),
        }
    }

    /// Checks if this executor still owns the lease and returns the revision if so.
    pub async fn still_owns_lease(
        &self,
        lease_name: &str,
    ) -> Result<Option<u64>, crate::cache::Error> {
        match self.cache.get_with_revision(lease_name).await? {
            Some((value, revision)) => {
                let metadata: LeaseMetadata = serde_json::from_slice(&value)
                    .map_err(|e| crate::cache::CacheError::GetFailed(Box::new(e)))?;
                if metadata.holder_identity == self.config.holder_identity {
                    Ok(Some(revision))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// Releases a lease that this executor owns (unconditional delete).
    pub async fn release_lease(&self, lease_name: &str) -> Result<(), crate::cache::Error> {
        self.cache.delete(lease_name).await?;
        tracing::debug!(
            lease_name = %lease_name,
            holder = %self.config.holder_identity,
            "Released lease"
        );
        Ok(())
    }

    /// Atomically releases a lease only if this executor still owns it at the given revision.
    pub async fn release_lease_with_revision(
        &self,
        lease_name: &str,
        expected_revision: u64,
    ) -> Result<(), crate::cache::Error> {
        self.cache
            .delete_with_revision(lease_name, expected_revision)
            .await?;
        tracing::debug!(
            lease_name = %lease_name,
            holder = %self.config.holder_identity,
            revision = %expected_revision,
            "Released lease atomically"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::memory::MemoryCache;

    fn create_executor(identity: &str) -> Executor {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let config = LeaseConfig {
            holder_identity: identity.to_string(),
            ..Default::default()
        };
        Executor::new(cache, config).unwrap()
    }

    #[tokio::test]
    async fn test_acquire_new_lease() {
        let executor = create_executor("executor-1");
        let result = executor.acquire_lease("test-lease").await.unwrap();

        assert!(matches!(result, LeaseResult::Acquired { .. }));
    }

    #[tokio::test]
    async fn test_acquire_lease_already_owned_by_self() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let config = LeaseConfig {
            holder_identity: "executor-1".to_string(),
            ..Default::default()
        };
        let executor = Executor::new(cache.clone(), config.clone()).unwrap();

        // Acquire lease first time.
        let result1 = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Try to acquire same lease with same executor.
        let result2 = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::Acquired { .. }));
    }

    #[tokio::test]
    async fn test_fail_to_acquire_lease_held_by_other() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        // Executor 1 acquires lease.
        let result1 = executor1.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Executor 2 tries to acquire same lease.
        let result2 = executor2.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::HeldByOther { .. }));
    }

    #[tokio::test]
    async fn test_takeover_expired_lease() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                lease_duration: Duration::from_secs(1),
                renewal_interval: Duration::from_millis(100), // Must be less than lease_duration
                ..Default::default()
            },
        )
        .unwrap();
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        // Executor 1 acquires lease.
        let result1 = executor1.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result1, LeaseResult::Acquired { .. }));

        // Wait for lease to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Executor 2 takes over expired lease.
        let result2 = executor2.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result2, LeaseResult::TakenOver { .. }));
    }

    #[tokio::test]
    async fn test_renew_lease_successfully() {
        let executor = create_executor("executor-1");

        let LeaseResult::Acquired {
            revision,
            generation,
        } = executor.acquire_lease("test-lease").await.unwrap()
        else {
            panic!("Failed to acquire lease");
        };

        let result = executor
            .renew_lease("test-lease", revision, generation)
            .await
            .unwrap();
        assert!(matches!(result, RenewalResult::Renewed { .. }));
    }

    #[tokio::test]
    async fn test_detect_lost_ownership_during_renewal() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                lease_duration: Duration::from_secs(1),
                renewal_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap();
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        // Executor 1 acquires lease.
        let LeaseResult::Acquired {
            revision,
            generation,
        } = executor1.acquire_lease("test-lease").await.unwrap()
        else {
            panic!("Failed to acquire lease");
        };

        // Wait for lease to expire.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Executor 2 takes over.
        executor2.acquire_lease("test-lease").await.unwrap();

        // Executor 1 tries to renew with old revision.
        let result = executor1
            .renew_lease("test-lease", revision, generation)
            .await
            .unwrap();
        assert!(matches!(result, RenewalResult::LostOwnership { .. }));
    }

    #[tokio::test]
    async fn test_release_lease() {
        let executor = create_executor("executor-1");

        executor.acquire_lease("test-lease").await.unwrap();
        executor.release_lease("test-lease").await.unwrap();

        // Verify lease is gone by trying to acquire it again.
        let result = executor.acquire_lease("test-lease").await.unwrap();
        assert!(matches!(result, LeaseResult::Acquired { .. }));
    }

    #[tokio::test]
    async fn test_lease_deleted_during_renewal() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let config = LeaseConfig {
            holder_identity: "executor-1".to_string(),
            ..Default::default()
        };
        let executor = Executor::new(cache.clone(), config).unwrap();

        // Acquire lease.
        let LeaseResult::Acquired {
            revision,
            generation,
        } = executor.acquire_lease("test-lease").await.unwrap()
        else {
            panic!("Failed to acquire lease");
        };

        // Delete the lease externally.
        cache.delete("test-lease").await.unwrap();

        let result = executor
            .renew_lease("test-lease", revision, generation)
            .await;
        assert!(matches!(result, Ok(RenewalResult::LeaseDeleted)));
    }

    #[tokio::test]
    async fn test_generation_counter_increments_on_takeover() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                lease_duration: Duration::from_secs(1),
                renewal_interval: Duration::from_millis(100),
                ..Default::default()
            },
        )
        .unwrap();

        // Acquire lease and check generation is 1.
        executor1.acquire_lease("test-lease").await.unwrap();

        let (value, _) = cache
            .get_with_revision("test-lease")
            .await
            .unwrap()
            .unwrap();
        let metadata: LeaseMetadata = serde_json::from_slice(&value).unwrap();
        assert_eq!(metadata.generation, 1);

        // Wait for expiry.
        tokio::time::sleep(Duration::from_secs(2)).await;

        // Executor 2 takes over.
        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        executor2.acquire_lease("test-lease").await.unwrap();

        // Check generation incremented to 2.
        let (value, _) = cache
            .get_with_revision("test-lease")
            .await
            .unwrap()
            .unwrap();
        let metadata: LeaseMetadata = serde_json::from_slice(&value).unwrap();
        assert_eq!(metadata.generation, 2);
        assert_eq!(metadata.holder_identity, "executor-2");
    }

    #[tokio::test]
    async fn test_concurrent_lease_acquisition() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;
        let lease_name = "concurrent-lease";

        // Spawn 10 executors trying to acquire the same lease.
        let mut handles = vec![];
        for i in 0..10 {
            let cache_clone = cache.clone();
            let lease_name = lease_name.to_string();
            let handle = tokio::spawn(async move {
                let executor = Executor::new(
                    cache_clone,
                    LeaseConfig {
                        holder_identity: format!("executor-{i}"),
                        ..Default::default()
                    },
                )
                .unwrap();
                executor.acquire_lease(&lease_name).await
            });
            handles.push(handle);
        }

        // Wait for all to complete.
        use futures_util::future::join_all;
        let results: Vec<_> = join_all(handles).await;

        // Exactly one should have acquired the lease.
        let acquired_count = results
            .iter()
            .filter(|join_result| matches!(join_result, Ok(Ok(LeaseResult::Acquired { .. }))))
            .count();

        assert_eq!(
            acquired_count, 1,
            "Exactly one executor should acquire the lease"
        );

        // Others should see it as held by other.
        let held_by_other_count = results
            .iter()
            .filter(|join_result| matches!(join_result, Ok(Ok(LeaseResult::HeldByOther { .. }))))
            .count();

        assert_eq!(
            held_by_other_count, 9,
            "Nine executors should see lease held by other"
        );
    }

    #[tokio::test]
    async fn test_lease_config_validation() {
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        // Test zero lease duration.
        let result = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "test".to_string(),
                lease_duration: Duration::from_secs(0),
                ..Default::default()
            },
        );
        assert!(matches!(result, Err(LeaseConfigError::ZeroLeaseDuration)));

        // Test excessive lease duration.
        let result = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "test".to_string(),
                lease_duration: Duration::from_secs(7200),
                ..Default::default()
            },
        );
        assert!(matches!(
            result,
            Err(LeaseConfigError::ExcessiveLeaseDuration(_))
        ));

        // Test invalid renewal interval.
        let result = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "test".to_string(),
                lease_duration: Duration::from_secs(10),
                renewal_interval: Duration::from_secs(15),
                ..Default::default()
            },
        );
        assert!(matches!(
            result,
            Err(LeaseConfigError::InvalidRenewalInterval { .. })
        ));
    }

    #[tokio::test]
    async fn test_acquire_lease_race_with_deletion() {
        // This test validates the fix for the NATS empty bytes issue.
        // When NATS returns empty bytes for expired TTL tombstones, cache.rs now
        // filters them out and returns None from get_with_revision(). This test
        // ensures the executor handles this case correctly by returning NotFound,
        // which allows the task_manager retry logic to acquire the lease.
        let cache = Arc::new(MemoryCache::new()) as Arc<dyn crate::cache::Cache>;

        let executor1 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-1".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        let executor2 = Executor::new(
            cache.clone(),
            LeaseConfig {
                holder_identity: "executor-2".to_string(),
                ..Default::default()
            },
        )
        .unwrap();

        // Executor 1 creates a lease.
        executor1.acquire_lease("test-lease").await.unwrap();

        // Delete the lease to simulate NATS returning empty bytes for expired TTL.
        // In the real scenario, NATS KV returns empty bytes which cache.rs filters
        // out, making get_with_revision() return None.
        cache.delete("test-lease").await.unwrap();

        // Executor 2 tries to acquire the deleted lease.
        // The acquire_lease flow:
        // 1. create() succeeds because the key doesn't exist anymore
        let result = executor2.acquire_lease("test-lease").await;

        // Since the key was deleted, create() should succeed and return Acquired.
        // This validates that after filtering out empty bytes (simulated by deletion),
        // the system can recover and acquire new leases.
        assert!(matches!(result, Ok(LeaseResult::Acquired { .. })));
    }
}
