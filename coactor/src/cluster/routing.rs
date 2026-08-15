use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::placement::Candidate;
use super::wall_time_millis;
use crate::{
    ActorAddress, ActorOwner, ActorOwnerRecord, LeaseMutation, NodeSessionId, OwnershipBackend,
    SendError,
};

pub(crate) struct ClusterRouter {
    storage: Arc<dyn OwnershipBackend>,
    node_id: String,
    session_id: NodeSessionId,
    operation_timeout: Duration,
    /// 惰性 TTL 缓存：放置决策用的 lease 快照（TTL = lease renewal interval）。
    lease_cache: tokio::sync::Mutex<Option<(Vec<Candidate>, u64)>>,
    cache_ttl: Duration,
    resolutions: tokio::sync::Mutex<HashMap<ActorAddress, Arc<tokio::sync::Mutex<()>>>>,
}

impl ClusterRouter {
    pub(crate) fn node_id(&self) -> &str {
        &self.node_id
    }

    pub(crate) fn new(
        storage: Arc<dyn OwnershipBackend>,
        node_id: String,
        session_id: NodeSessionId,
        operation_timeout: Duration,
        cache_ttl: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            node_id,
            session_id,
            operation_timeout,
            lease_cache: tokio::sync::Mutex::new(None),
            cache_ttl,
            resolutions: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    async fn resolution_lock(&self, address: &ActorAddress) -> Arc<tokio::sync::Mutex<()>> {
        let mut resolutions = self.resolutions.lock().await;
        resolutions
            .entry(address.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    fn is_local_owner(&self, record: &ActorOwnerRecord) -> bool {
        record.owner.as_ref().is_some_and(|owner| {
            owner.node_id == self.node_id && owner.session_id == self.session_id
        })
    }

    fn local_claim(&self, epoch: u64) -> ActorOwnerRecord {
        ActorOwnerRecord {
            owner: Some(ActorOwner {
                node_id: self.node_id.clone(),
                session_id: self.session_id.clone(),
            }),
            ownership_epoch: epoch,
        }
    }

    /// 只读的所有权判断（不 claim、不缓存）：本节点 / 他节点 / 未拥有或 stale。
    pub(crate) async fn owner_only(&self, address: &ActorAddress) -> Result<OwnerStatus, SendError> {
        let current = tokio::time::timeout(
            self.operation_timeout,
            self.storage.read_actor_owner(address),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        let Some(current) = current.as_ref() else {
            return Ok(OwnerStatus::Unowned);
        };
        if self.is_local_owner(&current.record) {
            return Ok(OwnerStatus::Local);
        }
        let Some(owner) = &current.record.owner else {
            return Ok(OwnerStatus::Unowned);
        };
        let lease = tokio::time::timeout(
            self.operation_timeout,
            self.storage.read_node_lease(&owner.session_id),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        match lease {
            Some(lease) if lease.lease.expires_at_unix_ms > wall_time_millis() => {
                Ok(OwnerStatus::Remote {
                    endpoint: lease.lease.advertised_address,
                })
            }
            _ => {
                // owner lease 缺失或过期：视为可放置（availability failover 语义）。
                Ok(OwnerStatus::Unowned)
            }
        }
    }

    pub(crate) async fn resolve(
        &self,
        address: &ActorAddress,
        capacity: &Arc<Semaphore>,
    ) -> Result<ResolvedOwner, SendError> {
        let lock = self.resolution_lock(address).await;
        let guard = lock.lock_owned().await;
        for _ in 0..3 {
            let current = tokio::time::timeout(
                self.operation_timeout,
                self.storage.read_actor_owner(address),
            )
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
            if let Some(current) = current.as_ref() {
                if self.is_local_owner(&current.record) {
                    return Ok(ResolvedOwner::Local {
                        reservation: None,
                        guard,
                    });
                }
                if let Some(owner) = &current.record.owner {
                    let lease = tokio::time::timeout(
                        self.operation_timeout,
                        self.storage.read_node_lease(&owner.session_id),
                    )
                    .await
                    .map_err(|_| SendError::OwnershipUnavailable)?
                    .map_err(|_| SendError::OwnershipUnavailable)?;
                    if let Some(lease) = lease {
                        if lease.lease.expires_at_unix_ms > wall_time_millis() {
                            let endpoint = lease.lease.advertised_address.clone();
                            let protocol_version = lease.lease.protocol_version;
                            return Ok(ResolvedOwner::Remote {
                                endpoint,
                                protocol_version,
                            });
                        }
                    }
                    tracing::info!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        prior_epoch = current.record.ownership_epoch,
                        lifecycle = "availability_failover",
                        "Actor Owner Node Lease is absent or expired; attempting empty-state takeover"
                    );
                }
            }

            let epoch = current.as_ref().map_or(1, |current| {
                current.record.ownership_epoch.saturating_add(1)
            });
            let expected = self.local_claim(epoch);
            let etag = current.as_ref().map(|current| current.etag.as_str());
            let reservation = capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| SendError::RuntimeAtCapacity)?;
            let mutation = tokio::time::timeout(
                self.operation_timeout,
                self.storage
                    .claim_actor_owner(address, expected.clone(), etag),
            )
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
            match mutation {
                LeaseMutation::Applied { .. } => {
                    return Ok(ResolvedOwner::Local {
                        reservation: Some(reservation),
                        guard,
                    });
                }
                LeaseMutation::ConditionalRejected => {
                    drop(reservation);
                    continue;
                }
                LeaseMutation::Ambiguous(_) => {
                    let mut should_reresolve = false;
                    for _ in 0..3 {
                        let confirmed = tokio::time::timeout(
                            self.operation_timeout,
                            self.storage.read_actor_owner(address),
                        )
                        .await;
                        if let Ok(Ok(Some(confirmed))) = confirmed {
                            if confirmed.record == expected {
                                return Ok(ResolvedOwner::Local {
                                    reservation: Some(reservation),
                                    guard,
                                });
                            }
                            if confirmed.record.owner.is_some() {
                                should_reresolve = true;
                                break;
                            }
                        }
                    }
                    if should_reresolve {
                        continue;
                    }
                    return Err(SendError::OwnershipUnavailable);
                }
            }
        }
        Err(SendError::OwnershipUnavailable)
    }

    pub(crate) async fn release_local_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<(), SendError> {
        let lock = self.resolution_lock(address).await;
        let _guard = lock.lock_owned().await;
        let current = tokio::time::timeout(
            self.operation_timeout,
            self.storage.read_actor_owner(address),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?
        .ok_or(SendError::OwnershipUnavailable)?;
        if !self.is_local_owner(&current.record) {
            return Err(SendError::OwnershipUnavailable);
        }
        let released = tokio::time::timeout(
            self.operation_timeout,
            self.storage.release_actor_owner(address, &current),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        let confirmed = match released {
            LeaseMutation::Applied { .. } => true,
            LeaseMutation::ConditionalRejected | LeaseMutation::Ambiguous(_) => {
                let expected = ActorOwnerRecord::unowned(current.record.ownership_epoch);
                let mut confirmed = false;
                for _ in 0..3 {
                    let read = tokio::time::timeout(
                        self.operation_timeout,
                        self.storage.read_actor_owner(address),
                    )
                    .await;
                    if let Ok(Ok(Some(read))) = read {
                        if read.record == expected {
                            confirmed = true;
                            break;
                        }
                        if read.record != current.record {
                            break;
                        }
                    }
                }
                confirmed
            }
        };
        if !confirmed {
            return Err(SendError::OwnershipUnavailable);
        }
        Ok(())
    }

    /// 放置候选：硬过滤后的其他节点（有效 lease / 协议匹配 / 非满 / 非压力 / 非排空），
    /// 带惰性 TTL 缓存（TTL = lease renewal interval）；排序与采样交给策略。
    pub(crate) async fn placement_candidates(
        &self,
        protocol_version: u32,
    ) -> Result<Vec<Candidate>, SendError> {
        let now = wall_time_millis();
        {
            let cache = self.lease_cache.lock().await;
            if let Some((candidates, fetched_at)) = cache.as_ref() {
                if now.saturating_sub(*fetched_at) < self.cache_ttl.as_millis() as u64 {
                    return Ok(candidates.clone());
                }
            }
        }
        let leases = tokio::time::timeout(self.operation_timeout, self.storage.list_node_leases())
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
        let candidates: Vec<Candidate> = leases
            .into_iter()
            .map(|versioned| versioned.lease)
            .filter(|lease| {
                lease.session_id != self.session_id
                    && lease.expires_at_unix_ms > now
                    && lease.protocol_version == protocol_version
                    && !lease.pressured
                    && !lease.draining
                    && lease.active_actor_count < lease.max_actor_count
            })
            .map(|lease| Candidate {
                endpoint: crate::transport::Endpoint::new(lease.advertised_address),
                active_actor_count: lease.active_actor_count,
                max_actor_count: lease.max_actor_count,
            })
            .collect();
        *self.lease_cache.lock().await = Some((candidates.clone(), now));
        Ok(candidates)
    }
}

pub(crate) enum ResolvedOwner {
    Local {
        reservation: Option<OwnedSemaphorePermit>,
        guard: tokio::sync::OwnedMutexGuard<()>,
    },
    #[allow(dead_code)]
    Remote {
        endpoint: String,
        protocol_version: u32,
    },
}

/// 只读所有权状态（放置决策用）。
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum OwnerStatus {
    Local,
    Remote {
        endpoint: String,
    },
    /// 无 owner 记录，或 owner lease 缺失/过期（可放置）。
    Unowned,
}
