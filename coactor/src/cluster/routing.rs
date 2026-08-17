use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::placement::Candidate;
use super::wall_time_millis;
use crate::{
    ActorAddress, ActorOwner, ActorOwnerRecord, ActorOwnerStore, MutationOutcome, NodeDirectory,
    NodeSessionId, SendError,
};

pub(crate) struct ClusterRouter {
    directory: Arc<dyn NodeDirectory>,
    owners: Arc<dyn ActorOwnerStore>,
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
        directory: Arc<dyn NodeDirectory>,
        owners: Arc<dyn ActorOwnerStore>,
        node_id: String,
        session_id: NodeSessionId,
        operation_timeout: Duration,
        cache_ttl: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            directory,
            owners,
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
    pub(crate) async fn owner_only(
        &self,
        address: &ActorAddress,
    ) -> Result<OwnerStatus, SendError> {
        let current = tokio::time::timeout(
            self.operation_timeout,
            self.owners.read_actor_owner(address),
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
        if owner.node_id == self.node_id {
            return Ok(OwnerStatus::Unowned);
        }
        let node = tokio::time::timeout(
            self.operation_timeout,
            self.directory.read_node(&owner.node_id),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        match node {
            Some(node) => Ok(OwnerStatus::Remote {
                endpoint: node.advertised_endpoint,
            }),
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
                self.owners.read_actor_owner(address),
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
                    if owner.node_id == self.node_id {
                        let expected =
                            self.local_claim(current.record.ownership_epoch.saturating_add(1));
                        let reservation = capacity
                            .clone()
                            .try_acquire_owned()
                            .map_err(|_| SendError::RuntimeAtCapacity)?;
                        let mutation = tokio::time::timeout(
                            self.operation_timeout,
                            self.owners.compare_exchange_actor_owner(
                                address,
                                expected.clone(),
                                Some(&current.revision),
                            ),
                        )
                        .await
                        .map_err(|_| SendError::OwnershipUnavailable)?
                        .map_err(|_| SendError::OwnershipUnavailable)?;
                        match mutation {
                            MutationOutcome::Applied(_) => {
                                return Ok(ResolvedOwner::Local {
                                    reservation: Some(reservation),
                                    guard,
                                });
                            }
                            MutationOutcome::Conflict => {
                                drop(reservation);
                                continue;
                            }
                            MutationOutcome::Indeterminate(_) => {
                                if let Ok(Ok(Some(confirmed))) = tokio::time::timeout(
                                    self.operation_timeout,
                                    self.owners.read_actor_owner(address),
                                )
                                .await
                                {
                                    if confirmed.record == expected {
                                        return Ok(ResolvedOwner::Local {
                                            reservation: Some(reservation),
                                            guard,
                                        });
                                    }
                                }
                                drop(reservation);
                                continue;
                            }
                        }
                    }
                    let node = tokio::time::timeout(
                        self.operation_timeout,
                        self.directory.read_node(&owner.node_id),
                    )
                    .await
                    .map_err(|_| SendError::OwnershipUnavailable)?
                    .map_err(|_| SendError::OwnershipUnavailable)?;
                    if let Some(node) = node {
                        let endpoint = node.advertised_endpoint.clone();
                        let protocol_version = node.protocol_version;
                        return Ok(ResolvedOwner::Remote {
                            endpoint,
                            protocol_version,
                        });
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
            let revision = current.as_ref().map(|current| &current.revision);
            let reservation = capacity
                .clone()
                .try_acquire_owned()
                .map_err(|_| SendError::RuntimeAtCapacity)?;
            let mutation = tokio::time::timeout(
                self.operation_timeout,
                self.owners
                    .compare_exchange_actor_owner(address, expected.clone(), revision),
            )
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
            match mutation {
                MutationOutcome::Applied(_) => {
                    return Ok(ResolvedOwner::Local {
                        reservation: Some(reservation),
                        guard,
                    });
                }
                MutationOutcome::Conflict => {
                    drop(reservation);
                    continue;
                }
                MutationOutcome::Indeterminate(_) => {
                    let mut should_reresolve = false;
                    for _ in 0..3 {
                        let confirmed = tokio::time::timeout(
                            self.operation_timeout,
                            self.owners.read_actor_owner(address),
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

    pub(crate) async fn clear_owner_for_fallback(
        &self,
        address: &ActorAddress,
        rejected_endpoint: &str,
    ) -> Result<bool, SendError> {
        let current = tokio::time::timeout(
            self.operation_timeout,
            self.owners.read_actor_owner(address),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        let Some(current) = current else {
            return Ok(false);
        };
        let Some(owner) = &current.record.owner else {
            return Ok(false);
        };
        let node = tokio::time::timeout(
            self.operation_timeout,
            self.directory.read_node(&owner.node_id),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        if !node.is_some_and(|node| node.advertised_endpoint == rejected_endpoint) {
            return Ok(false);
        }
        let expected = ActorOwnerRecord::unowned(current.record.ownership_epoch);
        let mutation = tokio::time::timeout(
            self.operation_timeout,
            self.owners.release_actor_owner(address, &current),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        match mutation {
            MutationOutcome::Applied(_) => Ok(true),
            MutationOutcome::Conflict => Ok(false),
            MutationOutcome::Indeterminate(_) => {
                let confirmed = tokio::time::timeout(
                    self.operation_timeout,
                    self.owners.read_actor_owner(address),
                )
                .await;
                Ok(matches!(confirmed, Ok(Ok(Some(record))) if record.record == expected))
            }
        }
    }

    pub(crate) async fn release_local_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<(), SendError> {
        let lock = self.resolution_lock(address).await;
        let _guard = lock.lock_owned().await;
        let current = tokio::time::timeout(
            self.operation_timeout,
            self.owners.read_actor_owner(address),
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
            self.owners.release_actor_owner(address, &current),
        )
        .await
        .map_err(|_| SendError::OwnershipUnavailable)?
        .map_err(|_| SendError::OwnershipUnavailable)?;
        let confirmed = match released {
            MutationOutcome::Applied(_) => true,
            MutationOutcome::Conflict | MutationOutcome::Indeterminate(_) => {
                let expected = ActorOwnerRecord::unowned(current.record.ownership_epoch);
                let mut confirmed = false;
                for _ in 0..3 {
                    let read = tokio::time::timeout(
                        self.operation_timeout,
                        self.owners.read_actor_owner(address),
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
        let nodes = tokio::time::timeout(self.operation_timeout, self.directory.list_nodes())
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
        let candidates: Vec<Candidate> = nodes
            .into_iter()
            .filter(|node| {
                node.session_id != self.session_id
                    && node.protocol_version == protocol_version
                    && !node.pressured
                    && !node.draining
                    && node.active_actor_count < node.max_actor_count
            })
            .map(|node| Candidate {
                endpoint: crate::transport::Endpoint::new(node.advertised_endpoint),
                active_actor_count: node.active_actor_count,
                max_actor_count: node.max_actor_count,
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
