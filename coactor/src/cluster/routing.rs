use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::wall_time_millis;
use crate::{
    ActorAddress, ActorOwner, ActorOwnerRecord, LeaseMutation, NodeSessionId, OwnershipBackend,
    SendError,
};

pub(crate) struct ClusterRouter {
    storage: Arc<dyn OwnershipBackend>,
    node_id: String,
    session_id: NodeSessionId,
    local_endpoint: String,
    operation_timeout: Duration,
    pub(crate) peer_connect_timeout: Duration,
    resolutions: tokio::sync::Mutex<HashMap<ActorAddress, Arc<tokio::sync::Mutex<()>>>>,
    resolved: tokio::sync::Mutex<HashMap<ActorAddress, CachedOwner>>,
}

impl ClusterRouter {
    pub(crate) fn new(
        storage: Arc<dyn OwnershipBackend>,
        node_id: String,
        session_id: NodeSessionId,
        local_endpoint: String,
        operation_timeout: Duration,
        peer_connect_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            node_id,
            session_id,
            local_endpoint,
            operation_timeout,
            peer_connect_timeout,
            resolutions: tokio::sync::Mutex::new(HashMap::new()),
            resolved: tokio::sync::Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn local_node_endpoint(&self) -> String {
        self.local_endpoint.clone()
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

    pub(crate) async fn resolve(
        &self,
        address: &ActorAddress,
        capacity: &Arc<Semaphore>,
    ) -> Result<ResolvedOwner, SendError> {
        let lock = self.resolution_lock(address).await;
        let guard = lock.lock_owned().await;
        if let Some(cached) = self.resolved.lock().await.get(address).cloned() {
            return Ok(match cached {
                CachedOwner::Local => ResolvedOwner::Local {
                    reservation: None,
                    guard,
                },
                CachedOwner::Remote {
                    endpoint,
                    protocol_version,
                } => ResolvedOwner::Remote {
                    endpoint,
                    protocol_version,
                },
                CachedOwner::ReleasePending => {
                    return Err(SendError::OwnershipUnavailable);
                }
            });
        }
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
                    self.resolved
                        .lock()
                        .await
                        .insert(address.clone(), CachedOwner::Local);
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
                            let endpoint = format!("http://{}", lease.lease.advertised_address);
                            let protocol_version = lease.lease.protocol_version;
                            self.resolved.lock().await.insert(
                                address.clone(),
                                CachedOwner::Remote {
                                    endpoint: endpoint.clone(),
                                    protocol_version,
                                },
                            );
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
                    self.resolved
                        .lock()
                        .await
                        .insert(address.clone(), CachedOwner::Local);
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
                                self.resolved
                                    .lock()
                                    .await
                                    .insert(address.clone(), CachedOwner::Local);
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

    #[allow(dead_code)]
    pub(crate) async fn invalidate(&self, address: &ActorAddress) {
        self.resolved.lock().await.remove(address);
    }

    /// 到某 Node 的连接断开时，失效所有指向该 Node 的解析缓存（failover 惰性检测依赖）。
    pub(crate) async fn invalidate_endpoint(&self, endpoint: &str) {
        let mut resolved = self.resolved.lock().await;
        resolved.retain(|_, owner| match owner {
            CachedOwner::Remote { endpoint: cached, .. } => cached != endpoint,
            _ => true,
        });
    }

    pub(crate) async fn release_local_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<(), SendError> {
        let lock = self.resolution_lock(address).await;
        let _guard = lock.lock_owned().await;
        self.resolved
            .lock()
            .await
            .insert(address.clone(), CachedOwner::ReleasePending);
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
        self.resolved.lock().await.remove(address);
        Ok(())
    }

    #[allow(dead_code)]
    pub(crate) async fn placement_candidates(
        &self,
        protocol_version: u32,
    ) -> Result<Vec<(String, u32)>, SendError> {
        let leases = tokio::time::timeout(self.operation_timeout, self.storage.list_node_leases())
            .await
            .map_err(|_| SendError::OwnershipUnavailable)?
            .map_err(|_| SendError::OwnershipUnavailable)?;
        let now = wall_time_millis();
        let mut candidates: Vec<_> = leases
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
            .collect();
        candidates.sort_by(|left, right| {
            left.active_actor_count
                .cmp(&right.active_actor_count)
                .then_with(|| left.node_id.cmp(&right.node_id))
                .then_with(|| left.session_id.as_str().cmp(right.session_id.as_str()))
        });
        Ok(candidates
            .into_iter()
            .take(2)
            .map(|lease| {
                (
                    format!("http://{}", lease.advertised_address),
                    lease.protocol_version,
                )
            })
            .collect())
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

#[derive(Clone)]
enum CachedOwner {
    Local,
    ReleasePending,
    #[allow(dead_code)]
    Remote {
        endpoint: String,
        protocol_version: u32,
    },
}
