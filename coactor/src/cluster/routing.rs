use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

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
    resolutions: tokio::sync::Mutex<HashMap<ActorAddress, Arc<tokio::sync::Mutex<()>>>>,
}

impl ClusterRouter {
    pub(crate) fn new(
        directory: Arc<dyn NodeDirectory>,
        owners: Arc<dyn ActorOwnerStore>,
        node_id: String,
        session_id: NodeSessionId,
        operation_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            directory,
            owners,
            node_id,
            session_id,
            operation_timeout,
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
                        return self
                            .claim(
                                address,
                                current.record.ownership_epoch.saturating_add(1),
                                Some(&current.revision),
                                capacity,
                                guard,
                            )
                            .await;
                    }
                    let node = tokio::time::timeout(
                        self.operation_timeout,
                        self.directory.read_node(&owner.node_id),
                    )
                    .await
                    .map_err(|_| SendError::OwnershipUnavailable)?
                    .map_err(|_| SendError::OwnershipUnavailable)?;
                    if node.is_some_and(|node| node.session_id == owner.session_id) {
                        return Ok(ResolvedOwner::Remote);
                    }
                    tracing::info!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        prior_epoch = current.record.ownership_epoch,
                        lifecycle = "availability_failover",
                        "Actor Owner Node Lease is absent or stale; attempting empty-state takeover"
                    );
                }
            }

            let epoch = current.as_ref().map_or(1, |current| {
                current.record.ownership_epoch.saturating_add(1)
            });
            let revision = current.as_ref().map(|current| &current.revision);
            let expected = self.local_claim(epoch);
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
        Ok(ResolvedOwner::Remote)
    }

    async fn claim(
        &self,
        address: &ActorAddress,
        epoch: u64,
        revision: Option<&crate::Revision>,
        capacity: &Arc<Semaphore>,
        guard: tokio::sync::OwnedMutexGuard<()>,
    ) -> Result<ResolvedOwner, SendError> {
        let expected = self.local_claim(epoch);
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
            MutationOutcome::Applied(_) => Ok(ResolvedOwner::Local {
                reservation: Some(reservation),
                guard,
            }),
            MutationOutcome::Conflict => Ok(ResolvedOwner::Remote),
            MutationOutcome::Indeterminate(_) => {
                let confirmed = tokio::time::timeout(
                    self.operation_timeout,
                    self.owners.read_actor_owner(address),
                )
                .await;
                if matches!(confirmed, Ok(Ok(Some(record))) if record.record == expected) {
                    Ok(ResolvedOwner::Local {
                        reservation: Some(reservation),
                        guard,
                    })
                } else {
                    Err(SendError::OwnershipUnavailable)
                }
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
        confirmed
            .then_some(())
            .ok_or(SendError::OwnershipUnavailable)
    }
}

pub(crate) enum ResolvedOwner {
    Local {
        reservation: Option<OwnedSemaphorePermit>,
        guard: tokio::sync::OwnedMutexGuard<()>,
    },
    Remote,
}
