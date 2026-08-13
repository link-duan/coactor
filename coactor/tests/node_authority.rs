use std::{
    collections::VecDeque,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use coactor::{
    ActorId, AmbiguousMutation, CommandContext, DistributedRuntimeConfig, LeaseMutation,
    LeaseTiming, NodeLease, NodeLeaseStorage, NodeSessionId, OwnershipStorageError, RuntimeBuilder,
    RuntimeStartError, RuntimeTerminationReason, SendError, VersionedNodeLease, actor,
};
use tokio::sync::Notify;

#[derive(Default)]
struct FakeLeaseStorage {
    acquire: Mutex<VecDeque<Result<LeaseMutation, OwnershipStorageError>>>,
    renew: Mutex<VecDeque<Result<LeaseMutation, OwnershipStorageError>>>,
    read: Mutex<VecDeque<Result<Option<VersionedNodeLease>, OwnershipStorageError>>>,
    confirm_latest_acquire: Mutex<bool>,
    confirm_latest_renewal: Mutex<bool>,
    acquired: Mutex<Vec<NodeLease>>,
    renewed: Mutex<Vec<(NodeLease, String)>>,
    renew_block: Mutex<Option<Arc<Notify>>>,
    acquire_block: Mutex<Option<Arc<Notify>>>,
    reads: Mutex<usize>,
    released: Mutex<Vec<(NodeSessionId, String)>>,
}

#[async_trait]
impl NodeLeaseStorage for FakeLeaseStorage {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.acquired.lock().unwrap().push(lease);
        let block = self.acquire_block.lock().unwrap().clone();
        if let Some(block) = block {
            block.notified().await;
        }
        self.acquire
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(LeaseMutation::Applied {
                etag: "lease-1".to_owned(),
            }))
    }

    async fn read_node_lease(
        &self,
        _session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipStorageError> {
        *self.reads.lock().unwrap() += 1;
        if let Some(result) = self.read.lock().unwrap().pop_front() {
            return result;
        }
        if *self.confirm_latest_acquire.lock().unwrap() {
            if let Some(lease) = self.acquired.lock().unwrap().last() {
                return Ok(Some(VersionedNodeLease {
                    lease: lease.clone(),
                    etag: "lease-after-lost-acquire-response".to_owned(),
                }));
            }
        }
        if *self.confirm_latest_renewal.lock().unwrap() {
            if let Some((lease, _)) = self.renewed.lock().unwrap().last() {
                return Ok(Some(VersionedNodeLease {
                    lease: lease.clone(),
                    etag: "lease-after-lost-response".to_owned(),
                }));
            }
        }
        Ok(None)
    }

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.renewed.lock().unwrap().push((lease, etag.to_owned()));
        let block = self.renew_block.lock().unwrap().clone();
        if let Some(block) = block {
            block.notified().await;
        }
        self.renew
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(Ok(LeaseMutation::Applied {
                etag: "renewed".to_owned(),
            }))
    }

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.released
            .lock()
            .unwrap()
            .push((session_id.clone(), etag.to_owned()));
        Ok(LeaseMutation::Applied {
            etag: etag.to_owned(),
        })
    }
}

#[derive(Clone, Default)]
struct BlockingState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct BlockingActor(Arc<BlockingState>);

#[actor(name = "lease-blocking")]
impl BlockingActor {
    pub fn new(_actor_id: ActorId, state: Arc<BlockingState>) -> Self {
        Self(state)
    }

    #[coactor::command]
    pub async fn block(&mut self, _context: &CommandContext) -> u64 {
        self.0.entered.notify_one();
        self.0.release.notified().await;
        7
    }

    #[coactor::command]
    pub async fn fail_after_release(
        &mut self,
        _context: &CommandContext,
    ) -> Result<(), &'static str> {
        self.0.entered.notify_one();
        self.0.release.notified().await;
        Err("business failure")
    }
}

fn fast_timing() -> LeaseTiming {
    LeaseTiming {
        ttl: Duration::from_secs(9),
        renewal_interval: Duration::from_secs(3),
        operation_timeout: Duration::from_secs(15),
        peer_connect_timeout: Duration::from_secs(3),
    }
}

fn config() -> DistributedRuntimeConfig {
    DistributedRuntimeConfig::new(
        "node-a",
        "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        "127.0.0.1:41001".parse::<SocketAddr>().unwrap(),
    )
}

#[test]
fn distributed_runtime_configuration_is_validated_before_startup() {
    let storage = Arc::new(FakeLeaseStorage::default());

    let empty_id = RuntimeBuilder::new(())
        .distributed(
            DistributedRuntimeConfig::new(
                " ",
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:41001".parse().unwrap(),
            ),
            storage.clone(),
        )
        .unwrap_err();
    assert_eq!(empty_id, RuntimeStartError::InvalidNodeId);

    let missing_advertised_port = RuntimeBuilder::new(())
        .distributed(
            DistributedRuntimeConfig::new(
                "node-a",
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
            ),
            storage.clone(),
        )
        .unwrap_err();
    assert_eq!(
        missing_advertised_port,
        RuntimeStartError::InvalidAdvertisedAddress
    );

    let invalid_timing = RuntimeBuilder::new(())
        .distributed(
            config().lease_timing(LeaseTiming {
                ttl: Duration::from_secs(1),
                renewal_interval: Duration::from_secs(1),
                operation_timeout: Duration::from_secs(1),
                peer_connect_timeout: Duration::from_secs(1),
            }),
            storage,
        )
        .unwrap_err();
    assert_eq!(invalid_timing, RuntimeStartError::InvalidLeaseTiming);
}

#[tokio::test]
async fn invalid_runtime_configuration_does_not_acquire_node_authority() {
    let storage = Arc::new(FakeLeaseStorage::default());
    let result = RuntimeBuilder::new(())
        .mailbox_capacity(0)
        .distributed(config(), storage.clone())
        .unwrap()
        .start()
        .await;

    assert!(matches!(
        result,
        Err(RuntimeStartError::Build(
            coactor::BuildError::InvalidMailboxCapacity
        ))
    ));
    assert!(storage.acquired.lock().unwrap().is_empty());
}

#[tokio::test]
async fn startup_acquires_a_unique_node_session_before_returning_a_runtime() {
    let first_storage = Arc::new(FakeLeaseStorage::default());
    let first = RuntimeBuilder::new(())
        .distributed(config(), first_storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let second_storage = Arc::new(FakeLeaseStorage::default());
    let second = RuntimeBuilder::new(())
        .distributed(config(), second_storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();

    let first_lease = first_storage.acquired.lock().unwrap()[0].clone();
    let second_lease = second_storage.acquired.lock().unwrap()[0].clone();
    assert_eq!(first_lease.node_id, "node-a");
    assert_eq!(first_lease.advertised_address, config().advertised_address);
    assert_ne!(first_lease.session_id, second_lease.session_id);
    assert!(first_lease.expires_at_unix_ms > 0);

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn an_ambiguous_acquire_requires_exact_bounded_read_back() {
    let confirmed = Arc::new(FakeLeaseStorage::default());
    confirmed
        .acquire
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Ambiguous(
            AmbiguousMutation::ResponseLost,
        )));
    *confirmed.confirm_latest_acquire.lock().unwrap() = true;
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), confirmed.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    assert_eq!(*confirmed.reads.lock().unwrap(), 1);
    runtime.shutdown().await;

    let rejected = Arc::new(FakeLeaseStorage::default());
    rejected
        .acquire
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Ambiguous(
            AmbiguousMutation::ResponseLost,
        )));
    assert!(matches!(
        RuntimeBuilder::new(())
            .distributed(config().lease_timing(fast_timing()), rejected.clone())
            .unwrap()
            .start()
            .await,
        Err(RuntimeStartError::LeaseUnconfirmed)
    ));
    assert_eq!(*rejected.reads.lock().unwrap(), 3);
}

#[tokio::test]
async fn graceful_shutdown_reports_shutdown_without_terminating_the_host() {
    let storage = Arc::new(FakeLeaseStorage::default());
    let runtime = RuntimeBuilder::new(())
        .distributed(config(), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let supervision = runtime.supervision().unwrap();

    runtime.shutdown().await;

    assert_eq!(
        supervision.terminated().await.reason,
        RuntimeTerminationReason::Shutdown
    );
    let released = storage.released.lock().unwrap();
    assert_eq!(released.len(), 1);
    assert_eq!(released[0].1, "lease-1");
}

#[tokio::test(start_paused = true)]
async fn graceful_shutdown_releases_with_the_latest_renewed_etag() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Applied {
            etag: "lease-2".to_owned(),
        }));
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;

    runtime.shutdown().await;

    assert_eq!(storage.released.lock().unwrap()[0].1, "lease-2");
}

#[tokio::test(start_paused = true)]
async fn a_temporary_renewal_failure_retries_while_local_authority_remains_valid() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Err(OwnershipStorageError::Unavailable));
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Applied {
            etag: "lease-2".to_owned(),
        }));
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let supervision = runtime.supervision().unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert_eq!(storage.renewed.lock().unwrap().len(), 1);
    assert!(
        tokio::time::timeout(Duration::ZERO, supervision.clone().terminated())
            .await
            .is_err()
    );

    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert_eq!(storage.renewed.lock().unwrap().len(), 2);
    assert!(
        tokio::time::timeout(Duration::ZERO, supervision.terminated())
            .await
            .is_err()
    );

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn repeated_temporary_failures_fence_at_the_original_monotonic_deadline() {
    let storage = Arc::new(FakeLeaseStorage::default());
    for _ in 0..3 {
        storage
            .renew
            .lock()
            .unwrap()
            .push_back(Err(OwnershipStorageError::Unavailable));
    }
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::ZERO, runtime.supervision().unwrap().terminated())
            .await
            .is_err()
    );
    tokio::time::advance(Duration::from_secs(1)).await;
    assert_eq!(
        runtime.supervision().unwrap().terminated().await.reason,
        RuntimeTerminationReason::Fenced
    );
    assert_eq!(storage.renewed.lock().unwrap().len(), 2);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn conditional_lease_loss_fences_pending_and_new_actor_calls() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::ConditionalRejected));
    let state = BlockingState::default();
    let runtime = RuntimeBuilder::new(state.clone())
        .register::<BlockingActor>()
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_ref = runtime
        .actor_ref::<BlockingActor>(ActorId::from("one"))
        .unwrap();
    let pending = tokio::spawn({
        let actor_ref = actor_ref.clone();
        async move { actor_ref.block().await }
    });
    state.entered.notified().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    let termination = runtime.supervision().unwrap().terminated().await;
    assert_eq!(termination.reason, RuntimeTerminationReason::Fenced);
    assert_eq!(pending.await.unwrap(), Err(SendError::NodeFenced));
    assert_eq!(actor_ref.block().await, Err(SendError::NodeFenced));

    runtime.shutdown().await;
    assert!(storage.released.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn fencing_supersedes_a_pending_business_error_reply() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::ConditionalRejected));
    let state = BlockingState::default();
    let runtime = RuntimeBuilder::new(state.clone())
        .register::<BlockingActor>()
        .distributed(config().lease_timing(fast_timing()), storage)
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_ref = runtime
        .actor_ref::<BlockingActor>(ActorId::from("business-error"))
        .unwrap();
    let pending = tokio::spawn(async move { actor_ref.fail_after_release().await });
    state.entered.notified().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(
        runtime.supervision().unwrap().terminated().await.reason,
        RuntimeTerminationReason::Fenced
    );
    assert_eq!(pending.await.unwrap(), Err(SendError::NodeFenced));

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_process_pause_past_the_monotonic_deadline_fences_before_renewing() {
    let storage = Arc::new(FakeLeaseStorage::default());
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();

    tokio::time::advance(Duration::from_secs(10)).await;
    let termination = runtime.supervision().unwrap().terminated().await;
    assert_eq!(termination.reason, RuntimeTerminationReason::Fenced);
    assert!(storage.renewed.lock().unwrap().is_empty());

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn an_ambiguous_renewal_is_confirmed_by_exact_read_back() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Ambiguous(
            AmbiguousMutation::ResponseLost,
        )));
    *storage.confirm_latest_renewal.lock().unwrap() = true;
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::ZERO, runtime.supervision().unwrap().terminated())
            .await
            .is_err()
    );
    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_delayed_renewal_response_cannot_extend_authority_past_its_deadline() {
    let storage = Arc::new(FakeLeaseStorage::default());
    let release = Arc::new(Notify::new());
    *storage.renew_block.lock().unwrap() = Some(release.clone());
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    tokio::task::yield_now().await;
    assert_eq!(storage.renewed.lock().unwrap().len(), 1);
    tokio::time::advance(Duration::from_secs(6)).await;
    let termination = runtime.supervision().unwrap().terminated().await;
    assert_eq!(termination.reason, RuntimeTerminationReason::Fenced);
    release.notify_one();

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn a_delayed_acquire_response_cannot_start_with_expired_authority() {
    let storage = Arc::new(FakeLeaseStorage::default());
    let release = Arc::new(Notify::new());
    *storage.acquire_block.lock().unwrap() = Some(release);
    let startup = tokio::spawn(
        RuntimeBuilder::new(())
            .distributed(config().lease_timing(fast_timing()), storage)
            .unwrap()
            .start(),
    );
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(9)).await;
    assert!(matches!(
        startup.await.unwrap(),
        Err(RuntimeStartError::LeaseUnconfirmed)
    ));
}

#[tokio::test(start_paused = true)]
async fn unreconciled_renewal_ambiguity_is_bounded_then_fences() {
    let storage = Arc::new(FakeLeaseStorage::default());
    storage
        .renew
        .lock()
        .unwrap()
        .push_back(Ok(LeaseMutation::Ambiguous(
            AmbiguousMutation::ResponseLost,
        )));
    let runtime = RuntimeBuilder::new(())
        .distributed(config().lease_timing(fast_timing()), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    tokio::task::yield_now().await;

    tokio::time::advance(Duration::from_secs(3)).await;
    let termination = runtime.supervision().unwrap().terminated().await;
    assert_eq!(termination.reason, RuntimeTerminationReason::Fenced);
    assert_eq!(*storage.reads.lock().unwrap(), 3);

    runtime.shutdown().await;
}
