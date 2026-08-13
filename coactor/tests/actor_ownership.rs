use std::{
    collections::HashMap,
    net::{Ipv4Addr, SocketAddr},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use coactor::{
    ActorAddress, ActorId, ActorOwner, ActorOwnerRecord, ActorOwnerStorage, AmbiguousMutation,
    CommandContext, DistributedRuntimeConfig, LeaseMutation, LeaseTiming, NodeLease,
    NodeLeaseStorage, NodeSessionId, OwnershipStorageError, RuntimeBuilder,
    VersionedActorOwnerRecord, VersionedNodeLease, actor,
};
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct AddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, Message)]
struct AddResponse {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Default)]
struct CounterActor {
    value: i64,
}

#[actor(name = "owned-counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self::default()
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: AddRequest) -> AddResponse {
        self.value += request.amount;
        AddResponse { value: self.value }
    }
}

#[derive(Default)]
struct OwnershipState {
    leases: HashMap<String, VersionedNodeLease>,
    owners: HashMap<ActorAddress, VersionedActorOwnerRecord>,
    next_etag: u64,
}

#[derive(Default)]
struct OwnershipFake {
    state: Mutex<OwnershipState>,
    owner_reads: AtomicUsize,
    owner_claims: AtomicUsize,
    ambiguous_next_claim: Mutex<bool>,
    lose_next_claim_response_without_applying: Mutex<bool>,
    reject_next_claim_for_node: Mutex<Option<String>>,
}

impl OwnershipFake {
    fn next_etag(state: &mut OwnershipState) -> String {
        state.next_etag += 1;
        format!("etag-{}", state.next_etag)
    }

    fn owner(&self, address: &ActorAddress) -> VersionedActorOwnerRecord {
        self.state.lock().unwrap().owners[address].clone()
    }
}

#[async_trait]
impl NodeLeaseStorage for OwnershipFake {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        let mut state = self.state.lock().unwrap();
        let etag = Self::next_etag(&mut state);
        state.leases.insert(
            lease.session_id.as_str().to_owned(),
            VersionedNodeLease {
                lease,
                etag: etag.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag })
    }

    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipStorageError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .leases
            .get(session_id.as_str())
            .cloned())
    }

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        _etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.acquire_node_lease(lease).await
    }

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.state
            .lock()
            .unwrap()
            .leases
            .remove(session_id.as_str());
        Ok(LeaseMutation::Applied {
            etag: etag.to_owned(),
        })
    }
}

#[async_trait]
impl ActorOwnerStorage for OwnershipFake {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, OwnershipStorageError> {
        self.owner_reads.fetch_add(1, Ordering::Relaxed);
        Ok(self.state.lock().unwrap().owners.get(address).cloned())
    }

    async fn claim_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.owner_claims.fetch_add(1, Ordering::Relaxed);
        let mut state = self.state.lock().unwrap();
        let matches = match (state.owners.get(address), etag) {
            (None, None) => true,
            (Some(current), Some(etag)) => current.etag == etag,
            _ => false,
        };
        if !matches {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        if std::mem::take(
            &mut *self
                .lose_next_claim_response_without_applying
                .lock()
                .unwrap(),
        ) {
            return Ok(LeaseMutation::Ambiguous(AmbiguousMutation::ResponseLost));
        }
        if let Some(node_id) = self.reject_next_claim_for_node.lock().unwrap().take() {
            let winner = state
                .leases
                .values()
                .find(|lease| lease.lease.node_id == node_id)
                .unwrap()
                .lease
                .clone();
            let next_etag = Self::next_etag(&mut state);
            state.owners.insert(
                address.clone(),
                VersionedActorOwnerRecord {
                    record: ActorOwnerRecord {
                        owner: Some(ActorOwner {
                            node_id: winner.node_id,
                            session_id: winner.session_id,
                        }),
                        ownership_epoch: record.ownership_epoch,
                    },
                    etag: next_etag,
                },
            );
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let next_etag = Self::next_etag(&mut state);
        state.owners.insert(
            address.clone(),
            VersionedActorOwnerRecord {
                record,
                etag: next_etag.clone(),
            },
        );
        if std::mem::take(&mut *self.ambiguous_next_claim.lock().unwrap()) {
            Ok(LeaseMutation::Ambiguous(AmbiguousMutation::ResponseLost))
        } else {
            Ok(LeaseMutation::Applied { etag: next_etag })
        }
    }
}

async fn free_address() -> SocketAddr {
    let listener = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn config(node: &str, address: SocketAddr) -> DistributedRuntimeConfig {
    DistributedRuntimeConfig::new(node, address, address).lease_timing(LeaseTiming {
        ttl: Duration::from_secs(60),
        renewal_interval: Duration::from_secs(20),
        operation_timeout: Duration::from_secs(1),
        peer_connect_timeout: Duration::from_secs(1),
    })
}

#[tokio::test]
async fn concurrent_cold_calls_share_one_claim_and_one_active_actor() {
    let storage = Arc::new(OwnershipFake::default());
    let address = free_address().await;
    let runtime = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-a", address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("shared"))
        .unwrap();

    let mut calls = Vec::new();
    for _ in 0..8 {
        let counter = counter.clone();
        calls.push(tokio::spawn(async move {
            counter.add(AddRequest { amount: 1 }).await.unwrap().value
        }));
    }
    let mut values = Vec::new();
    for call in calls {
        values.push(call.await.unwrap());
    }
    values.sort_unstable();

    assert_eq!(values, (1..=8).collect::<Vec<_>>());
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    runtime.shutdown().await;
}

#[tokio::test]
async fn a_losing_node_resolves_the_winner_and_forwards_the_typed_call() {
    let storage = Arc::new(OwnershipFake::default());
    let first_address = free_address().await;
    let second_address = free_address().await;
    let first = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-a", first_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let second = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-b", second_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let first_ref = first
        .actor_ref::<CounterActor>(ActorId::from("routed"))
        .unwrap();
    let second_ref = second
        .actor_ref::<CounterActor>(ActorId::from("routed"))
        .unwrap();

    assert_eq!(
        first_ref.add(AddRequest { amount: 2 }).await.unwrap().value,
        2
    );
    assert_eq!(
        second_ref
            .add(AddRequest { amount: 3 })
            .await
            .unwrap()
            .value,
        5
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn a_conditional_claim_rejection_is_reresolved_to_the_winner() {
    let storage = Arc::new(OwnershipFake::default());
    let first_address = free_address().await;
    let second_address = free_address().await;
    let first = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-a", first_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let second = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-b", second_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    *storage.reject_next_claim_for_node.lock().unwrap() = Some("node-a".to_owned());
    let counter = second
        .actor_ref::<CounterActor>(ActorId::from("claim-race"))
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 4 }).await.unwrap().value,
        4
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn local_capacity_is_reserved_before_an_owner_claim() {
    let storage = Arc::new(OwnershipFake::default());
    let address = free_address().await;
    let runtime = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-a", address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let first = runtime
        .actor_ref::<CounterActor>(ActorId::from("capacity-1"))
        .unwrap();
    let second = runtime
        .actor_ref::<CounterActor>(ActorId::from("capacity-2"))
        .unwrap();

    assert_eq!(first.add(AddRequest { amount: 1 }).await.unwrap().value, 1);
    assert_eq!(
        second.add(AddRequest { amount: 1 }).await,
        Err(coactor::SendError::RuntimeAtCapacity)
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);

    runtime.shutdown().await;
}

#[tokio::test]
async fn ambiguous_claim_is_confirmed_by_exact_owner_and_epoch_read_back() {
    let storage = Arc::new(OwnershipFake::default());
    *storage.ambiguous_next_claim.lock().unwrap() = true;
    let address = free_address().await;
    let runtime = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-a", address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_id = ActorId::from("ambiguous");
    let counter = runtime.actor_ref::<CounterActor>(actor_id.clone()).unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 1 }).await.unwrap().value,
        1
    );
    let record = storage.owner(&ActorAddress::new("owned-counter", actor_id));
    assert_eq!(record.record.ownership_epoch, 1);
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    assert!(storage.owner_reads.load(Ordering::Relaxed) <= 4);
    runtime.shutdown().await;
}

#[tokio::test]
async fn unreconciled_ambiguous_claim_is_bounded_and_never_replayed() {
    let storage = Arc::new(OwnershipFake::default());
    *storage
        .lose_next_claim_response_without_applying
        .lock()
        .unwrap() = true;
    let address = free_address().await;
    let runtime = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-a", address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("unconfirmed"))
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 1 }).await,
        Err(coactor::SendError::RemoteUnavailable)
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    assert_eq!(storage.owner_reads.load(Ordering::Relaxed), 4);
    runtime.shutdown().await;
}

#[tokio::test]
async fn releasing_an_owner_preserves_the_record_and_epoch() {
    let storage = OwnershipFake::default();
    let address = ActorAddress::new("owned-counter", ActorId::from("released"));
    let owner = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: serde_json::from_str("\"session-a\"").unwrap(),
        }),
        ownership_epoch: 7,
    };
    storage
        .claim_actor_owner(&address, owner, None)
        .await
        .unwrap();
    let current = storage.read_actor_owner(&address).await.unwrap().unwrap();

    storage
        .release_actor_owner(&address, &current)
        .await
        .unwrap();

    let released = storage.read_actor_owner(&address).await.unwrap().unwrap();
    assert_eq!(released.record.owner, None);
    assert_eq!(released.record.ownership_epoch, 7);
}

#[test]
fn an_unowned_record_preserves_the_existing_epoch() {
    assert_eq!(ActorOwnerRecord::unowned(7).ownership_epoch, 7);
    assert_eq!(ActorOwnerRecord::unowned(7).owner, None);
}
