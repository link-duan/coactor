use std::{
    collections::HashMap,
    io,
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
use tracing_subscriber::fmt::MakeWriter;

#[derive(Clone, Default)]
struct Captured(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl io::Write for CapturedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Captured {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

impl Captured {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

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

#[derive(Clone, Default)]
struct RebuildingState {
    activations: Arc<AtomicUsize>,
    external_seed: i64,
}

struct RebuildingActor {
    state: Arc<RebuildingState>,
    value: i64,
}

#[actor(name = "rebuilding-counter")]
impl RebuildingActor {
    pub fn new(_actor_id: ActorId, state: Arc<RebuildingState>) -> Self {
        Self { state, value: 0 }
    }

    pub async fn on_activate(&mut self) -> Result<(), String> {
        self.state.activations.fetch_add(1, Ordering::Relaxed);
        self.value = self.state.external_seed;
        Ok(())
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: AddRequest) -> AddResponse {
        self.value += request.amount;
        AddResponse { value: self.value }
    }
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
    redirect_owner_after_read: Mutex<HashMap<ActorAddress, String>>,
    ambiguous_next_claim: Mutex<bool>,
    lose_next_claim_response_without_applying: Mutex<bool>,
    reject_next_claim_for_node: Mutex<Option<String>>,
    fail_lease_reads: Mutex<bool>,
}

impl OwnershipFake {
    fn next_etag(state: &mut OwnershipState) -> String {
        state.next_etag += 1;
        format!("etag-{}", state.next_etag)
    }

    fn owner(&self, address: &ActorAddress) -> VersionedActorOwnerRecord {
        self.state.lock().unwrap().owners[address].clone()
    }

    fn remove_lease(&self, session_id: &NodeSessionId) {
        self.state
            .lock()
            .unwrap()
            .leases
            .remove(session_id.as_str());
    }

    fn redirect_lease(&self, session_id: &NodeSessionId, address: SocketAddr) {
        self.state
            .lock()
            .unwrap()
            .leases
            .get_mut(session_id.as_str())
            .unwrap()
            .lease
            .advertised_address = address;
    }

    fn assign_owner(&self, address: &ActorAddress, node_id: &str, ownership_epoch: u64) {
        let mut state = self.state.lock().unwrap();
        let owner = state
            .leases
            .values()
            .find(|lease| lease.lease.node_id == node_id)
            .unwrap()
            .lease
            .clone();
        let etag = Self::next_etag(&mut state);
        state.owners.insert(
            address.clone(),
            VersionedActorOwnerRecord {
                record: ActorOwnerRecord {
                    owner: Some(ActorOwner {
                        node_id: owner.node_id,
                        session_id: owner.session_id,
                    }),
                    ownership_epoch,
                },
                etag,
            },
        );
    }

    fn redirect_owner_after_read(&self, address: &ActorAddress, node_id: &str) {
        self.redirect_owner_after_read
            .lock()
            .unwrap()
            .insert(address.clone(), node_id.to_owned());
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
        if *self.fail_lease_reads.lock().unwrap() {
            return Err(OwnershipStorageError::Unavailable);
        }
        Ok(self
            .state
            .lock()
            .unwrap()
            .leases
            .get(session_id.as_str())
            .cloned())
    }

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipStorageError> {
        Ok(self
            .state
            .lock()
            .unwrap()
            .leases
            .values()
            .cloned()
            .collect())
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
        let redirect = self
            .redirect_owner_after_read
            .lock()
            .unwrap()
            .remove(address);
        let mut state = self.state.lock().unwrap();
        let current = state.owners.get(address).cloned();
        if let Some(node_id) = redirect {
            let owner = state
                .leases
                .values()
                .find(|lease| lease.lease.node_id == node_id)
                .unwrap()
                .lease
                .clone();
            let etag = Self::next_etag(&mut state);
            let ownership_epoch = current.as_ref().map_or(1, |current| {
                current.record.ownership_epoch.saturating_add(1)
            });
            state.owners.insert(
                address.clone(),
                VersionedActorOwnerRecord {
                    record: ActorOwnerRecord {
                        owner: Some(ActorOwner {
                            node_id: owner.node_id,
                            session_id: owner.session_id,
                        }),
                        ownership_epoch,
                    },
                    etag,
                },
            );
        }
        Ok(current)
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
async fn a_never_connected_route_refreshes_once_before_dispatch() {
    let storage = Arc::new(OwnershipFake::default());
    let stale_address = free_address().await;
    let winner_address = free_address().await;
    let unreachable = free_address().await;
    let stale = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-stale", stale_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let winner = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-winner", winner_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_id = ActorId::from("refresh-before-dispatch");
    let address = ActorAddress::new("owned-counter", actor_id.clone());
    storage.assign_owner(&address, "node-stale", 1);
    let stale_owner = storage.owner(&address);
    storage.redirect_lease(
        &stale_owner.record.owner.as_ref().unwrap().session_id,
        unreachable,
    );
    storage.redirect_owner_after_read(&address, "node-winner");

    let counter = winner.actor_ref::<CounterActor>(actor_id).unwrap();
    assert_eq!(
        counter.add(AddRequest { amount: 4 }).await.unwrap().value,
        4
    );
    assert_eq!(storage.owner_reads.load(Ordering::Relaxed), 2);

    stale.shutdown().await;
    winner.shutdown().await;
}

#[tokio::test]
async fn an_explicit_not_owner_response_refreshes_once() {
    let storage = Arc::new(OwnershipFake::default());
    let stale_address = free_address().await;
    let winner_address = free_address().await;
    let stale = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-stale", stale_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let winner = RuntimeBuilder::new(())
        .register::<CounterActor>()
        .distributed(config("node-winner", winner_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_id = ActorId::from("refresh-after-not-owner");
    let address = ActorAddress::new("owned-counter", actor_id.clone());
    storage.assign_owner(&address, "node-stale", 1);
    storage.redirect_owner_after_read(&address, "node-winner");

    let counter = winner.actor_ref::<CounterActor>(actor_id).unwrap();
    assert_eq!(
        counter.add(AddRequest { amount: 4 }).await.unwrap().value,
        4
    );
    assert_eq!(
        counter.add(AddRequest { amount: 1 }).await.unwrap().value,
        5
    );
    assert_eq!(storage.owner_reads.load(Ordering::Relaxed), 3);

    stale.shutdown().await;
    winner.shutdown().await;
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
async fn a_full_ingress_places_one_cold_actor_on_an_available_node() {
    let storage = Arc::new(OwnershipFake::default());
    let ingress_address = free_address().await;
    let target_address = free_address().await;
    let ingress = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-a", ingress_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let target = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-b", target_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    ingress
        .actor_ref::<CounterActor>(ActorId::from("occupies-ingress"))
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    let actor_id = ActorId::from("placed-remotely");

    assert_eq!(
        ingress
            .actor_ref::<CounterActor>(actor_id.clone())
            .unwrap()
            .add(AddRequest { amount: 4 })
            .await
            .unwrap()
            .value,
        4
    );
    let owner = storage.owner(&ActorAddress::new("owned-counter", actor_id));
    assert_eq!(owner.record.owner.unwrap().node_id, "node-b");

    ingress.shutdown().await;
    target.shutdown().await;
}

#[tokio::test]
async fn one_stale_capacity_rejection_selects_one_alternative() {
    let storage = Arc::new(OwnershipFake::default());
    let ingress_address = free_address().await;
    let stale_address = free_address().await;
    let alternative_address = free_address().await;
    let ingress = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-a", ingress_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let stale = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-b", stale_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let alternative = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-c", alternative_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    ingress
        .actor_ref::<CounterActor>(ActorId::from("occupies-ingress"))
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    stale
        .actor_ref::<CounterActor>(ActorId::from("occupies-stale-target"))
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    let claims_before = storage.owner_claims.load(Ordering::Relaxed);
    let actor_id = ActorId::from("placed-on-alternative");

    assert_eq!(
        ingress
            .actor_ref::<CounterActor>(actor_id.clone())
            .unwrap()
            .add(AddRequest { amount: 4 })
            .await
            .unwrap()
            .value,
        4
    );
    let owner = storage.owner(&ActorAddress::new("owned-counter", actor_id));
    assert_eq!(owner.record.owner.unwrap().node_id, "node-c");
    assert_eq!(
        storage.owner_claims.load(Ordering::Relaxed),
        claims_before + 1,
        "the full target rejects before ownership CAS"
    );

    ingress.shutdown().await;
    stale.shutdown().await;
    alternative.shutdown().await;
}

#[tokio::test]
async fn exhausted_placement_returns_runtime_capacity() {
    let storage = Arc::new(OwnershipFake::default());
    let ingress_address = free_address().await;
    let full_target_address = free_address().await;
    let ingress = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-a", ingress_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let full_target = RuntimeBuilder::new(())
        .max_active_actors(1)
        .register::<CounterActor>()
        .distributed(config("node-b", full_target_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    ingress
        .actor_ref::<CounterActor>(ActorId::from("occupies-ingress"))
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    full_target
        .actor_ref::<CounterActor>(ActorId::from("occupies-target"))
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();

    assert_eq!(
        ingress
            .actor_ref::<CounterActor>(ActorId::from("cannot-place"))
            .unwrap()
            .add(AddRequest { amount: 1 })
            .await,
        Err(coactor::SendError::RuntimeAtCapacity)
    );

    ingress.shutdown().await;
    full_target.shutdown().await;
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
        Err(coactor::SendError::OwnershipUnavailable)
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    assert_eq!(storage.owner_reads.load(Ordering::Relaxed), 4);
    runtime.shutdown().await;
}

#[tokio::test]
async fn absent_owner_lease_allows_higher_epoch_empty_state_failover() {
    let captured = Captured::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .without_time()
        .with_writer(captured.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let storage = Arc::new(OwnershipFake::default());
    let first_address = free_address().await;
    let second_address = free_address().await;
    let first_state = RebuildingState {
        external_seed: 0,
        ..RebuildingState::default()
    };
    let second_state = RebuildingState {
        external_seed: 40,
        ..RebuildingState::default()
    };
    let first = RuntimeBuilder::new(first_state.clone())
        .register::<RebuildingActor>()
        .distributed(config("node-a", first_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let second = RuntimeBuilder::new(second_state.clone())
        .register::<RebuildingActor>()
        .distributed(config("node-b", second_address), storage.clone())
        .unwrap()
        .start()
        .await
        .unwrap();
    let actor_id = ActorId::from("failed-over");
    let first_ref = first
        .actor_ref::<RebuildingActor>(actor_id.clone())
        .unwrap();
    let second_ref = second
        .actor_ref::<RebuildingActor>(actor_id.clone())
        .unwrap();

    assert_eq!(
        first_ref.add(AddRequest { amount: 5 }).await.unwrap().value,
        5
    );
    let address = ActorAddress::new("rebuilding-counter", actor_id);
    let prior = storage.owner(&address);
    storage.remove_lease(&prior.record.owner.as_ref().unwrap().session_id);

    assert_eq!(
        second_ref
            .add(AddRequest { amount: 1 })
            .await
            .unwrap()
            .value,
        41,
        "replacement uses consumer activation input, not prior CoActor memory"
    );
    assert_eq!(storage.owner(&address).record.ownership_epoch, 2);
    assert_eq!(first_state.activations.load(Ordering::Relaxed), 1);
    assert_eq!(second_state.activations.load(Ordering::Relaxed), 1);
    let trace = captured.text();
    assert!(trace.contains("lifecycle=\"availability_failover\""));
    assert!(!trace.contains("Recovery"));
    assert!(!trace.contains("Migration"));

    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn lease_read_failure_never_permits_takeover() {
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
    let actor_id = ActorId::from("lease-read-failed");
    first
        .actor_ref::<CounterActor>(actor_id.clone())
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    let owner = storage.owner(&ActorAddress::new("owned-counter", actor_id.clone()));
    *storage.fail_lease_reads.lock().unwrap() = true;

    assert_eq!(
        second
            .actor_ref::<CounterActor>(actor_id)
            .unwrap()
            .add(AddRequest { amount: 1 })
            .await,
        Err(coactor::SendError::OwnershipUnavailable)
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    assert_eq!(
        storage
            .owner(&ActorAddress::new(
                "owned-counter",
                ActorId::from("lease-read-failed")
            ))
            .record,
        owner.record
    );
    *storage.fail_lease_reads.lock().unwrap() = false;
    first.shutdown().await;
    second.shutdown().await;
}

#[tokio::test]
async fn unreachable_owner_endpoint_never_permits_takeover() {
    let storage = Arc::new(OwnershipFake::default());
    let first_address = free_address().await;
    let second_address = free_address().await;
    let unreachable = free_address().await;
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
    let actor_id = ActorId::from("endpoint-unreachable");
    first
        .actor_ref::<CounterActor>(actor_id.clone())
        .unwrap()
        .add(AddRequest { amount: 1 })
        .await
        .unwrap();
    let address = ActorAddress::new("owned-counter", actor_id.clone());
    let owner = storage.owner(&address);
    storage.redirect_lease(
        &owner.record.owner.as_ref().unwrap().session_id,
        unreachable,
    );

    assert_eq!(
        second
            .actor_ref::<CounterActor>(actor_id)
            .unwrap()
            .add(AddRequest { amount: 1 })
            .await,
        Err(coactor::SendError::RemoteUnavailable)
    );
    assert_eq!(storage.owner_claims.load(Ordering::Relaxed), 1);
    assert_eq!(storage.owner(&address).record, owner.record);
    first.shutdown().await;
    second.shutdown().await;
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
