use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use async_trait::async_trait;

use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::{
    Actor, ActorAddress, ActorOwnerReader, ActorRuntime, Client, CoordinationError, MessageContext,
    NodeDirectory, OpenError, PlacementContext, PlacementStrategy, ServerBuilderCore,
    VersionedActorOwnerRecord, actor,
    test_support::{TestCoordinationStore, start_cluster_inmem},
};

#[derive(Clone)]
struct State(&'static str);

#[actor]
struct Probe {
    runtime: ActorRuntime<State>,
}
impl Actor<State> for Probe {
    fn new(runtime: ActorRuntime<State>) -> Self {
        Self { runtime }
    }
    async fn on_message(&mut self, ctx: &MessageContext, _: &[u8]) {
        ctx.send(self.runtime.state().0.as_bytes().to_vec())
            .await
            .unwrap();
    }
}

fn core(state: State) -> ServerBuilderCore<State> {
    let mut core = ServerBuilderCore::base(Some(state));
    core.add_actor::<Probe>(crate::ActorConfig::new("probe"));
    core
}

fn core_with_limit(state: State, limit: usize) -> ServerBuilderCore<State> {
    let mut core = core(state);
    core.max_active_actors = limit;
    core
}

struct OrderedPlacement(Vec<String>);

impl PlacementStrategy for OrderedPlacement {
    fn candidates(&self, _: &ActorAddress, ctx: &PlacementContext) -> Vec<String> {
        self.0
            .iter()
            .filter(|endpoint| {
                ctx.candidates
                    .iter()
                    .any(|candidate| candidate.endpoint.as_str() == endpoint.as_str())
            })
            .cloned()
            .collect()
    }
}

#[derive(Clone)]
struct StaleOwnerView {
    store: Arc<TestCoordinationStore>,
    hidden_reads: Arc<AtomicUsize>,
}

impl StaleOwnerView {
    fn new(store: Arc<TestCoordinationStore>, hidden_reads: usize) -> Self {
        Self {
            store,
            hidden_reads: Arc::new(AtomicUsize::new(hidden_reads)),
        }
    }
}

#[async_trait]
impl NodeDirectory for StaleOwnerView {
    async fn read_node(
        &self,
        node_id: &str,
    ) -> Result<Option<crate::NodeRecord>, CoordinationError> {
        self.store.read_node(node_id).await
    }

    async fn list_nodes(&self) -> Result<Vec<crate::NodeRecord>, CoordinationError> {
        self.store.list_nodes().await
    }
}

#[async_trait]
impl ActorOwnerReader for StaleOwnerView {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        if self
            .hidden_reads
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Ok(None);
        }
        self.store.read_actor_owner(address).await
    }
}

#[tokio::test]
async fn client_reresolves_owner_after_not_owner_rejection() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let stale_target = start_cluster_inmem(
        core(State("stale-target")),
        store.clone(),
        "node-stale-target",
        registry.clone(),
    )
    .await;
    let owner = start_cluster_inmem(
        core(State("owner")),
        store.clone(),
        "node-real-owner",
        registry.clone(),
    )
    .await;
    let nodes = store.list_nodes().await.unwrap();
    let stale_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-stale-target")
        .unwrap()
        .advertised_endpoint
        .clone();
    let owner_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-real-owner")
        .unwrap()
        .advertised_endpoint
        .clone();
    let address = ActorAddress::new("probe", "not-owner-reresolve").unwrap();
    let owner_client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .placement_strategy(Arc::new(OrderedPlacement(vec![owner_endpoint])))
        .build()
        .unwrap();
    let _owner_session = owner_client.open(&address).await.unwrap();

    let client = Client::builder(StaleOwnerView::new(store.clone(), 1))
        .with_transport(Arc::new(InmemTransport::new(registry)))
        .placement_strategy(Arc::new(OrderedPlacement(vec![stale_endpoint])))
        .max_open_attempts(2)
        .build()
        .unwrap();
    let mut session = client.open(&address).await.unwrap();
    session.send(Vec::new()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"owner");

    client.shutdown().await;
    owner_client.shutdown().await;
    let _ = owner.shutdown().await;
    let _ = stale_target.shutdown().await;
}

#[tokio::test]
async fn repeated_not_owner_rejections_exhaust_the_attempt_limit() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let stale_target = start_cluster_inmem(
        core(State("stale-target")),
        store.clone(),
        "node-stale-limit",
        registry.clone(),
    )
    .await;
    let owner = start_cluster_inmem(
        core(State("owner")),
        store.clone(),
        "node-owner-limit",
        registry.clone(),
    )
    .await;
    let nodes = store.list_nodes().await.unwrap();
    let stale_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-stale-limit")
        .unwrap()
        .advertised_endpoint
        .clone();
    let owner_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-owner-limit")
        .unwrap()
        .advertised_endpoint
        .clone();
    let address = ActorAddress::new("probe", "not-owner-limit").unwrap();
    let owner_client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .placement_strategy(Arc::new(OrderedPlacement(vec![owner_endpoint])))
        .build()
        .unwrap();
    let _owner_session = owner_client.open(&address).await.unwrap();

    let client = Client::builder(StaleOwnerView::new(store.clone(), usize::MAX))
        .with_transport(Arc::new(InmemTransport::new(registry)))
        .placement_strategy(Arc::new(OrderedPlacement(vec![stale_endpoint])))
        .max_open_attempts(2)
        .build()
        .unwrap();
    let error = match client.open(&address).await {
        Ok(_) => panic!("repeated NotOwner responses should exhaust the attempt budget"),
        Err(error) => error,
    };
    assert_eq!(error, OpenError::AttemptsExhausted);

    client.shutdown().await;
    owner_client.shutdown().await;
    let _ = owner.shutdown().await;
    let _ = stale_target.shutdown().await;
}

#[tokio::test]
async fn client_retries_another_placement_candidate_after_capacity_rejection() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let full = start_cluster_inmem(
        core_with_limit(State("full"), 1),
        store.clone(),
        "node-full",
        registry.clone(),
    )
    .await;
    let filler = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let _busy = filler
        .open(&ActorAddress::new("probe", "busy-capacity").unwrap())
        .await
        .unwrap();
    let available = start_cluster_inmem(
        core(State("available")),
        store.clone(),
        "node-available",
        registry.clone(),
    )
    .await;
    let nodes = store.list_nodes().await.unwrap();
    let full_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-full")
        .unwrap()
        .advertised_endpoint
        .clone();
    let available_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-available")
        .unwrap()
        .advertised_endpoint
        .clone();
    let full_connections_before = registry.connection_ids(&full_endpoint).len();
    let available_connections_before = registry.connection_ids(&available_endpoint).len();
    let client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .placement_strategy(Arc::new(OrderedPlacement(vec![
            full_endpoint.clone(),
            available_endpoint.clone(),
        ])))
        .max_open_attempts(2)
        .build()
        .unwrap();
    let address = ActorAddress::new("probe", "capacity-retry").unwrap();
    let mut session = client.open(&address).await.unwrap();
    session.send(Vec::new()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"available");
    let owner = store.read_actor_owner(&address).await.unwrap().unwrap();
    assert_eq!(owner.record.owner.unwrap().node_id, "node-available");
    assert_eq!(
        registry.connection_ids(&full_endpoint).len(),
        full_connections_before + 1
    );
    assert_eq!(
        registry.connection_ids(&available_endpoint).len(),
        available_connections_before + 1
    );

    client.shutdown().await;
    let _ = available.shutdown().await;
    filler.shutdown().await;
    let _ = full.shutdown().await;
}

#[tokio::test]
async fn placement_attempt_limit_returns_attempts_exhausted() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let full = start_cluster_inmem(
        core_with_limit(State("full"), 1),
        store.clone(),
        "node-full-limit",
        registry.clone(),
    )
    .await;
    let filler = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let _busy = filler
        .open(&ActorAddress::new("probe", "busy-limit").unwrap())
        .await
        .unwrap();
    let endpoint = store
        .list_nodes()
        .await
        .unwrap()
        .into_iter()
        .find(|node| node.node_id == "node-full-limit")
        .unwrap()
        .advertised_endpoint;
    let client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry)))
        .placement_strategy(Arc::new(OrderedPlacement(vec![endpoint])))
        .max_open_attempts(1)
        .build()
        .unwrap();
    let error = match client
        .open(&ActorAddress::new("probe", "attempt-limit").unwrap())
        .await
    {
        Ok(_) => panic!("capacity rejection should exhaust the attempt budget"),
        Err(error) => error,
    };
    assert_eq!(error, OpenError::AttemptsExhausted);

    client.shutdown().await;
    filler.shutdown().await;
    let _ = full.shutdown().await;
}

#[tokio::test]
async fn same_node_rejection_falls_back_to_another_live_node() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let old = start_cluster_inmem(
        core(State("old")),
        store.clone(),
        "stable",
        registry.clone(),
    )
    .await;
    let old_client = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let target = ActorAddress::new("probe", "fallback").unwrap();
    let _old_session = old_client.open(&target).await.unwrap();

    store.expire_node("stable");
    let replacement = start_cluster_inmem(
        core_with_limit(State("replacement"), 1),
        store.clone(),
        "stable",
        registry.clone(),
    )
    .await;
    let replacement_client = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let busy = ActorAddress::new("probe", "busy").unwrap();
    let _busy_session = replacement_client.open(&busy).await.unwrap();

    let other = start_cluster_inmem(
        core(State("other")),
        store.clone(),
        "node-a",
        registry.clone(),
    )
    .await;
    let fallback = start_cluster_inmem(
        core(State("fallback")),
        store.clone(),
        "node-b",
        registry.clone(),
    )
    .await;
    let client = Client::with_transport(Arc::new(InmemTransport::new(registry)), store.clone());
    let mut session = client.open(&target).await.unwrap();
    session.send(Vec::new()).await.unwrap();
    let observed = session.recv().await.unwrap().unwrap();
    assert!(observed == b"other" || observed == b"fallback");
    let owner = store.read_actor_owner(&target).await.unwrap().unwrap();
    assert_ne!(owner.record.owner.unwrap().node_id, "stable");

    client.shutdown().await;
    let _ = fallback.shutdown().await;
    let _ = other.shutdown().await;
    replacement_client.shutdown().await;
    let _ = replacement.shutdown().await;
    old_client.shutdown().await;
    let _ = old.shutdown().await;
}

#[tokio::test]
async fn replacement_session_lazily_takes_over_same_node_with_higher_epoch() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let first_server = start_cluster_inmem(
        core(State("first")),
        store.clone(),
        "stable-node",
        registry.clone(),
    )
    .await;
    let first_client = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let address = ActorAddress::new("probe", "same-node").unwrap();
    let mut first_session = first_client.open(&address).await.unwrap();
    first_session.send(Vec::new()).await.unwrap();
    assert_eq!(first_session.recv().await.unwrap().unwrap(), b"first");
    let first_owner = store.read_actor_owner(&address).await.unwrap().unwrap();

    store.expire_node("stable-node");
    let replacement = start_cluster_inmem(
        core(State("replacement")),
        store.clone(),
        "stable-node",
        registry.clone(),
    )
    .await;
    let replacement_client =
        Client::with_transport(Arc::new(InmemTransport::new(registry)), store.clone());
    let mut replacement_session = replacement_client.open(&address).await.unwrap();
    replacement_session.send(Vec::new()).await.unwrap();
    assert_eq!(
        replacement_session.recv().await.unwrap().unwrap(),
        b"replacement"
    );
    let next_owner = store.read_actor_owner(&address).await.unwrap().unwrap();
    assert_eq!(
        next_owner.record.owner.as_ref().unwrap().node_id,
        "stable-node"
    );
    assert_ne!(
        next_owner.record.owner.as_ref().unwrap().session_id,
        first_owner.record.owner.as_ref().unwrap().session_id
    );
    assert!(next_owner.record.ownership_epoch > first_owner.record.ownership_epoch);

    replacement_client.shutdown().await;
    let _ = replacement.shutdown().await;
    first_client.shutdown().await;
    let _ = first_server.shutdown().await;
}

#[tokio::test]
async fn client_connects_directly_to_the_live_owner_without_server_forwarding() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let owner_server = start_cluster_inmem(
        core(State("owner")),
        store.clone(),
        "node-owner",
        registry.clone(),
    )
    .await;
    let other_server = start_cluster_inmem(
        core(State("other")),
        store.clone(),
        "node-other",
        registry.clone(),
    )
    .await;
    let nodes = store.list_nodes().await.unwrap();
    let owner_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-owner")
        .unwrap()
        .advertised_endpoint
        .clone();
    let other_endpoint = nodes
        .iter()
        .find(|node| node.node_id == "node-other")
        .unwrap()
        .advertised_endpoint
        .clone();
    let placer = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .placement_strategy(Arc::new(OrderedPlacement(vec![owner_endpoint.clone()])))
        .build()
        .unwrap();
    let address = ActorAddress::new("probe", "direct-owner").unwrap();
    let _first = placer.open(&address).await.unwrap();
    let owner_connections_before = registry.connection_ids(&owner_endpoint).len();
    let other_connections_before = registry.connection_ids(&other_endpoint).len();

    let client = Client::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        store.clone(),
    );
    let mut session = client.open(&address).await.unwrap();
    session.send(Vec::new()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"owner");
    assert_eq!(
        registry.connection_ids(&owner_endpoint).len(),
        owner_connections_before + 1
    );
    assert_eq!(
        registry.connection_ids(&other_endpoint).len(),
        other_connections_before
    );

    client.shutdown().await;
    placer.shutdown().await;
    let _ = other_server.shutdown().await;
    let _ = owner_server.shutdown().await;
}
