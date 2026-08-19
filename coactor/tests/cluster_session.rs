use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;

use crate::coordination::CoordinationErrorKind;
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::{
    Actor, ActorAddress, ActorOwner, ActorOwnerReader, ActorOwnerRecord, ActorRuntime, Client,
    ClientBuildError, CoordinationError, MessageContext, NodeDirectory, NodeRecord, NodeSessionId,
    OpenError, PlacementContext, PlacementStrategy, Revision, ServerBuilderCore,
    VersionedActorOwnerRecord, actor,
    test_support::{TestCoordinationStore, start_cluster_inmem},
};

#[derive(Clone)]
struct AppState(&'static str);

#[derive(Clone)]
struct CloseState {
    closed: Arc<AtomicUsize>,
}

#[actor]
struct CloseCountingActor {
    runtime: ActorRuntime<CloseState>,
}
impl Actor<CloseState> for CloseCountingActor {
    fn new(runtime: ActorRuntime<CloseState>) -> Self {
        Self { runtime }
    }

    async fn on_message(&mut self, _: &MessageContext, _: &[u8]) {}

    async fn on_session_closed(&mut self, _: &MessageContext) {
        self.runtime.state().closed.fetch_add(1, Ordering::Relaxed);
    }
}

#[actor]
struct EchoActor {
    runtime: ActorRuntime<AppState>,
}
impl Actor<AppState> for EchoActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime }
    }
    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let mut event = self.runtime.state().0.as_bytes().to_vec();
        event.extend_from_slice(msg);
        ctx.send(event).await.unwrap();
    }
}

fn server_core(state: AppState) -> ServerBuilderCore<AppState> {
    let mut core = ServerBuilderCore::base(Some(state));
    core.add_actor::<EchoActor>(crate::ActorConfig::new("cluster-echo"));
    core
}

fn close_server_core(state: CloseState) -> ServerBuilderCore<CloseState> {
    let mut core = ServerBuilderCore::base(Some(state));
    core.add_actor::<CloseCountingActor>(crate::ActorConfig::new("close-counting"));
    core
}

#[tokio::test]
async fn clients_open_direct_sessions_to_servers() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let server = start_cluster_inmem(
        server_core(AppState("a:")),
        store.clone(),
        "node-a",
        registry.clone(),
    )
    .await;
    let client = Client::with_transport(Arc::new(InmemTransport::new(registry)), store);
    let address = ActorAddress::new("cluster-echo", "remote-one").unwrap();

    let mut first = client.open(&address).await.unwrap();
    first.send(b"hello".to_vec()).await.unwrap();
    assert_eq!(first.recv().await.unwrap().unwrap(), b"a:hello");

    let mut second = client.open(&address).await.unwrap();
    second.send(b"again".to_vec()).await.unwrap();
    assert_eq!(second.recv().await.unwrap().unwrap(), b"a:again");

    client.shutdown().await;
    let _ = server.shutdown().await;
}

struct StaticDirectory(Vec<NodeRecord>);
#[async_trait]
impl NodeDirectory for StaticDirectory {
    async fn read_node(&self, node_id: &str) -> Result<Option<NodeRecord>, CoordinationError> {
        Ok(self.0.iter().find(|node| node.node_id == node_id).cloned())
    }
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl ActorOwnerReader for StaticDirectory {
    async fn read_actor_owner(
        &self,
        _address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        Ok(None)
    }
}

struct FixedLiveOwnerDirectory {
    nodes: Vec<NodeRecord>,
    owner: VersionedActorOwnerRecord,
}

struct FixedPlacement(Vec<String>);

impl PlacementStrategy for FixedPlacement {
    fn candidates(&self, _: &ActorAddress, ctx: &PlacementContext) -> Vec<String> {
        self.0
            .iter()
            .filter(|endpoint| {
                ctx.candidates
                    .iter()
                    .any(|candidate| candidate.endpoint == **endpoint)
            })
            .cloned()
            .collect()
    }
}

#[async_trait]
impl NodeDirectory for FixedLiveOwnerDirectory {
    async fn read_node(&self, node_id: &str) -> Result<Option<NodeRecord>, CoordinationError> {
        Ok(self
            .nodes
            .iter()
            .find(|node| node.node_id == node_id)
            .cloned())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        Ok(self.nodes.clone())
    }
}

#[async_trait]
impl ActorOwnerReader for FixedLiveOwnerDirectory {
    async fn read_actor_owner(
        &self,
        _address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        Ok(Some(self.owner.clone()))
    }
}

#[tokio::test]
async fn unchanged_live_owner_transport_connection_failure_does_not_trigger_placement() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let candidate = start_cluster_inmem(
        server_core(AppState("candidate:")),
        store.clone(),
        "node-candidate",
        registry.clone(),
    )
    .await;
    let candidate_node = store.list_nodes().await.unwrap().pop().unwrap();
    let candidate_connections = registry
        .connection_ids(&candidate_node.advertised_endpoint)
        .len();
    let owner_session = NodeSessionId::generate();
    let owner_node = NodeRecord {
        node_id: "node-unreachable-owner".to_owned(),
        session_id: owner_session.clone(),
        advertised_endpoint: "node-unreachable-owner-endpoint".to_owned(),
        protocol_version: crate::TRANSPORT_PROTOCOL_VERSION,
        lease_generation: 1,
        sampled_at_unix_ms: 0,
        active_actor_count: 0,
        max_actor_count: 10,
        pressured: false,
        draining: false,
    };
    let candidate_endpoint = candidate_node.advertised_endpoint.clone();
    let coordination = FixedLiveOwnerDirectory {
        nodes: vec![owner_node, candidate_node],
        owner: VersionedActorOwnerRecord {
            record: ActorOwnerRecord {
                owner: Some(ActorOwner {
                    node_id: "node-unreachable-owner".to_owned(),
                    session_id: owner_session,
                }),
                ownership_epoch: 1,
            },
            revision: Revision::new("owner-revision"),
        },
    };
    let client = Client::builder(coordination)
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .placement_strategy(Arc::new(FixedPlacement(vec![candidate_endpoint.clone()])))
        .max_open_attempts(1)
        .build()
        .unwrap();
    let address = ActorAddress::new("cluster-echo", "unchanged-owner").unwrap();
    let error = match client.open(&address).await {
        Ok(_) => panic!("an unchanged live Owner must not be bypassed by Placement"),
        Err(error) => error,
    };
    assert_eq!(error, OpenError::RemoteUnavailable);
    assert_eq!(
        registry.connection_ids(&candidate_endpoint).len(),
        candidate_connections,
        "Client must not connect to another Placement candidate"
    );

    client.shutdown().await;
    let _ = candidate.shutdown().await;
}

#[tokio::test]
async fn client_open_excludes_an_unreachable_candidate_and_retries_another() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let server = start_cluster_inmem(
        server_core(AppState("live:")),
        store.clone(),
        "node-a",
        registry.clone(),
    )
    .await;
    let live = store.list_nodes().await.unwrap().pop().unwrap();
    let live_endpoint = live.advertised_endpoint.clone();
    let mut ghost = live.clone();
    ghost.node_id = "ghost".to_owned();
    ghost.advertised_endpoint = "node-000-ghost".to_owned();
    let ghost_endpoint = ghost.advertised_endpoint.clone();
    let directory = StaticDirectory(vec![ghost, live]);
    let client = Client::builder(directory)
        .with_transport(Arc::new(InmemTransport::new(registry)))
        .placement_strategy(Arc::new(FixedPlacement(vec![
            ghost_endpoint,
            live_endpoint,
        ])))
        .max_open_attempts(1)
        .build()
        .unwrap();
    let address = ActorAddress::new("cluster-echo", "candidate-retry").unwrap();
    let mut session = client.open(&address).await.unwrap();
    session.send(b"ok".to_vec()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"live:ok");
    client.shutdown().await;
    let _ = server.shutdown().await;
}

struct CountingDirectory(Arc<AtomicUsize>);
#[async_trait]
impl NodeDirectory for CountingDirectory {
    async fn read_node(&self, _node_id: &str) -> Result<Option<NodeRecord>, CoordinationError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(None)
    }
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(CoordinationError::from_kind(
            CoordinationErrorKind::Unavailable,
        ))
    }
}

#[async_trait]
impl ActorOwnerReader for CountingDirectory {
    async fn read_actor_owner(
        &self,
        _address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(CoordinationError::from_kind(
            CoordinationErrorKind::Unavailable,
        ))
    }
}

#[tokio::test]
async fn client_build_is_synchronous_and_performs_no_coordination_io() {
    let calls = Arc::new(AtomicUsize::new(0));
    let client = Client::builder(CountingDirectory(calls.clone()))
        .build()
        .unwrap();
    assert_eq!(calls.load(Ordering::Relaxed), 0);
    assert!(matches!(
        Client::builder(CountingDirectory(calls.clone()))
            .open_timeout(Duration::ZERO)
            .build(),
        Err(ClientBuildError::InvalidOpenTimeout)
    ));
    assert!(matches!(
        Client::builder(CountingDirectory(calls.clone()))
            .transport_connect_timeout(Duration::ZERO)
            .build(),
        Err(ClientBuildError::InvalidTransportConnectTimeout)
    ));
    let address = ActorAddress::new("cluster-echo", "unavailable").unwrap();
    let error = match client.open(&address).await {
        Ok(_) => panic!("ownership failure should prevent opening"),
        Err(error) => error,
    };
    assert_eq!(error, OpenError::OwnershipUnavailable);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    client.shutdown().await;
}

#[tokio::test]
async fn client_uses_a_bounded_transport_connection_pool_and_keeps_sessions_affine() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let server = start_cluster_inmem(
        server_core(AppState("pool:")),
        store.clone(),
        "node-pool",
        registry.clone(),
    )
    .await;
    let endpoint = store
        .list_nodes()
        .await
        .unwrap()
        .pop()
        .unwrap()
        .advertised_endpoint;
    let client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .build()
        .unwrap();
    let mut sessions = Vec::new();
    for index in 0..6 {
        let address = ActorAddress::new("cluster-echo", format!("pool-{index}")).unwrap();
        sessions.push(client.open(&address).await.unwrap());
    }

    assert_eq!(registry.connection_ids(&endpoint).len(), 4);
    assert_eq!(
        registry.connection_session_counts(&endpoint),
        vec![1, 1, 2, 2]
    );
    let session_ids = sessions
        .iter()
        .map(|session| session.session_id())
        .collect::<Vec<_>>();
    for session in &sessions {
        session.send(Vec::new()).await.unwrap();
        assert_eq!(
            registry.session_connection_ids(session.session_id()).len(),
            1,
            "one Session must never span Transport Connections"
        );
    }
    for session in &mut sessions {
        assert_eq!(session.recv().await.unwrap().unwrap(), b"pool:");
    }
    drop(sessions);
    for session_id in session_ids {
        assert_eq!(
            registry.session_connection_ids(session_id).len(),
            1,
            "SessionClose must use the Transport Connection bound at open"
        );
    }

    client.shutdown().await;
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn client_shutdown_closes_transport_connections_while_session_handles_remain() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let closed = Arc::new(AtomicUsize::new(0));
    let server = start_cluster_inmem(
        close_server_core(CloseState {
            closed: closed.clone(),
        }),
        store.clone(),
        "node-shutdown",
        registry.clone(),
    )
    .await;
    let endpoint = store
        .list_nodes()
        .await
        .unwrap()
        .pop()
        .unwrap()
        .advertised_endpoint;
    let client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .max_connections_per_endpoint(2)
        .build()
        .unwrap();
    let first = client
        .open(&ActorAddress::new("close-counting", "first").unwrap())
        .await
        .unwrap();
    let second = client
        .open(&ActorAddress::new("close-counting", "second").unwrap())
        .await
        .unwrap();
    assert_eq!(registry.connection_ids(&endpoint).len(), 2);

    tokio::time::timeout(Duration::from_secs(1), client.shutdown())
        .await
        .expect("Client shutdown must not wait for Session handles to drop");
    tokio::time::timeout(Duration::from_secs(1), async {
        while closed.load(Ordering::Relaxed) < 2 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("Server must observe both Transport Connections closing");
    assert!(registry.connection_ids(&endpoint).is_empty());

    drop((first, second));
    let _ = server.shutdown().await;
}

#[tokio::test]
async fn one_transport_connection_failure_only_terminates_its_bound_sessions() {
    let registry = InmemRegistry::new();
    let store = Arc::new(TestCoordinationStore::default());
    let server = start_cluster_inmem(
        server_core(AppState("isolated:")),
        store.clone(),
        "node-isolation",
        registry.clone(),
    )
    .await;
    let client = Client::builder(store.as_ref().clone())
        .with_transport(Arc::new(InmemTransport::new(registry.clone())))
        .max_connections_per_endpoint(2)
        .build()
        .unwrap();
    let first_address = ActorAddress::new("cluster-echo", "isolated-a").unwrap();
    let second_address = ActorAddress::new("cluster-echo", "isolated-b").unwrap();
    let mut first = client.open(&first_address).await.unwrap();
    let mut second = client.open(&second_address).await.unwrap();
    let first_connection = registry.session_connection_ids(first.session_id())[0];
    let second_connection = registry.session_connection_ids(second.session_id())[0];
    assert_ne!(first_connection, second_connection);

    registry.close_connection(first_connection);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), first.recv())
            .await
            .unwrap()
            .unwrap(),
        Err(crate::SendError::RemoteUnavailable)
    );
    second.send(Vec::new()).await.unwrap();
    assert_eq!(second.recv().await.unwrap().unwrap(), b"isolated:");

    client.shutdown().await;
    let _ = server.shutdown().await;
}

#[test]
fn client_rejects_zero_attempt_and_transport_connection_pool_limits() {
    assert!(matches!(
        Client::builder(CountingDirectory(Arc::new(AtomicUsize::new(0))))
            .max_open_attempts(0)
            .build(),
        Err(ClientBuildError::InvalidMaxOpenAttempts)
    ));
    assert!(matches!(
        Client::builder(CountingDirectory(Arc::new(AtomicUsize::new(0))))
            .max_connections_per_endpoint(0)
            .build(),
        Err(ClientBuildError::InvalidMaxConnectionsPerEndpoint)
    ));
}
