use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::client::{Client, ClientBuilder};
use crate::test_support::{TestCoordinationStore, start_cluster_inmem};
use crate::transport::Endpoint;
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::{
    Actor, ActorAddress, ActorId, ActorOwnerStore, ActorRuntime, CoordinationError, MessageContext,
    NodeDirectory, NodeRecord, NodeSessionId, PlacementCtx, PlacementStrategy, SendError, Server,
    ServerBuilder, actor,
};

#[derive(Default, Clone)]
struct AppState;

#[actor]
struct EchoActor {
    _runtime: ActorRuntime<AppState>,
}

impl Actor<AppState> for EchoActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { _runtime: runtime }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let _ = ctx.send(msg.to_vec()).await;
    }

    async fn on_session_opened(&mut self, ctx: &MessageContext) {
        let _ = ctx.send(b"opened".to_vec()).await;
    }
}

async fn inmem_server(
    storage: Arc<TestCoordinationStore>,
    node_id: &str,
    registry: Arc<InmemRegistry>,
) -> Server<AppState> {
    start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("cluster-echo"),
        storage,
        node_id,
        registry,
    )
    .await
}

fn inmem_client(registry: Arc<InmemRegistry>, storage: Arc<TestCoordinationStore>) -> Client {
    ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), storage).start()
}

#[tokio::test]
async fn sessions_are_location_transparent_across_gateways() {
    let storage = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server_a = inmem_server(storage.clone(), "A", registry.clone()).await;
    let client_a = inmem_client(registry.clone(), storage.clone());

    let mut session_a = client_a
        .actor("cluster-echo", ActorId::from("remote-1"))
        .open()
        .await
        .expect("open via Node Directory");
    assert_eq!(session_a.recv().await, Some(Ok(b"opened".to_vec())));
    session_a.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session_a.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    let server_b = inmem_server(storage.clone(), "B", registry.clone()).await;
    let client_b = inmem_client(registry.clone(), storage.clone());
    let mut session_b = client_b
        .actor("cluster-echo", ActorId::from("remote-1"))
        .open()
        .await
        .expect("open through another Gateway");
    assert_eq!(session_b.recv().await, Some(Ok(b"opened".to_vec())));
    session_b.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session_b.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    client_b.shutdown().await;
    server_b.shutdown().await;
    client_a.shutdown().await;
    server_a.shutdown().await;
}

#[tokio::test]
async fn client_pool_distributes_sessions_across_gateways() {
    let storage = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server_a = inmem_server(storage.clone(), "A", registry.clone()).await;
    let server_b = inmem_server(storage.clone(), "B", registry.clone()).await;
    let client = ClientBuilder::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        storage.clone(),
    )
    .start();

    let mut first = client
        .actor("cluster-echo", ActorId::from("pool-1"))
        .open()
        .await
        .expect("first session");
    let mut second = client
        .actor("cluster-echo", ActorId::from("pool-2"))
        .open()
        .await
        .expect("second session");
    assert_eq!(first.recv().await, Some(Ok(b"opened".to_vec())));
    assert_eq!(second.recv().await, Some(Ok(b"opened".to_vec())));

    let record_a = storage
        .read_actor_owner(&ActorAddress::new("cluster-echo", ActorId::from("pool-1")))
        .await
        .unwrap()
        .unwrap()
        .record;
    let record_b = storage
        .read_actor_owner(&ActorAddress::new("cluster-echo", ActorId::from("pool-2")))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_ne!(
        record_a.owner.as_ref().unwrap().node_id,
        record_b.owner.as_ref().unwrap().node_id
    );

    client.shutdown().await;
    server_b.shutdown().await;
    server_a.shutdown().await;
}

#[tokio::test]
async fn failover_breaks_sessions_and_requires_reopen() {
    let storage = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server_a = inmem_server(storage.clone(), "A", registry.clone()).await;
    let client_a = inmem_client(registry.clone(), storage.clone());
    let mut session = client_a
        .actor("cluster-echo", ActorId::from("failover-1"))
        .open()
        .await
        .expect("initial open");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));

    server_a.shutdown().await;

    let server_b = inmem_server(storage.clone(), "B", registry.clone()).await;
    let client_b = inmem_client(registry.clone(), storage.clone());
    let mut fresh = client_b
        .actor("cluster-echo", ActorId::from("failover-1"))
        .open()
        .await
        .expect("reopen after failover");
    assert_eq!(fresh.recv().await, Some(Ok(b"opened".to_vec())));
    fresh
        .send(b"after-failover".to_vec())
        .await
        .expect("accepted");
    assert_eq!(fresh.recv().await, Some(Ok(b"after-failover".to_vec())));

    let send_result =
        tokio::time::timeout(Duration::from_secs(5), session.send(b"stale".to_vec())).await;
    match send_result {
        Ok(Err(_)) => {}
        Ok(Ok(())) => panic!("stale session send unexpectedly succeeded"),
        Err(_) => panic!("stale session send hung"),
    }

    client_b.shutdown().await;
    server_b.shutdown().await;
    client_a.shutdown().await;
}

struct FailingDirectory;

#[async_trait]
impl NodeDirectory for FailingDirectory {
    async fn read_node(
        &self,
        _session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, CoordinationError> {
        Err(CoordinationError::Unavailable)
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        Err(CoordinationError::Unavailable)
    }
}

struct CountingDirectory {
    inner: Arc<TestCoordinationStore>,
    lists: AtomicUsize,
}

#[async_trait]
impl NodeDirectory for CountingDirectory {
    async fn read_node(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, CoordinationError> {
        self.inner.read_node(session_id).await
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        self.lists.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(20)).await;
        self.inner.list_nodes().await
    }
}

struct MutableDirectory {
    nodes: Mutex<Vec<NodeRecord>>,
    unavailable: AtomicBool,
}

impl MutableDirectory {
    fn set_nodes(&self, nodes: Vec<NodeRecord>) {
        *self.nodes.lock() = nodes;
    }
}

#[async_trait]
impl NodeDirectory for MutableDirectory {
    async fn read_node(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, CoordinationError> {
        Ok(self
            .nodes
            .lock()
            .iter()
            .find(|node| node.session_id == *session_id)
            .cloned())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(CoordinationError::Unavailable);
        }
        Ok(self.nodes.lock().clone())
    }
}

#[derive(Clone)]
struct GatewayState(&'static str);

#[actor]
struct GatewayProbeActor {
    runtime: ActorRuntime<GatewayState>,
}

impl Actor<GatewayState> for GatewayProbeActor {
    fn new(runtime: ActorRuntime<GatewayState>) -> Self {
        Self { runtime }
    }

    async fn on_message(&mut self, _ctx: &MessageContext, _msg: &[u8]) {}

    async fn on_session_opened(&mut self, ctx: &MessageContext) {
        let _ = ctx
            .send(self.runtime.app_state().0.as_bytes().to_vec())
            .await;
    }
}

struct LocalPlacement;

#[async_trait]
impl PlacementStrategy for LocalPlacement {
    async fn candidates(&self, _address: &ActorAddress, _ctx: &PlacementCtx) -> Vec<Endpoint> {
        Vec::new()
    }
}

async fn gateway_probe_server(
    store: Arc<TestCoordinationStore>,
    node_id: &'static str,
    registry: Arc<InmemRegistry>,
) -> Server<GatewayState> {
    start_cluster_inmem(
        ServerBuilder::local(GatewayState(node_id))
            .with_placement_strategy(Arc::new(LocalPlacement))
            .register::<GatewayProbeActor>("gateway-probe"),
        store,
        node_id,
        registry,
    )
    .await
}

async fn gateway_probe(client: &Client, actor_id: &str) -> Vec<u8> {
    let mut session = client
        .actor("gateway-probe", ActorId::from(actor_id))
        .open()
        .await
        .expect("Gateway probe opens");
    session.recv().await.unwrap().unwrap()
}

#[tokio::test]
async fn empty_node_directory_is_reported_separately() {
    let registry = InmemRegistry::new();
    let directory = Arc::new(TestCoordinationStore::default());
    let client =
        ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), directory).start();

    let error = match client
        .actor("cluster-echo", ActorId::from("empty-directory"))
        .open()
        .await
    {
        Ok(_) => panic!("empty Node Directory must fail"),
        Err(error) => error,
    };
    assert_eq!(error, SendError::NoAvailableGateway);
    client.shutdown().await;
}

#[tokio::test]
async fn unavailable_node_directory_is_reported_separately() {
    let registry = InmemRegistry::new();
    let client = ClientBuilder::with_transport(
        Arc::new(InmemTransport::new(registry)),
        Arc::new(FailingDirectory),
    )
    .start();

    let error = match client
        .actor("cluster-echo", ActorId::from("directory-failure"))
        .open()
        .await
    {
        Ok(_) => panic!("unavailable Node Directory must fail"),
        Err(error) => error,
    };
    assert_eq!(error, SendError::DirectoryUnavailable);
    client.shutdown().await;
}

#[tokio::test]
async fn concurrent_opens_share_one_node_directory_refresh() {
    let storage = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server = inmem_server(storage.clone(), "A", registry.clone()).await;
    let directory = Arc::new(CountingDirectory {
        inner: storage,
        lists: AtomicUsize::new(0),
    });
    let client =
        ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), directory.clone())
            .directory_refresh_interval(Duration::from_secs(30))
            .start();

    let first = client.actor("cluster-echo", ActorId::from("singleflight-1"));
    let second = client.actor("cluster-echo", ActorId::from("singleflight-2"));
    let (first, second) = tokio::join!(first.open(), second.open());
    assert!(first.is_ok());
    assert!(second.is_ok());
    assert_eq!(directory.lists.load(Ordering::SeqCst), 1);

    client.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn due_refresh_replaces_a_non_empty_gateway_pool() {
    let store = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server_a = gateway_probe_server(store.clone(), "A", registry.clone()).await;
    let server_b = gateway_probe_server(store.clone(), "B", registry.clone()).await;
    let nodes = store.list_nodes().await.unwrap();
    let node_a = nodes
        .iter()
        .find(|node| node.node_id == "A")
        .unwrap()
        .clone();
    let node_b = nodes
        .iter()
        .find(|node| node.node_id == "B")
        .unwrap()
        .clone();
    let directory = Arc::new(MutableDirectory {
        nodes: Mutex::new(vec![node_a]),
        unavailable: AtomicBool::new(false),
    });
    let client =
        ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), directory.clone())
            .directory_refresh_interval(Duration::ZERO)
            .start();

    assert_eq!(gateway_probe(&client, "refresh-a").await, b"A");
    directory.set_nodes(vec![node_b]);
    assert_eq!(gateway_probe(&client, "refresh-b").await, b"B");

    client.shutdown().await;
    server_b.shutdown().await;
    server_a.shutdown().await;
}

#[tokio::test]
async fn gateway_filter_skips_draining_but_keeps_pressured_nodes() {
    let store = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server_a = gateway_probe_server(store.clone(), "A", registry.clone()).await;
    let server_b = gateway_probe_server(store.clone(), "B", registry.clone()).await;
    let nodes = store.list_nodes().await.unwrap();
    let mut node_a = nodes
        .iter()
        .find(|node| node.node_id == "A")
        .unwrap()
        .clone();
    let mut node_b = nodes
        .iter()
        .find(|node| node.node_id == "B")
        .unwrap()
        .clone();
    node_a.draining = true;
    node_b.pressured = true;
    node_b.active_actor_count = node_b.max_actor_count;
    let directory = Arc::new(MutableDirectory {
        nodes: Mutex::new(vec![node_a, node_b]),
        unavailable: AtomicBool::new(false),
    });
    let client =
        ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), directory).start();

    assert_eq!(gateway_probe(&client, "gateway-filter").await, b"B");

    client.shutdown().await;
    server_b.shutdown().await;
    server_a.shutdown().await;
}

#[tokio::test]
async fn failed_refresh_preserves_a_reachable_gateway_pool() {
    let store = Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let server = gateway_probe_server(store.clone(), "A", registry.clone()).await;
    let directory = Arc::new(MutableDirectory {
        nodes: Mutex::new(store.list_nodes().await.unwrap()),
        unavailable: AtomicBool::new(false),
    });
    let client =
        ClientBuilder::with_transport(Arc::new(InmemTransport::new(registry)), directory.clone())
            .directory_refresh_interval(Duration::ZERO)
            .start();

    assert_eq!(gateway_probe(&client, "cached-a").await, b"A");
    directory.unavailable.store(true, Ordering::SeqCst);
    assert_eq!(gateway_probe(&client, "cached-b").await, b"A");

    client.shutdown().await;
    server.shutdown().await;
}
