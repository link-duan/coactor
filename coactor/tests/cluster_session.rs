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
    Actor, ActorAddress, ActorRuntime, Client, ClientBuildError, CoordinationError, MessageContext,
    NodeDirectory, NodeRecord, OpenError, ServerBuilderCore, actor,
    test_support::{TestCoordinationStore, start_cluster_inmem},
};

#[derive(Clone)]
struct AppState(&'static str);

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

#[tokio::test]
async fn clients_open_directly_through_the_node_directory() {
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

#[tokio::test]
async fn client_open_evicts_an_unreachable_gateway_and_retries_another() {
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
    let mut ghost = live.clone();
    ghost.node_id = "ghost".to_owned();
    ghost.advertised_endpoint = "node-000-ghost".to_owned();
    let directory = Arc::new(StaticDirectory(vec![ghost, live]));
    let client = Client::with_transport(Arc::new(InmemTransport::new(registry)), directory);
    let address = ActorAddress::new("cluster-echo", "gateway-retry").unwrap();
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

#[tokio::test]
async fn client_build_is_synchronous_and_performs_no_directory_io() {
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
    let address = ActorAddress::new("cluster-echo", "unavailable").unwrap();
    let error = match client.open(&address).await {
        Ok(_) => panic!("directory failure should prevent opening"),
        Err(error) => error,
    };
    assert_eq!(error, OpenError::DirectoryUnavailable);
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    client.shutdown().await;
}
