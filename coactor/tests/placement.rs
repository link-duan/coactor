use std::sync::Arc;

use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::{
    Actor, ActorAddress, ActorOwnerStore, ActorRuntime, Client, MessageContext, ServerBuilderCore,
    actor,
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

    let gateway = start_cluster_inmem(
        core(State("gateway")),
        store.clone(),
        "gateway-a",
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
    assert_eq!(session.recv().await.unwrap().unwrap(), b"fallback");
    let owner = store.read_actor_owner(&target).await.unwrap().unwrap();
    assert_eq!(owner.record.owner.unwrap().node_id, "node-b");

    client.shutdown().await;
    fallback.shutdown().await;
    gateway.shutdown().await;
    replacement_client.shutdown().await;
    replacement.shutdown().await;
    old_client.shutdown().await;
    old.shutdown().await;
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
    replacement.shutdown().await;
    first_client.shutdown().await;
    first_server.shutdown().await;
}
