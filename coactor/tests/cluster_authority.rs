use std::time::Duration;

use crate::transport::inmem::InmemRegistry;
use crate::{
    Actor, ActorAddress, ActorRuntime, MessageContext, MutationOutcome, NodeDirectory,
    NodeLeaseStore, NodeRecord, NodeSessionId, Revision, Server, ServerBuilderCore, ServerFailure,
    ServerStartError, actor,
    test_support::{TestCoordinationStore, start_cluster_inmem},
};

#[actor]
struct Probe;
impl Actor<()> for Probe {
    fn new(_: ActorRuntime<()>) -> Self {
        Self
    }
    async fn on_message(&mut self, _: &MessageContext, _: &[u8]) {}
}

#[derive(Clone)]
struct SharedState;

#[actor]
struct StatefulProbe;
impl Actor<SharedState> for StatefulProbe {
    fn new(_: ActorRuntime<SharedState>) -> Self {
        Self
    }
    async fn on_message(&mut self, _: &MessageContext, _: &[u8]) {}
}

#[test]
fn production_builder_infers_state_in_both_orders() {
    let _ = Server::builder(TestCoordinationStore::default())
        .with_state(SharedState)
        .actor::<StatefulProbe>("stateful");
    let _ = Server::builder(TestCoordinationStore::default())
        .actor::<StatefulProbe>("stateful")
        .with_state(SharedState);
}

#[tokio::test]
async fn server_builder_validates_named_network_configuration_at_start() {
    let error = match Server::builder(TestCoordinationStore::default())
        .actor::<Probe>("probe")
        .start()
        .await
    {
        Ok(_) => panic!("missing bind address should fail"),
        Err(error) => error,
    };
    assert!(matches!(error, ServerStartError::MissingBindAddress));

    let bind = "127.0.0.1:0".parse().unwrap();
    let error = match Server::builder(TestCoordinationStore::default())
        .bind(bind)
        .actor::<Probe>("probe")
        .start()
        .await
    {
        Ok(_) => panic!("missing endpoint should fail"),
        Err(error) => error,
    };
    assert!(matches!(error, ServerStartError::MissingAdvertisedEndpoint));

    let error = match Server::builder(TestCoordinationStore::default())
        .bind(bind)
        .advertised_endpoint("http://example.com:7000")
        .actor::<Probe>("probe")
        .start()
        .await
    {
        Ok(_) => panic!("scheme should fail endpoint validation"),
        Err(error) => error,
    };
    assert!(matches!(error, ServerStartError::InvalidAdvertisedEndpoint));

    let error = match Server::builder(TestCoordinationStore::default())
        .bind(bind)
        .advertised_endpoint("example.com:7000")
        .node_id("Bad_Node")
        .actor::<Probe>("probe")
        .start()
        .await
    {
        Ok(_) => panic!("invalid Node ID should fail"),
        Err(error) => error,
    };
    assert!(matches!(error, ServerStartError::InvalidNodeId));

    assert_eq!(
        crate::cluster::canonical_endpoint("EXAMPLE.com:7000"),
        Some("example.com:7000".to_owned())
    );
    assert_eq!(
        crate::cluster::canonical_endpoint("[2001:0db8::1]:7000"),
        Some("[2001:db8::1]:7000".to_owned())
    );
    assert_eq!(
        crate::cluster::canonical_endpoint("http://example.com:7000"),
        None
    );
}
fn node(node_id: &str) -> NodeRecord {
    NodeRecord {
        node_id: node_id.to_owned(),
        session_id: NodeSessionId::generate(),
        advertised_endpoint: "127.0.0.1:7000".to_owned(),
        protocol_version: crate::PEER_PROTOCOL_VERSION,
        lease_generation: 0,
        sampled_at_unix_ms: crate::cluster::wall_time_millis(),
        active_actor_count: 0,
        max_actor_count: 10,
        pressured: false,
        draining: false,
    }
}

#[tokio::test(start_paused = true)]
async fn server_wait_reports_self_fencing() {
    let store = std::sync::Arc::new(TestCoordinationStore::default());
    let registry = InmemRegistry::new();
    let mut core = ServerBuilderCore::base(Some(()));
    core.add_actor::<Probe>(crate::ActorConfig::new("probe"));
    let server = start_cluster_inmem(core, store.clone(), "node-a", registry).await;
    tokio::task::yield_now().await;
    store.remove_node("node-a");
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;
    assert_eq!(server.wait().await, Err(ServerFailure::Fenced));
    server.shutdown().await;
}

#[tokio::test]
async fn node_lease_is_keyed_by_stable_node_id_and_fenced_by_revision() {
    let store = TestCoordinationStore::default();
    let first = node("node-a");
    let revision = match store
        .acquire_node(first.clone(), Duration::from_secs(10))
        .await
        .unwrap()
    {
        MutationOutcome::Applied(revision) => revision,
        _ => panic!("lease should be acquired"),
    };
    let replacement = node("node-a");
    assert!(matches!(
        store
            .acquire_node(replacement, Duration::from_secs(10))
            .await
            .unwrap(),
        MutationOutcome::Conflict
    ));
    assert_eq!(
        store.read_node("node-a").await.unwrap(),
        Some(first.clone())
    );
    assert!(matches!(
        store
            .release_node("node-a", &Revision::new("stale"))
            .await
            .unwrap(),
        MutationOutcome::Conflict
    ));
    assert!(matches!(
        store.release_node("node-a", &revision).await.unwrap(),
        MutationOutcome::Applied(())
    ));
    assert_eq!(store.read_node("node-a").await.unwrap(), None);
}

#[test]
fn canonical_actor_identity_is_readable() {
    let address = ActorAddress::new("collab-document", "document-42").unwrap();
    assert_eq!(address.to_string(), "collab-document/document-42");
}
