use std::time::Duration;

use crate::{
    Actor, ActorAddress, ActorRuntime, MessageContext, MutationOutcome, NodeDirectory,
    NodeLeaseStore, NodeRecord, NodeSessionId, Revision, Server, ServerError, actor,
    test_support::TestCoordinationStore,
};

#[actor]
struct Probe;
impl Actor<()> for Probe {
    fn new(_: ActorRuntime<()>) -> Self {
        Self
    }
    async fn on_message(&mut self, _: &MessageContext, _: &[u8]) {}
}

#[actor]
struct HangingProbe;
impl Actor<()> for HangingProbe {
    fn new(_: ActorRuntime<()>) -> Self {
        Self
    }
    async fn on_message(&mut self, _: &MessageContext, _: &[u8]) {}
    async fn on_deactivate(&mut self, _: crate::DeactivationReason) {
        std::future::pending().await
    }
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

#[tokio::test]
async fn production_builder_serves_with_state_in_both_orders() {
    Server::builder(TestCoordinationStore::default())
        .with_state(SharedState)
        .actor::<StatefulProbe>("stateful")
        .advertised_endpoint("127.0.0.1:7000")
        .shutdown_signal(async {})
        .serve(":0")
        .await
        .unwrap();
    Server::builder(TestCoordinationStore::default())
        .actor::<StatefulProbe>("stateful")
        .with_state(SharedState)
        .advertised_endpoint("127.0.0.1:7000")
        .shutdown_signal(async {})
        .serve(":0")
        .await
        .unwrap();
}

#[tokio::test]
async fn server_serve_accepts_wildcard_port_and_gracefully_stops() {
    Server::builder(TestCoordinationStore::default())
        .advertised_endpoint("127.0.0.1:7000")
        .actor::<Probe>("probe")
        .shutdown_signal(async {})
        .serve(":0")
        .await
        .unwrap();
}

#[tokio::test]
async fn server_serve_accepts_bracketed_ipv6_address_when_available() {
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        return;
    }
    Server::builder(TestCoordinationStore::default())
        .advertised_endpoint("[::1]:7000")
        .actor::<Probe>("probe")
        .shutdown_signal(async {})
        .serve("[::1]:0")
        .await
        .unwrap();
}

#[tokio::test]
async fn server_serve_releases_node_lease_after_shutdown_signal() {
    let store = TestCoordinationStore::default();
    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(
        Server::builder(store.clone())
            .advertised_endpoint("127.0.0.1:7000")
            .node_id("node-a")
            .actor::<Probe>("probe")
            .shutdown_signal(async {
                let _ = shutdown_signal.await;
            })
            .serve("127.0.0.1:0"),
    );

    while store.read_node("node-a").await.unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    shutdown.send(()).unwrap();

    serving.await.unwrap().unwrap();
    assert!(store.read_node("node-a").await.unwrap().is_none());
}

#[tokio::test]
async fn server_serve_reports_graceful_shutdown_timeout() {
    let socket = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let listen_address = socket.local_addr().unwrap().to_string();
    drop(socket);

    let store = TestCoordinationStore::default();
    let serving_store = store.clone();
    let advertised_endpoint = listen_address.clone();
    let (shutdown, shutdown_signal) = tokio::sync::oneshot::channel();
    let serving = tokio::spawn(async move {
        Server::builder(serving_store)
            .advertised_endpoint(&advertised_endpoint)
            .node_id("node-a")
            .actor::<HangingProbe>("hanging-probe")
            .shutdown_timeout(Duration::from_millis(10))
            .shutdown_signal(async {
                let _ = shutdown_signal.await;
            })
            .serve(&listen_address)
            .await
    });

    while store.read_node("node-a").await.unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    let client = crate::Client::builder(store).build().unwrap();
    let address = ActorAddress::new("hanging-probe", "probe-1").unwrap();
    let _session = client.open(&address).await.unwrap();

    shutdown.send(()).unwrap();

    assert!(matches!(
        serving.await.unwrap(),
        Err(ServerError::ShutdownTimedOut)
    ));
    client.shutdown().await;
}

#[tokio::test]
async fn server_builder_validates_named_network_configuration_when_serving() {
    let error = Server::builder(TestCoordinationStore::default())
        .actor::<Probe>("probe")
        .serve("127.0.0.1:0")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::MissingAdvertisedEndpoint));

    let error = Server::builder(TestCoordinationStore::default())
        .advertised_endpoint("http://example.com:7000")
        .actor::<Probe>("probe")
        .serve("127.0.0.1:0")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::InvalidAdvertisedEndpoint));

    let error = Server::builder(TestCoordinationStore::default())
        .advertised_endpoint("example.com:7000")
        .node_id("Bad_Node")
        .actor::<Probe>("probe")
        .serve("127.0.0.1:0")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::InvalidNodeId));

    let error = Server::builder(TestCoordinationStore::default())
        .advertised_endpoint("example.com:7000")
        .actor::<Probe>("probe")
        .serve("localhost:7000")
        .await
        .unwrap_err();
    assert!(matches!(error, ServerError::InvalidListenAddress));

    for address in ["127.0.0.1", "[::1", ":", ":70000"] {
        let error = Server::builder(TestCoordinationStore::default())
            .advertised_endpoint("example.com:7000")
            .actor::<Probe>("probe")
            .serve(address)
            .await
            .unwrap_err();
        assert!(matches!(error, ServerError::InvalidListenAddress));
    }

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
async fn server_serve_reports_self_fencing() {
    let store = TestCoordinationStore::default();
    let serving = tokio::spawn(
        Server::builder(store.clone())
            .advertised_endpoint("127.0.0.1:7000")
            .node_id("node-a")
            .actor::<Probe>("probe")
            .serve("127.0.0.1:0"),
    );

    while store.read_node("node-a").await.unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    store.remove_node("node-a");
    tokio::time::advance(Duration::from_secs(11)).await;
    tokio::task::yield_now().await;

    assert!(matches!(serving.await.unwrap(), Err(ServerError::Fenced)));
}

#[tokio::test(start_paused = true)]
async fn server_serve_reports_unexpected_renewal_service_stop() {
    let store = TestCoordinationStore::default();
    let serving = tokio::spawn(
        Server::builder(store.clone())
            .advertised_endpoint("127.0.0.1:7000")
            .node_id("node-a")
            .node_lease_ttl(Duration::from_secs(3))
            .actor::<Probe>("probe")
            .serve("127.0.0.1:0"),
    );

    while store.read_node("node-a").await.unwrap().is_none() {
        tokio::task::yield_now().await;
    }
    store.panic_on_renewal();
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;

    assert!(matches!(
        serving.await.unwrap(),
        Err(ServerError::ServiceStopped)
    ));
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
