use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use coactor::{
    Actor, ActorAddress, ActorAddressError, ActorConfig, ActorRuntime, MessageContext, SendError,
    actor, test_support::TestServer,
};

#[derive(Clone)]
struct AppState(Arc<AtomicUsize>);

#[actor]
struct EchoActor {
    runtime: ActorRuntime<AppState>,
}

impl Actor<AppState> for EchoActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime }
    }
    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        self.runtime.state().0.fetch_add(1, Ordering::Relaxed);
        ctx.send(msg.to_vec()).await.unwrap();
    }
}

#[actor]
struct UnitActor;
impl Actor<()> for UnitActor {
    fn new(_: ActorRuntime<()>) -> Self {
        Self
    }
    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        ctx.send(msg.to_vec()).await.unwrap();
    }
}

#[tokio::test]
async fn state_can_be_supplied_before_actor_registration() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = TestServer::builder()
        .with_state(AppState(calls.clone()))
        .actor::<EchoActor>("echo")
        .start()
        .await
        .unwrap();
    let address = ActorAddress::new("echo", "before").unwrap();
    let mut session = server.client().open(&address).await.unwrap();
    session.send(b"hello".to_vec()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"hello");
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn state_can_be_supplied_after_actor_registration() {
    let calls = Arc::new(AtomicUsize::new(0));
    let server = TestServer::builder()
        .actor::<EchoActor>(ActorConfig::new("echo").mailbox_capacity(4))
        .with_state(AppState(calls.clone()))
        .start()
        .await
        .unwrap();
    let address = ActorAddress::new("echo", "after").unwrap();
    let mut session = server.client().open(&address).await.unwrap();
    session.send(Vec::new()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), Vec::<u8>::new());
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    server.shutdown().await;
}

#[tokio::test]
async fn unit_state_and_default_actor_config_need_no_extra_values() {
    let server = TestServer::builder()
        .actor::<UnitActor>("unit")
        .start()
        .await
        .unwrap();
    let address = ActorAddress::new("unit", "one").unwrap();
    let mut session = server.client().open(&address).await.unwrap();
    session.send(b"ok".to_vec()).await.unwrap();
    assert_eq!(session.recv().await.unwrap().unwrap(), b"ok");
    server.shutdown().await;
}

#[test]
fn actor_address_validates_both_components_once() {
    assert_eq!(
        ActorAddress::new("Bad", "one"),
        Err(ActorAddressError::InvalidActorType)
    );
    assert_eq!(
        ActorAddress::new("good", "bad/id"),
        Err(ActorAddressError::InvalidActorId)
    );
    let address = ActorAddress::new("room", "room-123").unwrap();
    assert_eq!(address.actor_type(), "room");
    assert_eq!(address.actor_id(), "room-123");
}

#[tokio::test]
async fn session_observes_consuming_test_server_shutdown() {
    let server = TestServer::builder()
        .actor::<UnitActor>("unit")
        .start()
        .await
        .unwrap();
    let address = ActorAddress::new("unit", "shutdown").unwrap();
    let session = server.client().open(&address).await.unwrap();
    server.shutdown().await;
    assert_eq!(
        session.send(Vec::new()).await,
        Err(SendError::RuntimeStopped)
    );
}
