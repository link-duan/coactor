use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use coactor::{
    Actor, ActorId, ActorRuntime, DeactivationReason, MessageContext, RuntimeBuilder, SendError,
    actor,
};
use tokio::sync::Notify;

#[derive(Clone)]
struct AppState {
    lifecycle: Arc<Mutex<Vec<&'static str>>>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

fn state() -> AppState {
    AppState {
        lifecycle: Arc::new(Mutex::new(Vec::new())),
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    }
}

#[actor(name = "session-test")]
struct TestActor {
    runtime: ActorRuntime<AppState>,
    state: Arc<AppState>,
    value: i64,
}

impl Actor<AppState> for TestActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        let state = runtime.app_state().clone();
        Self {
            runtime,
            state,
            value: 0,
        }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        match msg {
                b"add" => {
                    self.value += 1;
                    let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;
                }
                b"echo" => {
                    let _ = ctx.send(b"echoed".to_vec()).await;
                }
                b"broadcast" => {
                    self.runtime.broadcast(b"broadcasted".to_vec()).await;
                }
                b"sleep" => {
                    tokio::time::sleep(Duration::from_secs(10)).await;
                }
                b"block" => {
                    self.state.entered.notify_one();
                    self.state.release.notified().await;
                }
                b"panic" => panic!("session test failure"),
                _ => {
                    let _ = ctx.send(b"unknown".to_vec()).await;
                }
            }
    }

    async fn on_activate(&mut self) -> Result<(), String> {
        self.state.lifecycle.lock().unwrap().push("activate");
        Ok(())
    }

    async fn on_deactivate(&mut self, reason: DeactivationReason) {
        self.state.lifecycle.lock().unwrap().push(match reason {
            DeactivationReason::Idle => "idle",
            DeactivationReason::Shutdown => "shutdown",
        });
    }

    async fn on_session_opened(&mut self, ctx: &MessageContext) {
        let _ = ctx.send(b"opened".to_vec()).await;
    }
}

#[tokio::test]
async fn session_carries_bidirectional_messages() {
    let state = state();
    let runtime = RuntimeBuilder::local(state)
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("bidirectional"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    // on_session_opened 立即推送
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    session.send(b"add".to_vec()).await.expect("action accepted");
    assert_eq!(
        session.recv().await,
        Some(Ok(1i64.to_be_bytes().to_vec()))
    );
    session.send(b"add".to_vec()).await.expect("action accepted");
    assert_eq!(
        session.recv().await,
        Some(Ok(2i64.to_be_bytes().to_vec()))
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn on_session_opened_and_broadcast_reach_all_sessions() {
    let runtime = RuntimeBuilder::local(state())
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("broadcast"))
        .expect("registered");

    let mut first = actor.open().await.expect("first session");
    assert_eq!(first.recv().await, Some(Ok(b"opened".to_vec())));
    let mut second = actor.open().await.expect("second session");
    assert_eq!(second.recv().await, Some(Ok(b"opened".to_vec())));

    first.send(b"broadcast".to_vec()).await.expect("action accepted");
    assert_eq!(first.recv().await, Some(Ok(b"broadcasted".to_vec())));
    assert_eq!(second.recv().await, Some(Ok(b"broadcasted".to_vec())));

    runtime.shutdown().await;
}

#[tokio::test]
async fn live_sessions_block_passivation() {
    let state = state();
    let runtime = RuntimeBuilder::local(state.clone())
        .idle_timeout(Duration::from_secs(1))
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("passivation"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    assert_eq!(*state.lifecycle.lock().unwrap(), ["activate"]);

    // session 存活时，idle 到期不 passivate
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(*state.lifecycle.lock().unwrap(), ["activate"]);

    // 关闭 session 后 idle 到期才 passivate
    drop(session);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(state.lifecycle.lock().unwrap().contains(&"idle"));

    runtime.shutdown().await;
}

#[tokio::test]
async fn panic_terminates_sessions_with_actor_stopped() {
    let runtime = RuntimeBuilder::local(state())
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("panic"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    session.send(b"panic".to_vec()).await.expect("action accepted");
    assert_eq!(session.recv().await, Some(Err(SendError::ActorStopped)));

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn mailbox_full_returns_send_error_without_terminating_session() {
    let runtime = RuntimeBuilder::local(state())
        .mailbox_capacity(1)
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("overload"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));

    // sleep 占用执行；第二个 add 进 mailbox；第三个满载
    session
        .send(b"sleep".to_vec())
        .await
        .expect("sleep action accepted");
    tokio::time::advance(Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    session
        .send(b"add".to_vec())
        .await
        .expect("queued action accepted");
    assert_eq!(
        session.send(b"add".to_vec()).await,
        Err(SendError::MailboxFull)
    );

    // 释放 sleep 后 actor 恢复处理
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        session.recv().await,
        Some(Ok(1i64.to_be_bytes().to_vec()))
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn shutdown_terminates_sessions_with_runtime_shutting_down() {
    let runtime = RuntimeBuilder::local(state())
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("shutdown"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));

    runtime.shutdown().await;
    assert_eq!(session.recv().await, Some(Err(SendError::RuntimeShuttingDown)));
}

#[tokio::test]
async fn actions_from_one_session_are_processed_in_order() {
    let runtime = RuntimeBuilder::local(state())
        .mailbox_capacity(128)
        .register::<TestActor>()
        .start()
        .await
        .expect("runtime builds");
    let actor = runtime
        .actor(TestActor::ACTOR_NAME, ActorId::from("ordering"))
        .expect("registered");

    let mut session = actor.open().await.expect("open succeeds");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    for _ in 0..50 {
        session.send(b"add".to_vec()).await.expect("queued");
    }
    for expected in 1..=50 {
        assert_eq!(
            session.recv().await,
            Some(Ok((expected as i64).to_be_bytes().to_vec()))
        );
    }

    runtime.shutdown().await;
}
