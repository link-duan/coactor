use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use coactor::{ActorId, CommandContext, DeactivationReason, RuntimeBuilder, SendError, actor};
use tokio::sync::Notify;

#[derive(Clone)]
struct AppState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    deactivations: Arc<Mutex<Vec<DeactivationReason>>>,
}

struct ShutdownActor {
    state: Arc<AppState>,
    value: i64,
}

#[actor(name = "shutdown")]
impl ShutdownActor {
    pub fn new(_actor_id: ActorId, state: Arc<AppState>) -> Self {
        Self { state, value: 0 }
    }

    pub async fn on_deactivate(&mut self, reason: DeactivationReason) {
        self.state.deactivations.lock().unwrap().push(reason);
    }

    #[coactor::command]
    pub async fn blocked_add(&mut self, _context: &CommandContext, amount: i64) -> i64 {
        self.state.entered.notify_one();
        self.state.release.notified().await;
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &CommandContext, amount: i64) -> i64 {
        self.value += amount;
        self.value
    }
}

async fn runtime(timeout: Duration) -> (coactor::Runtime<AppState>, AppState) {
    let state = AppState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        deactivations: Arc::new(Mutex::new(Vec::new())),
    };
    let runtime = RuntimeBuilder::local(state.clone())
        .shutdown_timeout(timeout)
        .register::<ShutdownActor>()
        .start()
        .await
        .expect("runtime should build");
    (runtime, state)
}

#[tokio::test]
async fn graceful_shutdown_rejects_new_calls_and_drains_accepted_work() {
    let (runtime, state) = runtime(Duration::from_secs(5)).await;
    let actor = runtime
        .actor_ref::<ShutdownActor>(ActorId::from("drain"))
        .expect("registered Actor Type");
    let accepted = tokio::spawn({
        let actor = actor.clone();
        async move { actor.blocked_add(1).await }
    });
    state.entered.notified().await;
    let queued = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    tokio::task::yield_now().await;

    let shutdown = tokio::spawn(async move { runtime.shutdown().await });
    tokio::task::yield_now().await;
    assert_eq!(actor.add(3).await, Err(SendError::RuntimeShuttingDown));

    state.release.notify_one();
    assert_eq!(accepted.await.unwrap().unwrap(), 1);
    assert_eq!(queued.await.unwrap().unwrap(), 3);
    shutdown.await.unwrap();
    assert_eq!(
        *state.deactivations.lock().unwrap(),
        [DeactivationReason::Shutdown]
    );
    assert_eq!(actor.add(1).await, Err(SendError::RuntimeStopped));
}

#[tokio::test(start_paused = true)]
async fn shutdown_timeout_stops_unfinished_commands() {
    let (runtime, state) = runtime(Duration::from_secs(2)).await;
    let actor = runtime
        .actor_ref::<ShutdownActor>(ActorId::from("timeout"))
        .expect("registered Actor Type");
    let accepted = tokio::spawn({
        let actor = actor.clone();
        async move { actor.blocked_add(1).await }
    });
    state.entered.notified().await;

    let shutdown = tokio::spawn(async move { runtime.shutdown().await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(2)).await;

    shutdown.await.unwrap();
    assert_eq!(accepted.await.unwrap(), Err(SendError::ActorStopped));
    assert!(state.deactivations.lock().unwrap().is_empty());
    assert_eq!(actor.add(1).await, Err(SendError::RuntimeStopped));
}
