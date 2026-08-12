use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use coactor::{ActorContext, ActorId, RuntimeBuilder, SendError, actor};
use tokio::sync::Notify;

#[derive(Clone)]
struct AppState {
    activation_attempts: Arc<AtomicUsize>,
    activation_entered: Arc<Notify>,
    activation_release: Arc<Notify>,
    panic_entered: Arc<Notify>,
    panic_release: Arc<Notify>,
}

struct LifecycleActor {
    value: i64,
}

#[actor(name = "lifecycle")]
impl LifecycleActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self { value: 0 }
    }

    pub async fn on_activate(
        &mut self,
        context: &ActorContext<'_, AppState>,
    ) -> Result<(), &'static str> {
        let attempt = context
            .state()
            .activation_attempts
            .fetch_add(1, Ordering::SeqCst);
        context.state().activation_entered.notify_one();
        context.state().activation_release.notified().await;
        if attempt == 0 {
            Err("first activation fails")
        } else {
            Ok(())
        }
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn panic_after_mutation(&mut self, context: &ActorContext<'_, AppState>) -> i64 {
        context.state().panic_entered.notify_one();
        context.state().panic_release.notified().await;
        self.value = 99;
        panic!("boom")
    }
}

fn runtime() -> (coactor::Runtime<AppState>, AppState) {
    let state = AppState {
        activation_attempts: Arc::new(AtomicUsize::new(0)),
        activation_entered: Arc::new(Notify::new()),
        activation_release: Arc::new(Notify::new()),
        panic_entered: Arc::new(Notify::new()),
        panic_release: Arc::new(Notify::new()),
    };
    let runtime = RuntimeBuilder::new(state.clone())
        .mailbox_capacity(4)
        .register::<LifecycleActor>()
        .build()
        .expect("runtime should build");
    (runtime, state)
}

#[tokio::test]
async fn activation_gates_queued_commands_and_a_failure_is_retryable() {
    let (runtime, state) = runtime();
    let actor = runtime
        .actor_ref::<LifecycleActor>(ActorId::from("activation"))
        .expect("registered Actor Type");

    let first = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(1).await }
    });
    state.activation_entered.notified().await;
    let second = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    tokio::task::yield_now().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(20), async {
            while !first.is_finished() || !second.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_err(),
        "commands must remain queued behind activation"
    );

    state.activation_release.notify_one();
    assert_eq!(first.await.unwrap(), Err(SendError::ActivationFailed));
    assert_eq!(second.await.unwrap(), Err(SendError::ActivationFailed));

    let retry = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(3).await }
    });
    state.activation_entered.notified().await;
    state.activation_release.notify_one();
    assert_eq!(retry.await.unwrap().unwrap(), 3);
    assert_eq!(state.activation_attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn handler_panic_stops_current_and_queued_commands_then_allows_fresh_activation() {
    let (runtime, state) = runtime();
    state.activation_attempts.store(1, Ordering::SeqCst);
    let actor = runtime
        .actor_ref::<LifecycleActor>(ActorId::from("panic"))
        .expect("registered Actor Type");

    let panic_call = tokio::spawn({
        let actor = actor.clone();
        async move { actor.panic_after_mutation().await }
    });
    state.activation_entered.notified().await;
    state.activation_release.notify_one();
    state.panic_entered.notified().await;
    let queued = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(1).await }
    });
    tokio::task::yield_now().await;
    state.panic_release.notify_one();

    assert_eq!(panic_call.await.unwrap(), Err(SendError::ActorStopped));
    assert_eq!(queued.await.unwrap(), Err(SendError::ActorStopped));

    let fresh = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    state.activation_entered.notified().await;
    state.activation_release.notify_one();
    assert_eq!(fresh.await.unwrap().unwrap(), 2);
}

#[tokio::test]
async fn activation_failure_releases_runtime_capacity() {
    let (runtime, state) = {
        let state = AppState {
            activation_attempts: Arc::new(AtomicUsize::new(0)),
            activation_entered: Arc::new(Notify::new()),
            activation_release: Arc::new(Notify::new()),
            panic_entered: Arc::new(Notify::new()),
            panic_release: Arc::new(Notify::new()),
        };
        let runtime = RuntimeBuilder::new(state.clone())
            .max_active_actors(1)
            .register::<LifecycleActor>()
            .build()
            .expect("runtime should build");
        (runtime, state)
    };
    let first = runtime
        .actor_ref::<LifecycleActor>(ActorId::from("first-fails"))
        .expect("registered Actor Type");
    let second = runtime
        .actor_ref::<LifecycleActor>(ActorId::from("second-starts"))
        .expect("registered Actor Type");

    let failed = tokio::spawn(async move { first.add(1).await });
    state.activation_entered.notified().await;
    state.activation_release.notify_one();
    assert_eq!(failed.await.unwrap(), Err(SendError::ActivationFailed));

    let replacement = tokio::spawn(async move { second.add(2).await });
    state.activation_entered.notified().await;
    state.activation_release.notify_one();
    assert_eq!(replacement.await.unwrap().unwrap(), 2);
}
