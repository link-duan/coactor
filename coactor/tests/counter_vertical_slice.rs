use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use coactor::{ActorContext, ActorId, DeactivationReason, RuntimeBuilder, SendError, actor};
use tokio::sync::Notify;

#[derive(Clone)]
struct AppState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
    lifecycle: Arc<Mutex<Vec<&'static str>>>,
}

struct CounterActor {
    value: i64,
}

#[actor(name = "vertical-counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self { value: 0 }
    }

    pub async fn on_activate(
        &mut self,
        context: &ActorContext<'_, AppState>,
    ) -> Result<(), &'static str> {
        context.state().lifecycle.lock().unwrap().push("activate");
        Ok(())
    }

    pub async fn on_deactivate(
        &mut self,
        context: &ActorContext<'_, AppState>,
        reason: DeactivationReason,
    ) {
        context
            .state()
            .lifecycle
            .lock()
            .unwrap()
            .push(match reason {
                DeactivationReason::Idle => "idle",
                DeactivationReason::Shutdown => "shutdown",
            });
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn blocked_add(&mut self, context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        context.state().entered.notify_one();
        context.state().release.notified().await;
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn panic_now(&mut self, _context: &ActorContext<'_, AppState>) {
        panic!("counter failure")
    }
}

fn state() -> AppState {
    AppState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        lifecycle: Arc::new(Mutex::new(Vec::new())),
    }
}

#[tokio::test]
async fn counter_ref_covers_activation_serialization_overload_and_failure() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .mailbox_capacity(1)
        .register::<CounterActor>()
        .build()
        .expect("runtime should build");
    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("hot"))
        .expect("registered Actor Type");

    let executing = tokio::spawn({
        let counter = counter.clone();
        async move { counter.blocked_add(1).await }
    });
    state.entered.notified().await;
    let queued = tokio::spawn({
        let counter = counter.clone();
        async move { counter.add(2).await }
    });
    tokio::task::yield_now().await;
    assert_eq!(counter.add(3).await, Err(SendError::MailboxFull));

    state.release.notify_one();
    assert_eq!(executing.await.unwrap().unwrap(), 1);
    assert_eq!(queued.await.unwrap().unwrap(), 3);
    assert_eq!(counter.panic_now().await, Err(SendError::ActorStopped));
    assert_eq!(counter.add(4).await.unwrap(), 4);
    assert_eq!(*state.lifecycle.lock().unwrap(), ["activate", "activate"]);

    runtime.shutdown().await;
}

#[tokio::test(start_paused = true)]
async fn counter_ref_covers_capacity_passivation_reset_and_shutdown() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .max_active_actors(1)
        .idle_timeout(Duration::from_secs(5))
        .register::<CounterActor>()
        .build()
        .expect("runtime should build");
    let first = runtime
        .actor_ref::<CounterActor>(ActorId::from("first"))
        .expect("registered Actor Type");
    let second = runtime
        .actor_ref::<CounterActor>(ActorId::from("second"))
        .expect("registered Actor Type");

    assert_eq!(first.add(5).await.unwrap(), 5);
    assert_eq!(second.add(1).await, Err(SendError::RuntimeAtCapacity));
    tokio::time::advance(Duration::from_secs(5)).await;
    tokio::task::yield_now().await;

    assert_eq!(first.add(2).await.unwrap(), 2);
    assert!(state.lifecycle.lock().unwrap().contains(&"idle"));

    runtime.shutdown().await;
    assert!(state.lifecycle.lock().unwrap().contains(&"shutdown"));
    assert_eq!(first.add(1).await, Err(SendError::RuntimeStopped));
}
