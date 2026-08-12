use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use coactor::{
    ActorContext, ActorId, ActorTypeConfig, DeactivationReason, RuntimeBuilder, SendError, actor,
};
use tokio::sync::Notify;

#[derive(Clone)]
struct AppState {
    deactivations: Arc<AtomicUsize>,
    deactivation_entered: Arc<Notify>,
    deactivation_release: Arc<Notify>,
}

struct PassivatingActor {
    value: i64,
}

struct PanickingDeactivationActor;

#[actor(name = "panicking-deactivation")]
impl PanickingDeactivationActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    pub async fn on_deactivate(
        &mut self,
        context: &ActorContext<'_, AppState>,
        _reason: DeactivationReason,
    ) {
        context.state().deactivation_entered.notify_one();
        panic!("deactivation panic");
    }

    #[coactor::command]
    pub async fn value(&mut self, _context: &ActorContext<'_, AppState>) -> i64 {
        11
    }
}

#[actor(name = "passivating")]
impl PassivatingActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self { value: 0 }
    }

    pub async fn on_deactivate(
        &mut self,
        context: &ActorContext<'_, AppState>,
        reason: DeactivationReason,
    ) {
        assert_eq!(reason, DeactivationReason::Idle);
        context.state().deactivations.fetch_add(1, Ordering::SeqCst);
        context.state().deactivation_entered.notify_one();
        context.state().deactivation_release.notified().await;
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount;
        self.value
    }
}

fn state() -> AppState {
    AppState {
        deactivations: Arc::new(AtomicUsize::new(0)),
        deactivation_entered: Arc::new(Notify::new()),
        deactivation_release: Arc::new(Notify::new()),
    }
}

#[tokio::test(start_paused = true)]
async fn idle_actor_deactivates_irreversibly_and_reactivates_empty() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .idle_timeout(Duration::from_secs(30))
        .deactivation_timeout(Duration::from_secs(10))
        .register::<PassivatingActor>()
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<PassivatingActor>(ActorId::from("idle"))
        .expect("registered Actor Type");

    assert_eq!(actor.add(5).await.unwrap(), 5);
    tokio::time::advance(Duration::from_secs(30)).await;
    state.deactivation_entered.notified().await;

    assert_eq!(actor.add(1).await, Err(SendError::ActorDeactivating));

    state.deactivation_release.notify_one();
    tokio::task::yield_now().await;
    assert_eq!(actor.add(2).await.unwrap(), 2);
    assert_eq!(state.deactivations.load(Ordering::SeqCst), 1);
}

#[tokio::test(start_paused = true)]
async fn active_actor_capacity_is_released_after_passivation() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .max_active_actors(1)
        .idle_timeout(Duration::from_secs(5))
        .register::<PassivatingActor>()
        .build()
        .expect("runtime should build");
    let first = runtime
        .actor_ref::<PassivatingActor>(ActorId::from("first"))
        .expect("registered Actor Type");
    let second = runtime
        .actor_ref::<PassivatingActor>(ActorId::from("second"))
        .expect("registered Actor Type");

    assert_eq!(first.add(1).await.unwrap(), 1);
    assert_eq!(second.add(1).await, Err(SendError::RuntimeAtCapacity));

    tokio::time::advance(Duration::from_secs(5)).await;
    state.deactivation_entered.notified().await;
    state.deactivation_release.notify_one();
    tokio::task::yield_now().await;

    assert_eq!(second.add(2).await.unwrap(), 2);
}

#[tokio::test(start_paused = true)]
async fn an_actor_type_can_override_the_runtime_idle_timeout() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .idle_timeout(Duration::from_secs(60))
        .register_with::<PassivatingActor>(
            ActorTypeConfig::new().idle_timeout(Duration::from_secs(5)),
        )
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<PassivatingActor>(ActorId::from("override"))
        .expect("registered Actor Type");

    actor.add(1).await.unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    state.deactivation_entered.notified().await;

    assert_eq!(actor.add(0).await, Err(SendError::ActorDeactivating));
    state.deactivation_release.notify_one();
}

#[tokio::test(start_paused = true)]
async fn deactivation_timeout_removes_the_route() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .idle_timeout(Duration::from_secs(5))
        .deactivation_timeout(Duration::from_secs(2))
        .register::<PassivatingActor>()
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<PassivatingActor>(ActorId::from("timeout"))
        .expect("registered Actor Type");

    actor.add(9).await.unwrap();
    tokio::time::advance(Duration::from_secs(5)).await;
    state.deactivation_entered.notified().await;
    tokio::time::advance(Duration::from_secs(2)).await;
    tokio::task::yield_now().await;

    let fresh = actor.add(1);
    state.deactivation_release.notify_one();
    assert_eq!(fresh.await.unwrap(), 1);
}

#[tokio::test(start_paused = true)]
async fn calls_racing_with_passivation_are_never_lost() {
    for round in 0_u32..100 {
        let state = state();
        let runtime = RuntimeBuilder::new(state.clone())
            .idle_timeout(Duration::from_secs(1))
            .deactivation_timeout(Duration::from_secs(1))
            .register::<PassivatingActor>()
            .build()
            .expect("runtime should build");
        let actor = runtime
            .actor_ref::<PassivatingActor>(ActorId::new(round.to_be_bytes()))
            .expect("registered Actor Type");

        actor.add(1).await.unwrap();
        tokio::time::advance(Duration::from_secs(1)).await;
        let call = actor.add(1).await;

        match call {
            Ok(value) => assert_eq!(value, 2),
            Err(SendError::ActorDeactivating) => {
                state.deactivation_release.notify_one();
                tokio::task::yield_now().await;
                assert_eq!(actor.add(1).await.unwrap(), 1);
            }
            other => panic!("unexpected race result: {other:?}"),
        }
    }
}

#[tokio::test(start_paused = true)]
async fn idle_deactivation_panic_does_not_poison_the_actor_address() {
    let state = state();
    let runtime = RuntimeBuilder::new(state.clone())
        .idle_timeout(Duration::from_secs(1))
        .register::<PanickingDeactivationActor>()
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<PanickingDeactivationActor>(ActorId::from("panic-idle"))
        .expect("registered Actor Type");

    assert_eq!(actor.value().await.unwrap(), 11);
    tokio::time::advance(Duration::from_secs(1)).await;
    state.deactivation_entered.notified().await;
    tokio::task::yield_now().await;

    assert_eq!(actor.value().await.unwrap(), 11);
}
