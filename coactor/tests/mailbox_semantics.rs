use std::{sync::Arc, time::Duration};

use coactor::{ActorContext, ActorId, ActorTypeConfig, RuntimeBuilder, SendError, actor};
use tokio::sync::{Barrier, Notify};

#[derive(Clone)]
struct AppState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct ProbeActor {
    value: i64,
}

#[actor(name = "probe")]
impl ProbeActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self { value: 0 }
    }

    #[coactor::command]
    pub async fn blocked_add(&mut self, context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        context.state().entered.notify_one();
        context.state().release.notified().await;
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount;
        self.value
    }

    #[coactor::command]
    pub async fn checked_add(
        &mut self,
        _context: &ActorContext<'_, AppState>,
        amount: i64,
    ) -> Result<i64, &'static str> {
        if amount < 0 {
            return Err("negative amount");
        }
        self.value += amount;
        Ok(self.value)
    }
}

fn runtime(mailbox_capacity: usize) -> (coactor::Runtime<AppState>, AppState) {
    let state = AppState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let runtime = RuntimeBuilder::new(state.clone())
        .mailbox_capacity(mailbox_capacity)
        .register::<ProbeActor>()
        .build()
        .expect("runtime should build");
    (runtime, state)
}

#[tokio::test]
async fn one_actor_is_non_reentrant_while_another_actor_progresses() {
    let (runtime, state) = runtime(4);
    let first = runtime
        .actor_ref::<ProbeActor>(ActorId::from("first"))
        .expect("registered Actor Type");
    let second = runtime
        .actor_ref::<ProbeActor>(ActorId::from("second"))
        .expect("registered Actor Type");

    let blocked = tokio::spawn({
        let first = first.clone();
        async move { first.blocked_add(1).await }
    });
    state.entered.notified().await;

    let same_actor = tokio::spawn({
        let first = first.clone();
        async move { first.add(2).await }
    });
    assert!(
        tokio::time::timeout(Duration::from_millis(20), same_actor)
            .await
            .is_err(),
        "a second method on the same Actor must not begin across await"
    );
    assert_eq!(second.add(5).await.expect("other Actor should run"), 5);

    state.release.notify_one();
    assert_eq!(blocked.await.unwrap().unwrap(), 1);
    assert_eq!(first.add(0).await.unwrap(), 3);
}

#[tokio::test]
async fn a_full_mailbox_is_rejected_immediately() {
    let (runtime, state) = runtime(1);
    let actor = runtime
        .actor_ref::<ProbeActor>(ActorId::from("hot"))
        .expect("registered Actor Type");

    let executing = tokio::spawn({
        let actor = actor.clone();
        async move { actor.blocked_add(1).await }
    });
    state.entered.notified().await;
    let queued = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    tokio::task::yield_now().await;

    let result = tokio::time::timeout(Duration::from_millis(20), actor.add(3))
        .await
        .expect("mailbox rejection must not wait");
    assert_eq!(result, Err(SendError::MailboxFull));

    state.release.notify_one();
    assert_eq!(executing.await.unwrap().unwrap(), 1);
    assert_eq!(queued.await.unwrap().unwrap(), 3);
}

#[tokio::test]
async fn cancelling_a_caller_does_not_retract_an_accepted_command() {
    let (runtime, state) = runtime(2);
    let actor = runtime
        .actor_ref::<ProbeActor>(ActorId::from("cancel"))
        .expect("registered Actor Type");

    let executing = tokio::spawn({
        let actor = actor.clone();
        async move { actor.blocked_add(1).await }
    });
    state.entered.notified().await;

    let cancelled = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    tokio::task::yield_now().await;
    cancelled.abort();

    state.release.notify_one();
    assert_eq!(executing.await.unwrap().unwrap(), 1);
    assert_eq!(actor.add(0).await.unwrap(), 3);
}

#[tokio::test]
async fn handler_errors_are_flattened_and_do_not_stop_the_actor() {
    let (runtime, _state) = runtime(2);
    let actor = runtime
        .actor_ref::<ProbeActor>(ActorId::from("errors"))
        .expect("registered Actor Type");

    assert_eq!(
        actor.checked_add(-1).await,
        Err(SendError::HandlerError("negative amount"))
    );
    assert_eq!(actor.checked_add(3).await.unwrap(), 3);
}

#[tokio::test]
async fn concurrent_accepted_commands_are_all_processed_once() {
    let (runtime, _state) = runtime(64);
    let actor = runtime
        .actor_ref::<ProbeActor>(ActorId::from("many"))
        .expect("registered Actor Type");
    let start = Arc::new(Barrier::new(17));
    let mut tasks = Vec::new();

    for _ in 0..16 {
        let actor = actor.clone();
        let start = start.clone();
        tasks.push(tokio::spawn(async move {
            start.wait().await;
            actor.add(1).await
        }));
    }
    start.wait().await;
    for task in tasks {
        task.await.unwrap().unwrap();
    }

    assert_eq!(actor.add(0).await.unwrap(), 16);
}

#[tokio::test]
async fn an_actor_type_can_override_the_runtime_mailbox_capacity() {
    let state = AppState {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
    };
    let runtime = RuntimeBuilder::new(state.clone())
        .mailbox_capacity(1)
        .register_with::<ProbeActor>(ActorTypeConfig::new().mailbox_capacity(2))
        .build()
        .expect("runtime should build");
    let actor = runtime
        .actor_ref::<ProbeActor>(ActorId::from("override"))
        .expect("registered Actor Type");

    let executing = tokio::spawn({
        let actor = actor.clone();
        async move { actor.blocked_add(1).await }
    });
    state.entered.notified().await;
    let first_queued = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(2).await }
    });
    let second_queued = tokio::spawn({
        let actor = actor.clone();
        async move { actor.add(3).await }
    });
    tokio::task::yield_now().await;

    assert_eq!(actor.add(4).await, Err(SendError::MailboxFull));

    state.release.notify_one();
    assert_eq!(executing.await.unwrap().unwrap(), 1);
    first_queued.await.unwrap().unwrap();
    second_queued.await.unwrap().unwrap();
    assert_eq!(actor.add(0).await.unwrap(), 6);
}
