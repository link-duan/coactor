use std::sync::atomic::{AtomicUsize, Ordering};

use coactor::{
    ActorAddress, ActorContext, ActorId, ActorRefError, BuildError, RuntimeBuilder, actor,
};

static CONSTRUCTIONS: AtomicUsize = AtomicUsize::new(0);

#[derive(Default)]
struct AppState {
    offset: i64,
}

struct CounterActor {
    value: i64,
}

#[actor(name = "counter")]
impl CounterActor {
    pub fn new(actor_id: ActorId) -> Self {
        if actor_id.as_bytes() == b"room-7" {
            CONSTRUCTIONS.fetch_add(1, Ordering::SeqCst);
        }
        Self { value: 0 }
    }

    #[coactor::command]
    pub async fn add(&mut self, context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount + context.state().offset;
        self.value
    }
}

struct DuplicateCounterActor;

#[actor(name = "counter")]
impl DuplicateCounterActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn value(&mut self, _context: &ActorContext<'_, AppState>) -> i64 {
        0
    }
}

struct UnregisteredActor;

#[actor(name = "unregistered")]
impl UnregisteredActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn value(&mut self, _context: &ActorContext<'_, AppState>) -> i64 {
        0
    }
}

#[tokio::test]
async fn counter_activates_lazily_and_returns_a_typed_result() {
    CONSTRUCTIONS.store(0, Ordering::SeqCst);
    let runtime = RuntimeBuilder::new(AppState { offset: 1 })
        .register::<CounterActor>()
        .build()
        .expect("runtime should build");

    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("room-7"))
        .expect("actor type should be registered");

    assert_eq!(CONSTRUCTIONS.load(Ordering::SeqCst), 0);
    assert_eq!(counter.add(2).await.expect("command should succeed"), 3);
    assert_eq!(CONSTRUCTIONS.load(Ordering::SeqCst), 1);
    assert_eq!(counter.add(4).await.expect("command should succeed"), 8);
    assert_eq!(CONSTRUCTIONS.load(Ordering::SeqCst), 1);
}

#[test]
fn actor_address_has_a_stable_length_prefixed_encoding() {
    let address = ActorAddress::new("counter", ActorId::new([0x10, 0x20]));

    assert_eq!(
        address.to_bytes(),
        [
            0, 0, 0, 7, b'c', b'o', b'u', b'n', b't', b'e', b'r', 0x10, 0x20
        ]
    );
}

#[test]
fn duplicate_actor_type_names_fail_runtime_construction() {
    let result = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .register::<DuplicateCounterActor>()
        .build();

    assert!(matches!(
        result,
        Err(BuildError::DuplicateActorType("counter"))
    ));
}

#[test]
fn unregistered_actor_type_fails_before_activation() {
    let runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .expect("runtime should build");

    let result = runtime.actor_ref::<UnregisteredActor>(ActorId::from("missing"));

    assert!(matches!(
        result,
        Err(ActorRefError::ActorTypeNotRegistered("unregistered"))
    ));
}

#[tokio::test]
async fn actor_ids_are_isolated_while_refs_for_one_address_share_state() {
    let runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .expect("runtime should build");

    let first = runtime
        .actor_ref::<CounterActor>(ActorId::from("first"))
        .expect("actor type should be registered");
    let same = runtime
        .actor_ref::<CounterActor>(ActorId::from("first"))
        .expect("actor type should be registered");
    let second = runtime
        .actor_ref::<CounterActor>(ActorId::from("second"))
        .expect("actor type should be registered");

    assert_eq!(first.add(2).await.expect("command should succeed"), 2);
    assert_eq!(same.add(3).await.expect("command should succeed"), 5);
    assert_eq!(second.add(4).await.expect("command should succeed"), 4);
}
