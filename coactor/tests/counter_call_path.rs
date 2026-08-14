use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use coactor::{
    ActorAddress, ActorId, ActorRefError, CommandContext, RuntimeBuilder, StartError, actor,
};

static CONSTRUCTIONS: AtomicUsize = AtomicUsize::new(0);

struct AppState {
    offset: i64,
    injected_pointers: Arc<Mutex<Vec<usize>>>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            offset: 0,
            injected_pointers: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

struct CounterActor {
    state: Arc<AppState>,
    value: i64,
}

#[actor(name = "counter")]
impl CounterActor {
    pub fn new(actor_id: ActorId, state: Arc<AppState>) -> Self {
        if actor_id.as_bytes() == b"room-7" {
            CONSTRUCTIONS.fetch_add(1, Ordering::SeqCst);
        }
        state
            .injected_pointers
            .lock()
            .unwrap()
            .push(Arc::as_ptr(&state) as usize);
        Self { state, value: 0 }
    }

    #[coactor::command]
    pub async fn add(&mut self, context: &CommandContext, amount: i64) -> i64 {
        assert_eq!(context.actor_address().actor_type(), "counter");
        self.value += amount + self.state.offset;
        self.value
    }

    #[coactor::command]
    pub async fn identity(&mut self, context: &CommandContext) -> (String, Vec<u8>) {
        (
            context.actor_address().actor_type().to_owned(),
            context.actor_id().as_bytes().to_vec(),
        )
    }
}

struct DuplicateCounterActor;

#[actor(name = "counter")]
impl DuplicateCounterActor {
    pub fn new(_actor_id: ActorId, _state: Arc<AppState>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn value(&mut self, _context: &CommandContext) -> i64 {
        0
    }
}

struct UnregisteredActor;

#[actor(name = "unregistered")]
impl UnregisteredActor {
    pub fn new(_actor_id: ActorId, _state: Arc<AppState>) -> Self {
        Self
    }

    #[coactor::command]
    pub async fn value(&mut self, _context: &CommandContext) -> i64 {
        0
    }
}

#[tokio::test]
async fn counter_activates_lazily_and_returns_a_typed_result() {
    CONSTRUCTIONS.store(0, Ordering::SeqCst);
    let runtime = RuntimeBuilder::local(AppState {
        offset: 1,
        ..AppState::default()
    })
    .register::<CounterActor>()
    .start()
    .await
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

#[tokio::test]
async fn duplicate_actor_type_names_fail_runtime_construction() {
    let result = RuntimeBuilder::local(AppState::default())
        .register::<CounterActor>()
        .register::<DuplicateCounterActor>()
        .start()
        .await;

    assert!(matches!(
        result,
        Err(StartError::DuplicateActorType("counter"))
    ));
}

#[tokio::test]
async fn unregistered_actor_type_fails_before_activation() {
    let runtime = RuntimeBuilder::local(AppState::default())
        .register::<CounterActor>()
        .start()
        .await
        .expect("runtime should build");

    let result = runtime.actor_ref::<UnregisteredActor>(ActorId::from("missing"));

    assert!(matches!(
        result,
        Err(ActorRefError::ActorTypeNotRegistered("unregistered"))
    ));
}

#[tokio::test]
async fn actor_ids_are_isolated_while_refs_for_one_address_share_state() {
    let state = AppState::default();
    let injected_pointers = state.injected_pointers.clone();
    let runtime = RuntimeBuilder::local(state)
        .register::<CounterActor>()
        .start()
        .await
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
    assert_eq!(
        first.identity().await.unwrap(),
        ("counter".to_owned(), b"first".to_vec())
    );
    assert_eq!(
        second.identity().await.unwrap(),
        ("counter".to_owned(), b"second".to_vec())
    );
    let pointers = injected_pointers.lock().unwrap();
    assert_eq!(pointers.len(), 2);
    assert_eq!(pointers[0], pointers[1]);
}
