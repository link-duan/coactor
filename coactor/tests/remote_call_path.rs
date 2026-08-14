use std::{sync::Arc, time::Duration};

use crate::{
    http_fixture::ReplyDroppingProxy,
    runtime::testing,
    test_support::{TestOwnershipBackend, start_cluster},
};
use coactor::{
    ActorAddress, ActorId, CommandContext, RemoteProtocolError, RuntimeBuilder, SendError, actor,
};
use prost::Message;
use tokio::sync::Notify;

#[derive(Clone, PartialEq, Message)]
struct AddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, Message)]
struct AddResponse {
    #[prost(int64, tag = "1")]
    value: i64,
}

#[derive(Clone, PartialEq, Message)]
struct CheckedAddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, Message)]
struct CheckedAddError {
    #[prost(string, tag = "1")]
    reason: String,
}

#[derive(Clone, PartialEq, Message)]
struct BlockRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, Message)]
struct PanicRequest {}

#[derive(Clone, Default)]
struct AppState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct CounterActor {
    state: Arc<AppState>,
    value: i64,
}

#[actor(name = "remote-counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId, state: Arc<AppState>) -> Self {
        Self { state, value: 0 }
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: AddRequest) -> AddResponse {
        self.value += request.amount;
        AddResponse { value: self.value }
    }

    #[coactor::command(remote)]
    pub async fn checked_add(
        &mut self,
        _context: &CommandContext,
        request: CheckedAddRequest,
    ) -> Result<AddResponse, CheckedAddError> {
        if request.amount < 0 {
            return Err(CheckedAddError {
                reason: "negative amount".to_owned(),
            });
        }
        self.value += request.amount;
        Ok(AddResponse { value: self.value })
    }

    #[coactor::command(remote)]
    pub async fn blocked_add(
        &mut self,
        _context: &CommandContext,
        request: BlockRequest,
    ) -> AddResponse {
        self.state.entered.notify_one();
        self.state.release.notified().await;
        self.value += request.amount;
        AddResponse { value: self.value }
    }

    #[coactor::command(remote)]
    pub async fn panic(
        &mut self,
        _context: &CommandContext,
        _request: PanicRequest,
    ) -> AddResponse {
        panic!("remote handler panic")
    }
}

struct ClientOnlyActor;

#[actor(name = "client-only")]
impl ClientOnlyActor {
    pub fn new(_actor_id: ActorId, _state: Arc<AppState>) -> Self {
        Self
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: AddRequest) -> AddResponse {
        AddResponse {
            value: request.amount,
        }
    }
}

struct MismatchedCommandActor;

#[actor(name = "remote-counter")]
impl MismatchedCommandActor {
    pub fn new(_actor_id: ActorId, _state: Arc<AppState>) -> Self {
        Self
    }

    #[coactor::command(remote)]
    pub async fn different(
        &mut self,
        _context: &CommandContext,
        request: AddRequest,
    ) -> AddResponse {
        AddResponse {
            value: request.amount,
        }
    }
}

#[tokio::test]
async fn a_typed_actor_ref_calls_an_actor_on_another_runtime() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let server_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("counter-1"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("counter-1"))
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 2 }).await.unwrap(),
        AddResponse { value: 2 }
    );
    assert_eq!(
        counter.add(AddRequest { amount: 3 }).await.unwrap(),
        AddResponse { value: 5 }
    );

    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_enabled_commands_keep_the_local_typed_ref_contract() {
    let runtime = RuntimeBuilder::local(AppState::default())
        .register::<CounterActor>()
        .start()
        .await
        .unwrap();
    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("local"))
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 2 }).await,
        Ok(AddResponse { value: 2 })
    );
    assert_eq!(
        counter.checked_add(CheckedAddRequest { amount: -1 }).await,
        Err(SendError::HandlerError(CheckedAddError {
            reason: "negative amount".to_owned(),
        }))
    );

    runtime.shutdown().await;
}

#[tokio::test]
async fn remote_calls_preserve_business_errors_and_mailbox_overload() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let state = AppState::default();
    let server_runtime = start_cluster(
        RuntimeBuilder::local(state.clone())
            .mailbox_capacity(1)
            .register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("overload"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("overload"))
        .unwrap();

    assert_eq!(
        counter.checked_add(CheckedAddRequest { amount: -1 }).await,
        Err(SendError::HandlerError(CheckedAddError {
            reason: "negative amount".to_owned(),
        }))
    );

    let executing = tokio::spawn({
        let counter = counter.clone();
        async move { counter.blocked_add(BlockRequest { amount: 1 }).await }
    });
    state.entered.notified().await;
    let queued = tokio::spawn({
        let counter = counter.clone();
        async move { counter.add(AddRequest { amount: 2 }).await }
    });
    tokio::task::yield_now().await;
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(1),
            counter.add(AddRequest { amount: 3 }),
        )
        .await
        .unwrap(),
        Err(SendError::MailboxFull)
    );

    state.release.notify_one();
    assert_eq!(executing.await.unwrap().unwrap().value, 1);
    assert_eq!(queued.await.unwrap().unwrap().value, 3);
    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_handler_failure_preserves_the_local_error_category() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let server_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("panic"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("panic"))
        .unwrap();

    assert_eq!(
        counter.panic(PanicRequest {}).await,
        Err(SendError::ActorStopped)
    );

    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_protocol_failures_are_structured() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let empty_server = start_cluster(
        RuntimeBuilder::local(AppState::default()),
        storage.clone(),
        "empty",
    )
    .await;
    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<ClientOnlyActor>(),
        storage.clone(),
        "client",
    )
    .await;
    let missing_id = ActorId::from("missing");
    storage.assign_owner(
        ActorAddress::new("client-only", missing_id.clone()),
        "empty",
    );
    let missing_actor = client_runtime
        .actor_ref::<ClientOnlyActor>(missing_id)
        .unwrap();
    assert_eq!(
        missing_actor.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::ActorTypeNotRegistered
        ))
    );
    empty_server.shutdown().await;
    client_runtime.shutdown().await;

    let storage = Arc::new(TestOwnershipBackend::default());
    let mismatched_server = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<MismatchedCommandActor>(),
        storage.clone(),
        "mismatched",
    )
    .await;
    let command_client = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage.clone(),
        "client",
    )
    .await;
    let missing_id = ActorId::from("missing");
    storage.assign_owner(
        ActorAddress::new("remote-counter", missing_id.clone()),
        "mismatched",
    );
    let missing_command = command_client
        .actor_ref::<CounterActor>(missing_id)
        .unwrap();
    assert_eq!(
        missing_command.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::CommandNotRegistered
        ))
    );
    mismatched_server.shutdown().await;
    command_client.shutdown().await;
}

#[tokio::test]
async fn incompatible_runtime_protocols_are_rejected() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let server_runtime = start_cluster(
        testing::with_peer_protocol_version(RuntimeBuilder::local(AppState::default()), 2)
            .register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("version"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("version"))
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::VersionMismatch
        ))
    );

    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn a_lost_reply_reports_unknown_outcome_without_replaying_the_command() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let state = AppState::default();
    let server_runtime = start_cluster(
        RuntimeBuilder::local(state.clone()).register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("lost-reply"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let upstream = storage.endpoint("server");
    let proxy = ReplyDroppingProxy::start(upstream).await;
    storage.redirect(
        "server",
        proxy
            .endpoint()
            .strip_prefix("http://")
            .unwrap()
            .parse()
            .unwrap(),
    );
    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("lost-reply"))
        .unwrap();

    let call = tokio::spawn({
        let counter = counter.clone();
        async move { counter.blocked_add(BlockRequest { amount: 1 }).await }
    });
    state.entered.notified().await;
    proxy.drop_connection().await;
    assert_eq!(call.await.unwrap(), Err(SendError::OutcomeUnknown));
    state.release.notify_one();

    assert_eq!(
        server_counter.add(AddRequest { amount: 1 }).await.unwrap(),
        AddResponse { value: 2 }
    );

    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn caller_timeout_does_not_retract_an_accepted_remote_command() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let state = AppState::default();
    let server_runtime = start_cluster(
        RuntimeBuilder::local(state.clone()).register::<CounterActor>(),
        storage.clone(),
        "server",
    )
    .await;
    let server_counter = server_runtime
        .actor_ref::<CounterActor>(ActorId::from("caller-timeout"))
        .unwrap();
    server_counter.add(AddRequest { amount: 0 }).await.unwrap();

    let client_runtime = start_cluster(
        RuntimeBuilder::local(AppState::default()).register::<CounterActor>(),
        storage,
        "client",
    )
    .await;
    let counter = client_runtime
        .actor_ref::<CounterActor>(ActorId::from("caller-timeout"))
        .unwrap();

    let timed_out = tokio::spawn({
        let counter = counter.clone();
        async move {
            tokio::time::timeout(
                Duration::from_millis(20),
                counter.blocked_add(BlockRequest { amount: 2 }),
            )
            .await
        }
    });
    state.entered.notified().await;
    assert!(timed_out.await.unwrap().is_err());
    state.release.notify_one();

    assert_eq!(
        counter.add(AddRequest { amount: 3 }).await.unwrap(),
        AddResponse { value: 5 }
    );

    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}
