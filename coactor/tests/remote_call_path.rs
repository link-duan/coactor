use std::{sync::Arc, time::Duration};

use coactor::{ActorId, CommandContext, RemoteProtocolError, RuntimeBuilder, SendError, actor};
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
    let server_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .expect("server runtime should build");
    let peer = server_runtime
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .expect("peer server should bind");

    let client_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .expect("client runtime should build");
    let counter = client_runtime
        .test_remote_actor_ref::<CounterActor>(ActorId::from("counter-1"), peer.endpoint())
        .expect("remote Actor Type should be registered");

    assert_eq!(
        counter.add(AddRequest { amount: 2 }).await.unwrap(),
        AddResponse { value: 2 }
    );
    assert_eq!(
        counter.add(AddRequest { amount: 3 }).await.unwrap(),
        AddResponse { value: 5 }
    );

    peer.shutdown().await;
    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_enabled_commands_keep_the_local_typed_ref_contract() {
    let runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
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
    let state = AppState::default();
    let server_runtime = RuntimeBuilder::new(state.clone())
        .mailbox_capacity(1)
        .register::<CounterActor>()
        .build()
        .unwrap();
    let peer = server_runtime
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .unwrap();
    let counter = client_runtime
        .test_remote_actor_ref::<CounterActor>(ActorId::from("overload"), peer.endpoint())
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
    peer.shutdown().await;
    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_handler_failure_preserves_the_local_error_category() {
    let server_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .unwrap();
    let peer = server_runtime
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .unwrap();
    let counter = client_runtime
        .test_remote_actor_ref::<CounterActor>(ActorId::from("panic"), peer.endpoint())
        .unwrap();

    assert_eq!(
        counter.panic(PanicRequest {}).await,
        Err(SendError::ActorStopped)
    );

    peer.shutdown().await;
    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn remote_protocol_failures_are_structured() {
    let empty_server = RuntimeBuilder::new(AppState::default()).build().unwrap();
    let empty_peer = empty_server
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client_runtime = RuntimeBuilder::new(AppState::default())
        .register::<ClientOnlyActor>()
        .build()
        .unwrap();
    let missing_actor = client_runtime
        .test_remote_actor_ref::<ClientOnlyActor>(ActorId::from("missing"), empty_peer.endpoint())
        .unwrap();
    assert_eq!(
        missing_actor.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::ActorTypeNotRegistered
        ))
    );
    empty_peer.shutdown().await;
    empty_server.shutdown().await;

    let mismatched_server = RuntimeBuilder::new(AppState::default())
        .register::<MismatchedCommandActor>()
        .build()
        .unwrap();
    let mismatched_peer = mismatched_server
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let command_client = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .unwrap();
    let missing_command = command_client
        .test_remote_actor_ref::<CounterActor>(ActorId::from("missing"), mismatched_peer.endpoint())
        .unwrap();
    assert_eq!(
        missing_command.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::CommandNotRegistered
        ))
    );
    mismatched_peer.shutdown().await;
    mismatched_server.shutdown().await;
    command_client.shutdown().await;
    client_runtime.shutdown().await;
}

#[tokio::test]
async fn incompatible_runtime_protocols_are_rejected() {
    let server_runtime = RuntimeBuilder::new(AppState::default())
        .peer_protocol_version(2)
        .register::<CounterActor>()
        .build()
        .unwrap();
    let peer = server_runtime
        .serve_test_peer("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let client_runtime = RuntimeBuilder::new(AppState::default())
        .register::<CounterActor>()
        .build()
        .unwrap();
    let counter = client_runtime
        .test_remote_actor_ref::<CounterActor>(ActorId::from("version"), peer.endpoint())
        .unwrap();

    assert_eq!(
        counter.add(AddRequest { amount: 1 }).await,
        Err(SendError::RemoteProtocol(
            RemoteProtocolError::VersionMismatch
        ))
    );

    peer.shutdown().await;
    server_runtime.shutdown().await;
    client_runtime.shutdown().await;
}
