use std::{fmt, future::Future, pin::Pin, sync::Arc, sync::Weak};

use crate::{ActorAddress, DeactivationReason};

use super::core::ServerInner;
use super::session::SessionHandle;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// Runtime capabilities provided when an Active Actor is constructed.
///
/// The handle exposes the Actor's address, shared App State, and Event broadcast.
pub struct ActorRuntime<S> {
    pub(crate) address: ActorAddress,
    pub(crate) state: Arc<S>,
    pub(crate) runtime: Weak<ServerInner<S>>,
}

impl<S> Clone for ActorRuntime<S> {
    fn clone(&self) -> Self {
        Self {
            address: self.address.clone(),
            state: self.state.clone(),
            runtime: self.runtime.clone(),
        }
    }
}

impl<S> fmt::Debug for ActorRuntime<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ActorRuntime")
            .field("address", &self.address)
            .finish_non_exhaustive()
    }
}

impl<S: Send + Sync + 'static> ActorRuntime<S> {
    /// Returns this Actor's ID within its Actor Type.
    pub fn actor_id(&self) -> &str {
        self.address.actor_id()
    }

    /// Returns this Actor's complete logical address.
    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }

    /// Returns the App State shared by all Actor Types in this Server.
    pub fn state(&self) -> &S {
        &self.state
    }

    /// Broadcasts an Event to every live Session of this Actor.
    ///
    /// Delivery is best-effort; failure for one Session does not stop delivery to others.
    pub async fn broadcast(&self, msg: Vec<u8>) {
        let Some(runtime) = self.runtime.upgrade() else {
            return;
        };
        runtime.broadcast_event(&self.address, msg).await;
    }
}

/// Read-only context for processing one Action.
///
/// It identifies the Actor and provides the current Session's outbound handle.
pub struct MessageContext {
    pub(crate) address: ActorAddress,
    pub(crate) session: SessionHandle,
}

impl MessageContext {
    /// Returns the current Actor's ID within its Actor Type.
    pub fn actor_id(&self) -> &str {
        self.address.actor_id()
    }

    /// Returns the current Actor's complete logical address.
    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }

    /// Returns the current Session's outbound handle.
    ///
    /// Clone and retain the handle when the Actor needs to push later Events to this caller.
    pub fn session(&self) -> &SessionHandle {
        &self.session
    }

    /// Sends an Event to the caller associated with the current Session.
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), crate::SendError> {
        self.session.send(msg).await
    }
}

/// The consumer-defined Actor contract.
///
/// CoActor calls these lifecycle methods serially for one Active Actor.
#[allow(async_fn_in_trait)]
pub trait Actor<S>: Send + 'static {
    /// Constructs a new Active Actor using only inexpensive, non-blocking
    /// in-memory state initialization.
    ///
    /// This method must not perform I/O, wait for synchronization, or execute
    /// expensive computation. Blocking here may delay routing and lifecycle work
    /// for unrelated Actors hosted by the same Server.
    ///
    /// Perform asynchronous initialization in [`Actor::on_activate`].
    fn new(runtime: ActorRuntime<S>) -> Self;

    /// Handles one inbound byte Action.
    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]);

    /// Performs asynchronous initialization before the Active Actor begins serving Sessions.
    ///
    /// Use this method for network, database, filesystem, and other asynchronous work. Offload
    /// unavoidable blocking operations with [`tokio::task::spawn_blocking`].
    async fn on_activate(&mut self) -> Result<(), String> {
        Ok(())
    }

    /// Runs before the Active Actor stops for the supplied reason.
    async fn on_deactivate(&mut self, _reason: DeactivationReason) {}

    /// Runs when a new Session has opened and may immediately emit Events.
    async fn on_session_opened(&mut self, _ctx: &MessageContext) {}

    /// Runs after a Session closes. Sending through its context is no longer valid.
    async fn on_session_closed(&mut self, _ctx: &MessageContext) {}
}

/// 一条消息处理的执行结果。
pub enum MessageOutcome {
    Completed,
    Panicked,
}

/// 类型擦除的 dispatch 能力（非泛型，macro 生成 impl）：
/// 在具体类型上下文中调用 `Actor` 的 async fn，使 Send 自动继承生效。
pub trait ErasedActor: Send + 'static {
    fn activate<'a>(actor: &'a mut (dyn std::any::Any + Send))
    -> BoxFuture<'a, Result<(), String>>;

    fn deactivate<'a>(
        actor: &'a mut (dyn std::any::Any + Send),
        reason: DeactivationReason,
    ) -> BoxFuture<'a, ()>;

    fn handle<'a>(
        actor: &'a mut (dyn std::any::Any + Send),
        ctx: &'a MessageContext,
        payload: &'a [u8],
    ) -> BoxFuture<'a, MessageOutcome>;

    fn session_opened<'a>(
        actor: &'a mut (dyn std::any::Any + Send),
        ctx: &'a MessageContext,
    ) -> BoxFuture<'a, ()>;

    fn session_closed<'a>(
        actor: &'a mut (dyn std::any::Any + Send),
        ctx: &'a MessageContext,
    ) -> BoxFuture<'a, ()>;
}

/// runtime 注册一个 Actor Type 所需的能力；由 `#[coactor::actor]` macro 生成实现。
pub trait ActorType<S>: ErasedActor + Send + 'static {
    fn create(runtime: ActorRuntime<S>) -> Self;
}
