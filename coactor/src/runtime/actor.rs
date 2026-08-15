use std::{fmt, future::Future, pin::Pin, sync::Arc, sync::Weak};

use crate::{ActorAddress, ActorId, DeactivationReason};

use super::core::ServerInner;
use super::session::SessionHandle;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// actor 侧持有的运行时句柄：绑定自身 Actor Address，携带 AppState 与输出能力。
/// 每个 Active Actor 在构造时获得一次，是未来扩展（session 查询、定向、timer）的挂载点。
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
    pub fn actor_id(&self) -> &ActorId {
        self.address.actor_id()
    }

    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }

    pub fn app_state(&self) -> &Arc<S> {
        &self.state
    }

    /// 向当前 Actor 的全部存活 Session 广播一个 Event；尽力而为，单点失败不影响其余。
    pub async fn broadcast(&self, msg: Vec<u8>) {
        let Some(runtime) = self.runtime.upgrade() else {
            return;
        };
        runtime.broadcast_event(&self.address, msg).await;
    }
}

/// 一次 Action 处理的最小只读运行环境：标识当前 Actor，携带当前 Session 的出站句柄。
pub struct MessageContext {
    pub(crate) address: ActorAddress,
    pub(crate) session: SessionHandle,
}

impl MessageContext {
    pub fn actor_id(&self) -> &ActorId {
        self.address.actor_id()
    }

    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }

    /// 当前 Session 的出站句柄；`clone()` 后存入状态可作定向推送。
    pub fn session(&self) -> &SessionHandle {
        &self.session
    }

    /// 向当前 Session 的 caller 定向发送一个 Event。
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), crate::SendError> {
        self.session.send(msg).await
    }
}

/// Consumer 实现的 Actor 契约：业务逻辑 + 生命周期 hook。
/// 依赖 `Send` supertrait 让 `async fn` 的 future 自动 Send（见 ADR-0007）。
#[allow(async_fn_in_trait)]
pub trait Actor<S>: Send + 'static {
    /// 构造 Active Actor；`runtime` 提供 AppState、Actor 身份与 `broadcast` 等输出能力。
    fn new(runtime: ActorRuntime<S>) -> Self;

    /// 处理一条入站 Action（字节负载）。
    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]);

    async fn on_activate(&mut self) -> Result<(), String> {
        Ok(())
    }

    async fn on_deactivate(&mut self, _reason: DeactivationReason) {}

    /// 新 Session 建立（open）时调用；ctx 可直接推送。
    async fn on_session_opened(&mut self, _ctx: &MessageContext) {}

    /// Session 关闭（caller 断开）时调用；ctx 的发送已不可用。
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
    fn activate<'a>(
        actor: &'a mut (dyn std::any::Any + Send),
    ) -> BoxFuture<'a, Result<(), String>>;

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
