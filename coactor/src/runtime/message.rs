use std::{any::Any, marker::PhantomData, time::Duration};

use tokio::sync::oneshot;

use super::actor::{ActorRuntime, ActorType, BoxFuture, ErasedActor, MessageOutcome};
use super::session::SessionId;
use crate::{DeactivationReason, SendError};

/// 进入 Actor mailbox 的内部消息：业务 Action 与 Session 生命周期控制消息。
pub(crate) enum MailboxMessage {
    Action { session_id: SessionId, payload: Vec<u8> },
    SessionOpened {
        session_id: SessionId,
        complete: oneshot::Sender<Result<(), SendError>>,
    },
    SessionClosed { session_id: SessionId },
}

pub(crate) type MailboxSender = tokio::sync::mpsc::Sender<MailboxMessage>;

pub(crate) type Activate =
    for<'a> fn(&'a mut (dyn Any + Send)) -> BoxFuture<'a, Result<(), String>>;
pub(crate) type Deactivate =
    for<'a> fn(&'a mut (dyn Any + Send), DeactivationReason) -> BoxFuture<'a, ()>;
pub(crate) type Handle = for<'a> fn(
    &'a mut (dyn Any + Send),
    &'a super::actor::MessageContext,
    &'a [u8],
) -> BoxFuture<'a, MessageOutcome>;
pub(crate) type SessionHook =
    for<'a> fn(&'a mut (dyn Any + Send), &'a super::actor::MessageContext) -> BoxFuture<'a, ()>;

pub struct Registration<S> {
    pub name: &'static str,
    pub(crate) create: fn(ActorRuntime<S>) -> Box<dyn Any + Send>,
    pub(crate) activate: Activate,
    pub(crate) deactivate: Deactivate,
    pub(crate) handle: Handle,
    pub(crate) session_opened: SessionHook,
    pub(crate) session_closed: SessionHook,
    pub mailbox_capacity: Option<usize>,
    pub idle_timeout: Option<Duration>,
    marker: PhantomData<fn(S)>,
}

impl<S> Registration<S> {
    pub fn of<A>(name: &'static str) -> Self
    where
        A: ActorType<S>,
    {
        Self {
            name,
            create: |runtime| Box::new(A::create(runtime)),
            activate: <A as ErasedActor>::activate,
            deactivate: <A as ErasedActor>::deactivate,
            handle: <A as ErasedActor>::handle,
            session_opened: <A as ErasedActor>::session_opened,
            session_closed: <A as ErasedActor>::session_closed,
            mailbox_capacity: None,
            idle_timeout: None,
            marker: PhantomData,
        }
    }
}
