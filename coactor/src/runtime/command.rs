use std::{
    any::Any, collections::HashMap, future::Future, marker::PhantomData, pin::Pin, sync::Arc,
    time::Duration,
};
use tokio::sync::mpsc;

use super::core::ActorRef;
use crate::{
    ActorId, CommandContext, DeactivationReason, RemoteProtocolError, SendError, peer_protocol,
};

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub trait ErasedCommand<S>: Send + 'static {
    fn execute<'a>(
        self: Box<Self>,
        actor: &'a mut (dyn Any + Send),
        context: CommandContext,
    ) -> BoxFuture<'a, CommandOutcome>;

    fn fail(self: Box<Self>, error: RuntimeError);
}

pub(crate) type Command<S> = Box<dyn ErasedCommand<S>>;
pub(crate) type CommandSender<S> = mpsc::Sender<Command<S>>;
pub(crate) type Activate =
    for<'a> fn(&'a mut (dyn Any + Send)) -> BoxFuture<'a, Result<(), String>>;
pub(crate) type Deactivate =
    for<'a> fn(&'a mut (dyn Any + Send), DeactivationReason) -> BoxFuture<'a, ()>;

pub enum CommandOutcome {
    Completed,
    Panicked(Box<dyn FnOnce() + Send>),
}

#[derive(Clone, Copy)]
pub enum RuntimeError {
    ActorStopped,
    RuntimeStopped,
    NodeFenced,
    MailboxFull,
    ActivationFailed,
    ActorDeactivating,
    RuntimeAtCapacity,
    RuntimeShuttingDown,
    RemoteUnavailable,
    OwnershipUnavailable,
    OutcomeUnknown,
    NotOwner,
    RemoteProtocol,
    ActorTypeNotRegistered,
    CommandNotRegistered,
    MalformedPayload,
}

impl RuntimeError {
    pub(crate) fn to_wire(self) -> i32 {
        use peer_protocol::RuntimeFailure;
        (match self {
            Self::MailboxFull => RuntimeFailure::MailboxFull,
            Self::ActivationFailed => RuntimeFailure::ActivationFailed,
            Self::ActorDeactivating => RuntimeFailure::ActorDeactivating,
            Self::RuntimeAtCapacity => RuntimeFailure::RuntimeAtCapacity,
            Self::RuntimeShuttingDown => RuntimeFailure::RuntimeShuttingDown,
            Self::ActorStopped => RuntimeFailure::ActorStopped,
            Self::RuntimeStopped => RuntimeFailure::RuntimeStopped,
            Self::NodeFenced => RuntimeFailure::NodeFenced,
            Self::RemoteProtocol => RuntimeFailure::ProtocolMismatch,
            Self::ActorTypeNotRegistered => RuntimeFailure::ActorTypeNotRegistered,
            Self::CommandNotRegistered => RuntimeFailure::CommandNotRegistered,
            Self::MalformedPayload => RuntimeFailure::MalformedPayload,
            Self::RemoteUnavailable => RuntimeFailure::RemoteUnavailable,
            Self::OwnershipUnavailable => RuntimeFailure::OwnershipUnavailable,
            Self::OutcomeUnknown => {
                unreachable!("unknown outcomes are classified only by the caller")
            }
            Self::NotOwner => RuntimeFailure::NotOwner,
        }) as i32
    }

    pub(crate) fn from_wire(value: i32) -> Self {
        use peer_protocol::RuntimeFailure;
        match RuntimeFailure::try_from(value).unwrap_or(RuntimeFailure::Unspecified) {
            RuntimeFailure::MailboxFull => Self::MailboxFull,
            RuntimeFailure::ActivationFailed => Self::ActivationFailed,
            RuntimeFailure::ActorDeactivating => Self::ActorDeactivating,
            RuntimeFailure::RuntimeAtCapacity => Self::RuntimeAtCapacity,
            RuntimeFailure::RuntimeShuttingDown => Self::RuntimeShuttingDown,
            RuntimeFailure::ActorStopped => Self::ActorStopped,
            RuntimeFailure::RuntimeStopped => Self::RuntimeStopped,
            RuntimeFailure::NodeFenced => Self::NodeFenced,
            RuntimeFailure::ActorTypeNotRegistered => Self::ActorTypeNotRegistered,
            RuntimeFailure::CommandNotRegistered => Self::CommandNotRegistered,
            RuntimeFailure::MalformedPayload => Self::MalformedPayload,
            RuntimeFailure::RemoteUnavailable => Self::RemoteUnavailable,
            RuntimeFailure::OwnershipUnavailable => Self::OwnershipUnavailable,
            RuntimeFailure::NotOwner => Self::NotOwner,
            RuntimeFailure::ProtocolMismatch | RuntimeFailure::Unspecified => Self::RemoteProtocol,
        }
    }
}

impl<E> From<RuntimeError> for SendError<E> {
    fn from(value: RuntimeError) -> Self {
        match value {
            RuntimeError::ActorStopped => Self::ActorStopped,
            RuntimeError::RuntimeStopped => Self::RuntimeStopped,
            RuntimeError::NodeFenced => Self::NodeFenced,
            RuntimeError::MailboxFull => Self::MailboxFull,
            RuntimeError::ActivationFailed => Self::ActivationFailed,
            RuntimeError::ActorDeactivating => Self::ActorDeactivating,
            RuntimeError::RuntimeAtCapacity => Self::RuntimeAtCapacity,
            RuntimeError::RuntimeShuttingDown => Self::RuntimeShuttingDown,
            RuntimeError::RemoteUnavailable => Self::RemoteUnavailable,
            RuntimeError::OwnershipUnavailable => Self::OwnershipUnavailable,
            RuntimeError::OutcomeUnknown => Self::OutcomeUnknown,
            RuntimeError::NotOwner => Self::OwnershipUnavailable,
            RuntimeError::RemoteProtocol => {
                Self::RemoteProtocol(RemoteProtocolError::VersionMismatch)
            }
            RuntimeError::ActorTypeNotRegistered => {
                Self::RemoteProtocol(RemoteProtocolError::ActorTypeNotRegistered)
            }
            RuntimeError::CommandNotRegistered => {
                Self::RemoteProtocol(RemoteProtocolError::CommandNotRegistered)
            }
            RuntimeError::MalformedPayload => {
                Self::RemoteProtocol(RemoteProtocolError::MalformedRequest)
            }
        }
    }
}

pub trait ActorType<S>: Send + 'static {
    const NAME: &'static str;
    type Ref;

    fn create(actor_id: ActorId, state: Arc<S>) -> Self;
    fn activate<'a>(actor: &'a mut (dyn Any + Send)) -> BoxFuture<'a, Result<(), String>>;
    fn deactivate<'a>(
        actor: &'a mut (dyn Any + Send),
        reason: DeactivationReason,
    ) -> BoxFuture<'a, ()>;
    fn make_ref(inner: ActorRef<S>) -> Self::Ref;
    fn remote_commands() -> HashMap<&'static str, RemoteCommandFactory<S>>;
}

pub struct Registration<S> {
    pub name: &'static str,
    pub(crate) create: fn(ActorId, Arc<S>) -> Box<dyn Any + Send>,
    pub(crate) activate: Activate,
    pub(crate) deactivate: Deactivate,
    pub remote_commands: HashMap<&'static str, RemoteCommandFactory<S>>,
    pub mailbox_capacity: Option<usize>,
    pub idle_timeout: Option<Duration>,
    marker: PhantomData<fn(S)>,
}

impl<S> Registration<S> {
    pub fn of<A>() -> Self
    where
        A: ActorType<S>,
    {
        Self {
            name: A::NAME,
            create: |actor_id, state| Box::new(A::create(actor_id, state)),
            activate: A::activate,
            deactivate: A::deactivate,
            remote_commands: A::remote_commands(),
            mailbox_capacity: None,
            idle_timeout: None,
            marker: PhantomData,
        }
    }
}

pub enum RemotePayload {
    Success(Vec<u8>),
    HandlerError(Vec<u8>),
}

pub struct RemoteCall {
    pub command: &'static str,
    pub payload: Vec<u8>,
}

pub enum DispatchOutcome {
    Local,
    Remote(RemotePayload),
}

pub enum RemoteReplyError {
    Handler(Vec<u8>),
    Runtime(RuntimeError),
}

pub struct RemoteInvocation<S> {
    pub command: Command<S>,
    pub reply: BoxFuture<'static, Result<Vec<u8>, RemoteReplyError>>,
}

pub type RemoteCommandFactory<S> = fn(Vec<u8>) -> Result<RemoteInvocation<S>, RuntimeError>;
