use std::{convert::Infallible, fmt, sync::Arc, time::Duration};

use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ActorId(Arc<[u8]>);

impl ActorId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into().into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&str> for ActorId {
    fn from(value: &str) -> Self {
        Self::new(value.as_bytes())
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ActorId").field(&self.0).finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorAddress {
    actor_type: Arc<str>,
    actor_id: ActorId,
}

impl ActorAddress {
    pub fn new(actor_type: impl Into<Arc<str>>, actor_id: ActorId) -> Self {
        Self {
            actor_type: actor_type.into(),
            actor_id,
        }
    }

    pub fn actor_type(&self) -> &str {
        &self.actor_type
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let name = self.actor_type.as_bytes();
        let mut bytes = Vec::with_capacity(4 + name.len() + self.actor_id.as_bytes().len());
        bytes.extend_from_slice(&(name.len() as u32).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(self.actor_id.as_bytes());
        bytes
    }
}

pub struct CommandContext {
    pub(crate) address: ActorAddress,
}

impl CommandContext {
    pub fn actor_id(&self) -> &ActorId {
        self.address.actor_id()
    }

    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum StartError {
    #[error("Actor Type `{0}` was registered more than once")]
    DuplicateActorType(&'static str),
    #[error("mailbox capacity must be greater than zero")]
    InvalidMailboxCapacity,
    #[error("max_active_actors must be greater than zero")]
    InvalidMaxActiveActors,
    #[error("Node ID must not be empty")]
    InvalidNodeId,
    #[error("advertised address must have a non-zero port")]
    InvalidAdvertisedAddress,
    #[error("lease timing is invalid")]
    InvalidLeaseTiming,
    #[error("the peer listener could not bind")]
    BindFailed,
    #[error("Node Lease is already owned")]
    LeaseConflict,
    #[error("Node Lease acquisition could not be confirmed")]
    LeaseUnconfirmed,
    #[error("ownership authority is unavailable during startup")]
    OwnershipUnavailable,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActorRefError {
    #[error("Actor Type `{0}` is not registered")]
    ActorTypeNotRegistered(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ActorTypeConfig {
    pub(crate) mailbox_capacity: Option<usize>,
    pub(crate) idle_timeout: Option<Duration>,
}

impl ActorTypeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeactivationReason {
    Idle,
    Shutdown,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SendError<E = Infallible> {
    #[error("handler failed: {0:?}")]
    HandlerError(E),
    #[error("the Active Actor mailbox is full")]
    MailboxFull,
    #[error("the Active Actor failed to activate")]
    ActivationFailed,
    #[error("the Active Actor is deactivating")]
    ActorDeactivating,
    #[error("the runtime has reached its Active Actor limit")]
    RuntimeAtCapacity,
    #[error("the runtime is shutting down")]
    RuntimeShuttingDown,
    #[error("the Active Actor stopped")]
    ActorStopped,
    #[error("the CoActor runtime stopped")]
    RuntimeStopped,
    #[error("the CoActor runtime lost Node authority")]
    NodeFenced,
    #[error("the remote runtime is unavailable")]
    RemoteUnavailable,
    #[error("distributed ownership is unavailable")]
    OwnershipUnavailable,
    #[error("the remote command outcome is unknown")]
    OutcomeUnknown,
    #[error("the remote runtime rejected the protocol: {0}")]
    RemoteProtocol(RemoteProtocolError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RemoteProtocolError {
    #[error("runtime protocol mismatch")]
    VersionMismatch,
    #[error("Actor Type is not registered")]
    ActorTypeNotRegistered,
    #[error("command is not registered")]
    CommandNotRegistered,
    #[error("malformed request payload")]
    MalformedRequest,
    #[error("malformed success payload")]
    MalformedSuccess,
    #[error("malformed handler error payload")]
    MalformedHandlerError,
    #[error("unexpected handler error payload")]
    UnexpectedHandlerError,
}
