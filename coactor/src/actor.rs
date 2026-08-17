use std::{fmt, sync::Arc, time::Duration};

use thiserror::Error;

/// A validated, reusable Actor address.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorAddress {
    actor_type: Arc<str>,
    actor_id: Arc<str>,
}

impl ActorAddress {
    /// Validates and constructs an Actor Address from an Actor Type and Actor ID.
    pub fn new(
        actor_type: impl AsRef<str>,
        actor_id: impl AsRef<str>,
    ) -> Result<Self, ActorAddressError> {
        let actor_type = actor_type.as_ref();
        let actor_id = actor_id.as_ref();
        if !is_dns_label(actor_type) {
            return Err(ActorAddressError::InvalidActorType);
        }
        if !is_dns_label(actor_id) {
            return Err(ActorAddressError::InvalidActorId);
        }
        Ok(Self {
            actor_type: Arc::from(actor_type),
            actor_id: Arc::from(actor_id),
        })
    }

    /// Returns the stable Actor Type name.
    pub fn actor_type(&self) -> &str {
        &self.actor_type
    }

    /// Returns the stable Actor ID within its Actor Type.
    pub fn actor_id(&self) -> &str {
        &self.actor_id
    }
}

pub(crate) fn is_dns_label(value: &str) -> bool {
    let bytes = value.as_bytes();
    (1..=63).contains(&bytes.len())
        && bytes.first().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || *byte == b'-')
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ActorAddressError {
    #[error("Actor Type must be a Kubernetes DNS label")]
    InvalidActorType,
    #[error("Actor ID must be a Kubernetes DNS label")]
    InvalidActorId,
}

#[derive(Debug, Error)]
pub enum ServerStartError {
    #[error("bind address is required")]
    MissingBindAddress,
    #[error("advertised endpoint is required")]
    MissingAdvertisedEndpoint,
    #[error("Actor Type `{0}` is invalid")]
    InvalidActorType(String),
    #[error("Actor Type `{0}` was registered more than once")]
    DuplicateActorType(String),
    #[error("mailbox capacity must be greater than zero")]
    InvalidMailboxCapacity,
    #[error("max_active_actors must be greater than zero")]
    InvalidMaxActiveActors,
    #[error("Node ID must be a Kubernetes DNS label")]
    InvalidNodeId,
    #[error("advertised endpoint must be a canonical host:port without a scheme or path")]
    InvalidAdvertisedEndpoint,
    #[error("Node Lease TTL must be greater than zero")]
    InvalidNodeLeaseTtl,
    #[error("Coordination operation timeout must be greater than zero")]
    InvalidCoordinationTimeout,
    #[error("peer connection timeout must be greater than zero")]
    InvalidPeerConnectTimeout,
    #[error("deactivation timeout must be greater than zero")]
    InvalidDeactivationTimeout,
    #[error("shutdown timeout must be greater than zero")]
    InvalidShutdownTimeout,
    #[error("the peer listener could not bind")]
    BindFailed(#[source] std::io::Error),
    #[error("Node Lease is already owned")]
    LeaseConflict,
    #[error("Node Lease acquisition could not be confirmed")]
    LeaseUnconfirmed,
    #[error("Coordination Store is unavailable during startup")]
    Coordination(#[source] crate::coordination::CoordinationError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ServerFailure {
    #[error("the Server self-fenced after losing Node authority")]
    Fenced,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ClientBuildError {
    #[error("open timeout must be greater than zero")]
    InvalidOpenTimeout,
    #[error("peer connection timeout must be greater than zero")]
    InvalidPeerConnectTimeout,
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum OpenError {
    #[error("the Client runtime stopped")]
    RuntimeStopped,
    #[error("the Node Directory is unavailable")]
    DirectoryUnavailable,
    #[error("the Node Directory has no available Gateway")]
    NoAvailableGateway,
    #[error("the remote runtime is unavailable")]
    RemoteUnavailable,
    #[error("the Actor Type is not registered")]
    ActorTypeNotRegistered,
    #[error("the runtime has reached its Active Actor limit")]
    RuntimeAtCapacity,
    #[error("distributed ownership is unavailable")]
    OwnershipUnavailable,
    #[error("the remote runtime rejected the protocol")]
    RemoteProtocol,
}

impl From<SendError> for OpenError {
    fn from(error: SendError) -> Self {
        match error {
            SendError::RuntimeStopped | SendError::RuntimeShuttingDown => Self::RuntimeStopped,
            SendError::DirectoryUnavailable => Self::DirectoryUnavailable,
            SendError::NoAvailableGateway => Self::NoAvailableGateway,
            SendError::ActorTypeNotRegistered(_) => Self::ActorTypeNotRegistered,
            SendError::RuntimeAtCapacity => Self::RuntimeAtCapacity,
            SendError::OwnershipUnavailable => Self::OwnershipUnavailable,
            SendError::RemoteProtocol(_) => Self::RemoteProtocol,
            _ => Self::RemoteUnavailable,
        }
    }
}

/// Per-Actor-Type runtime policy overrides.
#[derive(Clone, Copy, Debug, Default)]
pub struct ActorConfig {
    pub(crate) name: &'static str,
    pub(crate) mailbox_capacity: Option<usize>,
    pub(crate) idle_timeout: Option<Duration>,
}

impl ActorConfig {
    /// Creates a configuration with the supplied Actor Type name and default policy.
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            ..Self::default()
        }
    }

    /// Overrides the mailbox capacity for this Actor Type.
    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }

    /// Overrides the idle timeout for this Actor Type.
    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }
}

pub trait IntoActorConfig {
    fn into_actor_config(self) -> ActorConfig;
}

impl IntoActorConfig for &'static str {
    fn into_actor_config(self) -> ActorConfig {
        ActorConfig::new(self)
    }
}

impl IntoActorConfig for ActorConfig {
    fn into_actor_config(self) -> ActorConfig {
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeactivationReason {
    Idle,
    Shutdown,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum SendError {
    #[error("Actor Type `{0}` is not registered")]
    ActorTypeNotRegistered(String),
    #[error("the node does not own this Actor Address")]
    NotOwner,
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
    #[error("the Node Directory is unavailable")]
    DirectoryUnavailable,
    #[error("the Node Directory has no available Gateway")]
    NoAvailableGateway,
    #[error("distributed ownership is unavailable")]
    OwnershipUnavailable,
    #[error("the remote runtime rejected the protocol: {0}")]
    RemoteProtocol(RemoteProtocolError),
}

#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum RemoteProtocolError {
    #[error("runtime protocol mismatch")]
    VersionMismatch,
    #[error("Actor Type is not registered")]
    ActorTypeNotRegistered,
    #[error("malformed session message")]
    MalformedMessage,
}

impl SendError {
    pub(crate) fn to_wire(&self) -> i32 {
        use crate::peer_protocol::RuntimeFailure;
        (match self {
            Self::ActorTypeNotRegistered(_) => RuntimeFailure::ActorTypeNotRegistered,
            Self::NotOwner => RuntimeFailure::NotOwner,
            Self::MailboxFull => RuntimeFailure::MailboxFull,
            Self::ActivationFailed => RuntimeFailure::ActivationFailed,
            Self::ActorDeactivating => RuntimeFailure::ActorDeactivating,
            Self::RuntimeAtCapacity => RuntimeFailure::RuntimeAtCapacity,
            Self::RuntimeShuttingDown => RuntimeFailure::RuntimeShuttingDown,
            Self::ActorStopped => RuntimeFailure::ActorStopped,
            Self::RuntimeStopped => RuntimeFailure::RuntimeStopped,
            Self::NodeFenced => RuntimeFailure::NodeFenced,
            Self::RemoteUnavailable => RuntimeFailure::RemoteUnavailable,
            Self::DirectoryUnavailable | Self::NoAvailableGateway => {
                RuntimeFailure::RemoteUnavailable
            }
            Self::OwnershipUnavailable => RuntimeFailure::OwnershipUnavailable,
            Self::RemoteProtocol(_) => RuntimeFailure::ProtocolMismatch,
        }) as i32
    }

    pub(crate) fn from_wire(value: i32) -> Self {
        use crate::peer_protocol::RuntimeFailure;
        match RuntimeFailure::try_from(value).unwrap_or(RuntimeFailure::Unspecified) {
            RuntimeFailure::ActorTypeNotRegistered => Self::ActorTypeNotRegistered(String::new()),
            RuntimeFailure::NotOwner => Self::NotOwner,
            RuntimeFailure::MailboxFull => Self::MailboxFull,
            RuntimeFailure::ActivationFailed => Self::ActivationFailed,
            RuntimeFailure::ActorDeactivating => Self::ActorDeactivating,
            RuntimeFailure::RuntimeAtCapacity => Self::RuntimeAtCapacity,
            RuntimeFailure::RuntimeShuttingDown => Self::RuntimeShuttingDown,
            RuntimeFailure::ActorStopped => Self::ActorStopped,
            RuntimeFailure::RuntimeStopped => Self::RuntimeStopped,
            RuntimeFailure::NodeFenced => Self::NodeFenced,
            RuntimeFailure::RemoteUnavailable => Self::RemoteUnavailable,
            RuntimeFailure::OwnershipUnavailable => Self::OwnershipUnavailable,
            _ => Self::RemoteProtocol(RemoteProtocolError::VersionMismatch),
        }
    }
}

impl fmt::Display for ActorAddress {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.actor_type, self.actor_id)
    }
}
