extern crate self as coactor;

mod node_authority;
mod s3_node_lease;

use std::{
    any::Any,
    collections::HashMap,
    convert::Infallible,
    fmt,
    net::SocketAddr,
    sync::{Arc, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, oneshot, watch};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::{Request, Response, Status};

pub use node_authority::{
    ActorOwner, ActorOwnerRecord, ActorOwnerStorage, AmbiguousMutation, DistributedRuntimeBuilder,
    DistributedRuntimeConfig, LeaseMutation, LeaseTiming, NodeLease, NodeLeaseStorage,
    NodeSessionId, OwnershipStorage, OwnershipStorageError, RuntimeStartError, RuntimeSupervision,
    RuntimeTermination, RuntimeTerminationReason, VersionedActorOwnerRecord, VersionedNodeLease,
};
pub use s3_node_lease::{S3NodeLeaseConfig, S3NodeLeaseStorage};

pub use coactor_macros::{actor, command};

const PEER_PROTOCOL_VERSION: u32 = 1;

mod peer_protocol {
    tonic::include_proto!("coactor.peer.v1");
}

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
    address: ActorAddress,
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
pub enum BuildError {
    #[error("Actor Type `{0}` was registered more than once")]
    DuplicateActorType(&'static str),
    #[error("mailbox capacity must be greater than zero")]
    InvalidMailboxCapacity,
    #[error("max_active_actors must be greater than zero")]
    InvalidMaxActiveActors,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActorRefError {
    #[error("Actor Type `{0}` is not registered")]
    ActorTypeNotRegistered(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ActorTypeConfig {
    mailbox_capacity: Option<usize>,
    idle_timeout: Option<Duration>,
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

pub struct RuntimeBuilder<S> {
    state: S,
    registrations: Vec<__private::Registration<S>>,
    mailbox_capacity: usize,
    max_active_actors: usize,
    idle_timeout: Duration,
    deactivation_timeout: Duration,
    shutdown_timeout: Duration,
    peer_protocol_version: u32,
}

impl<S> RuntimeBuilder<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(state: S) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            mailbox_capacity: 32,
            max_active_actors: 10_000,
            idle_timeout: Duration::from_secs(60),
            deactivation_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            peer_protocol_version: PEER_PROTOCOL_VERSION,
        }
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
        self
    }

    pub fn max_active_actors(mut self, maximum: usize) -> Self {
        self.max_active_actors = maximum;
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn deactivation_timeout(mut self, timeout: Duration) -> Self {
        self.deactivation_timeout = timeout;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    #[doc(hidden)]
    pub fn peer_protocol_version(mut self, version: u32) -> Self {
        self.peer_protocol_version = version;
        self
    }

    pub fn register<A>(mut self) -> Self
    where
        A: __private::ActorType<S>,
    {
        self.registrations.push(__private::Registration::of::<A>());
        self
    }

    pub fn register_with<A>(mut self, config: ActorTypeConfig) -> Self
    where
        A: __private::ActorType<S>,
    {
        let mut registration = __private::Registration::of::<A>();
        registration.mailbox_capacity = config.mailbox_capacity;
        registration.idle_timeout = config.idle_timeout;
        self.registrations.push(registration);
        self
    }

    pub fn distributed(
        self,
        config: DistributedRuntimeConfig,
        storage: Arc<dyn OwnershipStorage>,
    ) -> Result<DistributedRuntimeBuilder<S>, RuntimeStartError> {
        config.validate()?;
        Ok(DistributedRuntimeBuilder {
            builder: self,
            config,
            storage,
        })
    }

    pub fn build(self) -> Result<Runtime<S>, BuildError> {
        self.build_with_authority(None, None)
    }

    pub(crate) fn validate(&self) -> Result<(), BuildError> {
        if self.mailbox_capacity == 0 {
            return Err(BuildError::InvalidMailboxCapacity);
        }
        if self.max_active_actors == 0 {
            return Err(BuildError::InvalidMaxActiveActors);
        }
        let mut names = std::collections::HashSet::new();
        for registration in &self.registrations {
            if registration.mailbox_capacity == Some(0) {
                return Err(BuildError::InvalidMailboxCapacity);
            }
            if !names.insert(registration.name) {
                return Err(BuildError::DuplicateActorType(registration.name));
            }
        }
        Ok(())
    }

    fn build_with_authority(
        self,
        authority: Option<Arc<__private::NodeAuthority>>,
        distributed: Option<Arc<__private::DistributedContext>>,
    ) -> Result<Runtime<S>, BuildError> {
        self.validate()?;
        let mut registrations = HashMap::new();
        for mut registration in self.registrations {
            if registration.mailbox_capacity.is_none() {
                registration.mailbox_capacity = Some(self.mailbox_capacity);
            }
            if registration.idle_timeout.is_none() {
                registration.idle_timeout = Some(self.idle_timeout);
            }
            let name = registration.name;
            let previous = registrations.insert(name, registration);
            debug_assert!(previous.is_none(), "registrations were validated");
        }
        Ok(Runtime {
            inner: Arc::new(__private::RuntimeInner {
                state: Arc::new(self.state),
                registrations,
                actors: Mutex::new(HashMap::new()),
                capacity: Arc::new(Semaphore::new(self.max_active_actors)),
                deactivation_timeout: self.deactivation_timeout,
                next_generation: std::sync::atomic::AtomicU64::new(1),
                status: std::sync::atomic::AtomicU8::new(__private::RUNNING),
                shutdown_timeout: self.shutdown_timeout,
                peer_protocol_version: self.peer_protocol_version,
                authority,
                distributed,
            }),
            distributed: None,
        })
    }
}

pub struct Runtime<S> {
    pub(crate) inner: Arc<__private::RuntimeInner<S>>,
    distributed: Option<__private::DistributedTasks>,
}

impl<S> Runtime<S>
where
    S: Send + Sync + 'static,
{
    pub fn actor_ref<A>(&self, actor_id: ActorId) -> Result<A::Ref, ActorRefError>
    where
        A: __private::ActorType<S>,
    {
        if !self.inner.registrations.contains_key(A::NAME) {
            return Err(ActorRefError::ActorTypeNotRegistered(A::NAME));
        }
        Ok(A::make_ref(__private::ActorRef {
            target: __private::ActorRefTarget::Local(Arc::downgrade(&self.inner)),
            address: ActorAddress::new(A::NAME, actor_id),
        }))
    }

    #[doc(hidden)]
    pub fn test_remote_actor_ref<A>(
        &self,
        actor_id: ActorId,
        endpoint: impl Into<String>,
    ) -> Result<A::Ref, ActorRefError>
    where
        A: __private::ActorType<S>,
    {
        if !self.inner.registrations.contains_key(A::NAME) {
            return Err(ActorRefError::ActorTypeNotRegistered(A::NAME));
        }
        Ok(A::make_ref(__private::ActorRef {
            target: __private::ActorRefTarget::Remote {
                endpoint: endpoint.into(),
                protocol_version: self.inner.peer_protocol_version,
            },
            address: ActorAddress::new(A::NAME, actor_id),
        }))
    }

    #[doc(hidden)]
    pub async fn serve_test_peer(&self, address: SocketAddr) -> std::io::Result<TestPeerServer> {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let peer = self.spawn_peer(listener);
        Ok(TestPeerServer {
            endpoint,
            shutdown: Some(peer.shutdown),
            task: peer.task,
        })
    }

    fn spawn_peer(&self, listener: tokio::net::TcpListener) -> __private::PeerTask {
        __private::spawn_peer(self.inner.clone(), listener)
    }

    fn with_distributed_tasks(
        mut self,
        peer: __private::PeerTask,
        renewal: __private::RenewalTask,
        termination: watch::Receiver<Option<RuntimeTermination>>,
    ) -> Self {
        self.distributed = Some(__private::DistributedTasks {
            peer,
            renewal,
            termination,
        });
        self
    }

    pub fn supervision(&self) -> Option<RuntimeSupervision> {
        self.distributed.as_ref().map(|tasks| RuntimeSupervision {
            receiver: tasks.termination.clone(),
        })
    }

    pub async fn shutdown(mut self) {
        self.inner.shutdown().await;
        if let Some(tasks) = self.distributed.take() {
            tasks.shutdown().await;
        }
    }
}

#[doc(hidden)]
pub struct TestPeerServer {
    endpoint: String,
    shutdown: Option<oneshot::Sender<()>>,
    task: tokio::task::JoinHandle<()>,
}

impl TestPeerServer {
    pub fn endpoint(&self) -> String {
        self.endpoint.clone()
    }

    pub async fn shutdown(mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        let _ = self.task.await;
    }
}

#[doc(hidden)]
pub mod __private {
    use super::*;
    use crate::node_authority::{confirm_node_lease, wall_time_millis};
    use std::{
        future::Future,
        marker::PhantomData,
        pin::Pin,
        sync::atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    };
    use tokio::sync::OwnedSemaphorePermit;

    pub use futures_util::FutureExt;
    pub use prost;
    pub use tokio;

    pub const RUNNING: u8 = 0;
    const SHUTTING_DOWN: u8 = 1;
    const STOPPED: u8 = 2;
    const FENCED: u8 = 3;

    pub struct NodeAuthority {
        valid: AtomicBool,
        deadline: Mutex<tokio::time::Instant>,
        ttl: Duration,
        termination: watch::Sender<Option<RuntimeTermination>>,
    }

    impl NodeAuthority {
        pub fn new(
            operation_started: tokio::time::Instant,
            ttl: Duration,
            termination: watch::Sender<Option<RuntimeTermination>>,
        ) -> Self {
            Self {
                valid: AtomicBool::new(true),
                deadline: Mutex::new(operation_started + ttl),
                ttl,
                termination,
            }
        }

        pub fn is_valid(&self) -> bool {
            self.valid.load(Ordering::Acquire)
                && tokio::time::Instant::now() < *self.deadline.lock()
        }

        fn renew(&self, operation_started: tokio::time::Instant) {
            *self.deadline.lock() = operation_started + self.ttl;
        }

        fn remaining(&self) -> Option<Duration> {
            self.deadline
                .lock()
                .checked_duration_since(tokio::time::Instant::now())
        }

        fn deadline(&self) -> tokio::time::Instant {
            *self.deadline.lock()
        }

        fn fence(&self) {
            if self.valid.swap(false, Ordering::AcqRel) {
                let _ = self.termination.send(Some(RuntimeTermination {
                    reason: RuntimeTerminationReason::Fenced,
                }));
            }
        }
    }

    pub struct PeerTask {
        pub shutdown: oneshot::Sender<()>,
        pub task: tokio::task::JoinHandle<()>,
    }

    pub struct RenewalTask {
        shutdown: oneshot::Sender<()>,
        task: tokio::task::JoinHandle<RenewalExit>,
    }

    struct RenewalExit {
        storage: Arc<dyn OwnershipStorage>,
        session_id: NodeSessionId,
        etag: String,
        release: bool,
    }

    pub struct DistributedTasks {
        pub peer: PeerTask,
        pub renewal: RenewalTask,
        pub termination: watch::Receiver<Option<RuntimeTermination>>,
    }

    impl DistributedTasks {
        pub async fn shutdown(self) {
            let _ = self.renewal.shutdown.send(());
            let _ = self.peer.shutdown.send(());
            if let Ok(exit) = self.renewal.task.await {
                if exit.release {
                    let _ = exit
                        .storage
                        .release_node_lease(&exit.session_id, &exit.etag)
                        .await;
                }
            }
            let _ = self.peer.task.await;
        }
    }

    pub fn spawn_peer<S>(
        runtime: Arc<RuntimeInner<S>>,
        listener: tokio::net::TcpListener,
    ) -> PeerTask
    where
        S: Send + Sync + 'static,
    {
        let (shutdown, shutdown_receiver) = oneshot::channel();
        let service = PeerService { runtime };
        let task = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(peer_protocol::peer_server::PeerServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let _ = shutdown_receiver.await;
                })
                .await;
        });
        PeerTask { shutdown, task }
    }

    pub fn spawn_lease_renewal<S>(
        runtime: Arc<RuntimeInner<S>>,
        authority: Arc<NodeAuthority>,
        storage: Arc<dyn OwnershipStorage>,
        mut lease: NodeLease,
        mut etag: String,
        timing: LeaseTiming,
    ) -> RenewalTask
    where
        S: Send + Sync + 'static,
    {
        let (shutdown, mut shutdown_receiver) = oneshot::channel();
        let task = tokio::spawn(async move {
            loop {
                let renewal_due = tokio::time::Instant::now() + timing.renewal_interval;
                let wake_at = renewal_due.min(authority.deadline());
                tokio::select! {
                    _ = tokio::time::sleep_until(wake_at) => {}
                    _ = &mut shutdown_receiver => return RenewalExit {
                        storage,
                        session_id: lease.session_id,
                        etag,
                        release: true,
                    },
                }
                if !authority.is_valid() {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        storage,
                        session_id: lease.session_id,
                        etag,
                        release: false,
                    };
                }
                let operation_started = tokio::time::Instant::now();
                lease.expires_at_unix_ms =
                    wall_time_millis().saturating_add(timing.ttl.as_millis() as u64);
                let Some(remaining) = authority.remaining() else {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        storage,
                        session_id: lease.session_id,
                        etag,
                        release: false,
                    };
                };
                let outcome = tokio::time::timeout(
                    timing.operation_timeout.min(remaining),
                    storage.renew_node_lease(lease.clone(), &etag),
                )
                .await;
                match outcome {
                    Ok(Ok(LeaseMutation::Applied { etag: next })) => {
                        etag = next;
                        authority.renew(operation_started);
                    }
                    Ok(Ok(LeaseMutation::Ambiguous(_))) => {
                        let Some(next) = confirm_node_lease(
                            storage.as_ref(),
                            &lease,
                            authority.deadline(),
                            timing.operation_timeout,
                        )
                        .await
                        else {
                            authority.fence();
                            runtime.fence().await;
                            return RenewalExit {
                                storage,
                                session_id: lease.session_id,
                                etag,
                                release: false,
                            };
                        };
                        etag = next;
                        authority.renew(operation_started);
                    }
                    Ok(Ok(LeaseMutation::ConditionalRejected)) => {
                        authority.fence();
                        runtime.fence().await;
                        return RenewalExit {
                            storage,
                            session_id: lease.session_id,
                            etag,
                            release: false,
                        };
                    }
                    _ if !authority.is_valid() => {
                        authority.fence();
                        runtime.fence().await;
                        return RenewalExit {
                            storage,
                            session_id: lease.session_id,
                            etag,
                            release: false,
                        };
                    }
                    _ => {}
                }
            }
        });
        RenewalTask { shutdown, task }
    }

    pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

    pub trait ErasedCommand<S>: Send + 'static {
        fn execute<'a>(
            self: Box<Self>,
            actor: &'a mut (dyn Any + Send),
            context: CommandContext,
        ) -> BoxFuture<'a, CommandOutcome>;

        fn fail(self: Box<Self>, error: RuntimeError);
    }

    type Command<S> = Box<dyn ErasedCommand<S>>;
    type CommandSender<S> = mpsc::Sender<Command<S>>;
    type Activate = for<'a> fn(&'a mut (dyn Any + Send)) -> BoxFuture<'a, Result<(), String>>;
    type Deactivate = for<'a> fn(&'a mut (dyn Any + Send), DeactivationReason) -> BoxFuture<'a, ()>;

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
        RemoteProtocol,
        ActorTypeNotRegistered,
        CommandNotRegistered,
        MalformedPayload,
    }

    impl RuntimeError {
        fn to_wire(self) -> i32 {
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
            }) as i32
        }

        fn from_wire(value: i32) -> Self {
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
                RuntimeFailure::ProtocolMismatch | RuntimeFailure::Unspecified => {
                    Self::RemoteProtocol
                }
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
        create: fn(ActorId, Arc<S>) -> Box<dyn Any + Send>,
        activate: Activate,
        deactivate: Deactivate,
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

    pub struct RuntimeInner<S> {
        pub state: Arc<S>,
        pub registrations: HashMap<&'static str, Registration<S>>,
        pub actors: Mutex<HashMap<ActorAddress, Route<S>>>,
        pub capacity: Arc<Semaphore>,
        pub deactivation_timeout: Duration,
        pub next_generation: AtomicU64,
        pub status: AtomicU8,
        pub shutdown_timeout: Duration,
        pub peer_protocol_version: u32,
        pub authority: Option<Arc<NodeAuthority>>,
        pub distributed: Option<Arc<DistributedContext>>,
    }

    pub struct DistributedContext {
        storage: Arc<dyn OwnershipStorage>,
        node_id: String,
        session_id: NodeSessionId,
        operation_timeout: Duration,
        resolutions: tokio::sync::Mutex<HashMap<ActorAddress, Arc<tokio::sync::Mutex<()>>>>,
        resolved: tokio::sync::Mutex<HashMap<ActorAddress, CachedOwner>>,
    }

    impl DistributedContext {
        pub fn new(
            storage: Arc<dyn OwnershipStorage>,
            node_id: String,
            session_id: NodeSessionId,
            operation_timeout: Duration,
        ) -> Arc<Self> {
            Arc::new(Self {
                storage,
                node_id,
                session_id,
                operation_timeout,
                resolutions: tokio::sync::Mutex::new(HashMap::new()),
                resolved: tokio::sync::Mutex::new(HashMap::new()),
            })
        }

        async fn resolution_lock(&self, address: &ActorAddress) -> Arc<tokio::sync::Mutex<()>> {
            let mut resolutions = self.resolutions.lock().await;
            resolutions
                .entry(address.clone())
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
                .clone()
        }

        fn is_local_owner(&self, record: &ActorOwnerRecord) -> bool {
            record.owner.as_ref().is_some_and(|owner| {
                owner.node_id == self.node_id && owner.session_id == self.session_id
            })
        }

        fn local_claim(&self, epoch: u64) -> ActorOwnerRecord {
            ActorOwnerRecord {
                owner: Some(ActorOwner {
                    node_id: self.node_id.clone(),
                    session_id: self.session_id.clone(),
                }),
                ownership_epoch: epoch,
            }
        }

        async fn resolve(
            &self,
            address: &ActorAddress,
            capacity: &Arc<Semaphore>,
        ) -> Result<ResolvedOwner, RuntimeError> {
            let lock = self.resolution_lock(address).await;
            let guard = lock.lock_owned().await;
            if let Some(cached) = self.resolved.lock().await.get(address).cloned() {
                return Ok(match cached {
                    CachedOwner::Local => ResolvedOwner::Local {
                        reservation: None,
                        guard,
                    },
                    CachedOwner::Remote {
                        endpoint,
                        protocol_version,
                    } => ResolvedOwner::Remote {
                        endpoint,
                        protocol_version,
                    },
                });
            }
            for _ in 0..3 {
                let current = tokio::time::timeout(
                    self.operation_timeout,
                    self.storage.read_actor_owner(address),
                )
                .await
                .map_err(|_| RuntimeError::RemoteUnavailable)?
                .map_err(|_| RuntimeError::RemoteUnavailable)?;
                if let Some(current) = current.as_ref() {
                    if self.is_local_owner(&current.record) {
                        self.resolved
                            .lock()
                            .await
                            .insert(address.clone(), CachedOwner::Local);
                        return Ok(ResolvedOwner::Local {
                            reservation: None,
                            guard,
                        });
                    }
                    if let Some(owner) = &current.record.owner {
                        let lease = tokio::time::timeout(
                            self.operation_timeout,
                            self.storage.read_node_lease(&owner.session_id),
                        )
                        .await
                        .map_err(|_| RuntimeError::RemoteUnavailable)?
                        .map_err(|_| RuntimeError::RemoteUnavailable)?
                        .ok_or(RuntimeError::RemoteUnavailable)?;
                        let endpoint = format!("http://{}", lease.lease.advertised_address);
                        let protocol_version = lease.lease.protocol_version;
                        self.resolved.lock().await.insert(
                            address.clone(),
                            CachedOwner::Remote {
                                endpoint: endpoint.clone(),
                                protocol_version,
                            },
                        );
                        return Ok(ResolvedOwner::Remote {
                            endpoint,
                            protocol_version,
                        });
                    }
                }

                let epoch = current.as_ref().map_or(1, |current| {
                    current.record.ownership_epoch.saturating_add(1)
                });
                let expected = self.local_claim(epoch);
                let etag = current.as_ref().map(|current| current.etag.as_str());
                let reservation = capacity
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| RuntimeError::RuntimeAtCapacity)?;
                let mutation = tokio::time::timeout(
                    self.operation_timeout,
                    self.storage
                        .claim_actor_owner(address, expected.clone(), etag),
                )
                .await
                .map_err(|_| RuntimeError::RemoteUnavailable)?
                .map_err(|_| RuntimeError::RemoteUnavailable)?;
                match mutation {
                    LeaseMutation::Applied { .. } => {
                        self.resolved
                            .lock()
                            .await
                            .insert(address.clone(), CachedOwner::Local);
                        return Ok(ResolvedOwner::Local {
                            reservation: Some(reservation),
                            guard,
                        });
                    }
                    LeaseMutation::ConditionalRejected => {
                        drop(reservation);
                        continue;
                    }
                    LeaseMutation::Ambiguous(_) => {
                        let mut should_reresolve = false;
                        for _ in 0..3 {
                            let confirmed = tokio::time::timeout(
                                self.operation_timeout,
                                self.storage.read_actor_owner(address),
                            )
                            .await;
                            if let Ok(Ok(Some(confirmed))) = confirmed {
                                if confirmed.record == expected {
                                    self.resolved
                                        .lock()
                                        .await
                                        .insert(address.clone(), CachedOwner::Local);
                                    return Ok(ResolvedOwner::Local {
                                        reservation: Some(reservation),
                                        guard,
                                    });
                                }
                                if confirmed.record.owner.is_some() {
                                    should_reresolve = true;
                                    break;
                                }
                            }
                        }
                        if should_reresolve {
                            continue;
                        }
                        return Err(RuntimeError::RemoteUnavailable);
                    }
                }
            }
            Err(RuntimeError::RemoteUnavailable)
        }

        async fn resolve_local(
            &self,
            address: &ActorAddress,
            capacity: &Arc<Semaphore>,
        ) -> Result<LocalResolution, RuntimeError> {
            match self.resolve(address, capacity).await? {
                ResolvedOwner::Local { reservation, guard } => Ok(LocalResolution {
                    reservation,
                    guard: Some(guard),
                }),
                ResolvedOwner::Remote { .. } => Err(RuntimeError::RemoteUnavailable),
            }
        }
    }

    enum ResolvedOwner {
        Local {
            reservation: Option<OwnedSemaphorePermit>,
            guard: tokio::sync::OwnedMutexGuard<()>,
        },
        Remote {
            endpoint: String,
            protocol_version: u32,
        },
    }

    struct LocalResolution {
        reservation: Option<OwnedSemaphorePermit>,
        guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    }

    #[derive(Clone)]
    enum CachedOwner {
        Local,
        Remote {
            endpoint: String,
            protocol_version: u32,
        },
    }

    pub struct Route<S> {
        generation: u64,
        state: RouteState<S>,
        _capacity: OwnedSemaphorePermit,
        shutdown: watch::Sender<bool>,
        abort: tokio::task::AbortHandle,
        completed: watch::Receiver<bool>,
    }

    enum RouteState<S> {
        Active(CommandSender<S>),
        Deactivating,
    }

    pub struct ActorRef<S> {
        pub target: ActorRefTarget<S>,
        pub address: ActorAddress,
    }

    pub enum ActorRefTarget<S> {
        Local(Weak<RuntimeInner<S>>),
        Remote {
            endpoint: String,
            protocol_version: u32,
        },
    }

    impl<S> Clone for ActorRef<S> {
        fn clone(&self) -> Self {
            Self {
                target: match &self.target {
                    ActorRefTarget::Local(runtime) => ActorRefTarget::Local(runtime.clone()),
                    ActorRefTarget::Remote {
                        endpoint,
                        protocol_version,
                    } => ActorRefTarget::Remote {
                        endpoint: endpoint.clone(),
                        protocol_version: *protocol_version,
                    },
                },
                address: self.address.clone(),
            }
        }
    }

    impl<S> ActorRef<S>
    where
        S: Send + Sync + 'static,
    {
        pub fn send(&self, command: Command<S>) -> Result<(), RuntimeError> {
            self.send_with_reservation(command, None)
        }

        pub fn send_with_reservation(
            &self,
            mut command: Command<S>,
            reservation: Option<OwnedSemaphorePermit>,
        ) -> Result<(), RuntimeError> {
            let ActorRefTarget::Local(runtime) = &self.target else {
                command.fail(RuntimeError::RemoteUnavailable);
                return Err(RuntimeError::RemoteUnavailable);
            };
            let Some(runtime) = runtime.upgrade() else {
                command.fail(RuntimeError::RuntimeStopped);
                return Err(RuntimeError::RuntimeStopped);
            };

            let mut actors = runtime.actors.lock();
            if runtime
                .authority
                .as_ref()
                .is_some_and(|authority| !authority.is_valid())
            {
                command.fail(RuntimeError::NodeFenced);
                return Err(RuntimeError::NodeFenced);
            }
            match runtime.status.load(Ordering::Acquire) {
                RUNNING => {}
                SHUTTING_DOWN => {
                    command.fail(RuntimeError::RuntimeShuttingDown);
                    return Err(RuntimeError::RuntimeShuttingDown);
                }
                FENCED => {
                    command.fail(RuntimeError::NodeFenced);
                    return Err(RuntimeError::NodeFenced);
                }
                _ => {
                    command.fail(RuntimeError::RuntimeStopped);
                    return Err(RuntimeError::RuntimeStopped);
                }
            }
            if let Some(route) = actors.get(&self.address) {
                match &route.state {
                    RouteState::Active(sender) => match sender.try_send(command) {
                        Ok(()) => return Ok(()),
                        Err(mpsc::error::TrySendError::Full(command)) => {
                            command.fail(RuntimeError::MailboxFull);
                            return Err(RuntimeError::MailboxFull);
                        }
                        Err(mpsc::error::TrySendError::Closed(returned)) => {
                            let generation = route.generation;
                            actors.remove(&self.address);
                            command = returned;
                            tracing::debug!(
                                actor_type = self.address.actor_type(),
                                actor_id = ?self.address.actor_id(),
                                generation,
                                lifecycle = "routing",
                                error_category = "ClosedRouteReplaced",
                                "Replacing a closed Actor route"
                            );
                        }
                    },
                    RouteState::Deactivating => {
                        command.fail(RuntimeError::ActorDeactivating);
                        return Err(RuntimeError::ActorDeactivating);
                    }
                }
            }

            let permit = match reservation {
                Some(permit) => permit,
                None => match runtime.capacity.clone().try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        command.fail(RuntimeError::RuntimeAtCapacity);
                        return Err(RuntimeError::RuntimeAtCapacity);
                    }
                },
            };
            let generation = runtime.next_generation.fetch_add(1, Ordering::Relaxed);
            let spawned = spawn_actor(runtime.clone(), self.address.clone(), generation);
            let result = try_send(&spawned.sender, command);
            actors.insert(
                self.address.clone(),
                Route {
                    generation,
                    state: RouteState::Active(spawned.sender),
                    _capacity: permit,
                    shutdown: spawned.shutdown,
                    abort: spawned.abort,
                    completed: spawned.completed,
                },
            );
            result
        }

        pub fn reply_channel_closed_error<E>(&self) -> SendError<E> {
            let ActorRefTarget::Local(runtime) = &self.target else {
                return SendError::ActorStopped;
            };
            if runtime
                .upgrade()
                .is_some_and(|runtime| runtime.status.load(Ordering::Acquire) == FENCED)
            {
                SendError::NodeFenced
            } else {
                SendError::ActorStopped
            }
        }

        pub fn ensure_reply_authority<E>(&self) -> Result<(), SendError<E>> {
            let ActorRefTarget::Local(runtime) = &self.target else {
                return Ok(());
            };
            let Some(runtime) = runtime.upgrade() else {
                return Err(SendError::RuntimeStopped);
            };
            if runtime.status.load(Ordering::Acquire) == FENCED
                || runtime
                    .authority
                    .as_ref()
                    .is_some_and(|authority| !authority.is_valid())
            {
                Err(SendError::NodeFenced)
            } else {
                Ok(())
            }
        }

        pub async fn invoke_remote(
            &self,
            command: &'static str,
            payload: Vec<u8>,
        ) -> Result<RemotePayload, RuntimeError> {
            let ActorRefTarget::Remote {
                endpoint,
                protocol_version,
            } = &self.target
            else {
                return Err(RuntimeError::RemoteUnavailable);
            };
            let mut client = peer_protocol::peer_client::PeerClient::connect(endpoint.clone())
                .await
                .map_err(|_| RuntimeError::RemoteUnavailable)?;
            let response = client
                .invoke(peer_protocol::InvokeRequest {
                    protocol_version: *protocol_version,
                    actor_type: self.address.actor_type().to_owned(),
                    actor_id: self.address.actor_id().as_bytes().to_vec(),
                    command: command.to_owned(),
                    payload,
                })
                .await
                .map_err(|_| RuntimeError::RemoteUnavailable)?
                .into_inner();
            use peer_protocol::invoke_response::Outcome;
            match response.outcome {
                Some(Outcome::Success(bytes)) => Ok(RemotePayload::Success(bytes)),
                Some(Outcome::HandlerError(bytes)) => Ok(RemotePayload::HandlerError(bytes)),
                Some(Outcome::RuntimeFailure(failure)) => Err(RuntimeError::from_wire(failure)),
                None => Err(RuntimeError::RemoteProtocol),
            }
        }

        pub async fn route_remote_command(
            &self,
            command: &'static str,
            payload: Vec<u8>,
        ) -> Result<RouteDecision, RuntimeError> {
            match &self.target {
                ActorRefTarget::Remote { .. } => self
                    .invoke_remote(command, payload)
                    .await
                    .map(RouteDecision::Remote),
                ActorRefTarget::Local(runtime) => {
                    let runtime = runtime.upgrade().ok_or(RuntimeError::RuntimeStopped)?;
                    let Some(distributed) = &runtime.distributed else {
                        return Ok(RouteDecision::Local {
                            reservation: None,
                            resolution: None,
                        });
                    };
                    match distributed
                        .resolve(&self.address, &runtime.capacity)
                        .await?
                    {
                        ResolvedOwner::Local { reservation, guard } => Ok(RouteDecision::Local {
                            reservation,
                            resolution: Some(guard),
                        }),
                        ResolvedOwner::Remote {
                            endpoint,
                            protocol_version,
                        } => ActorRef::<S> {
                            target: ActorRefTarget::Remote {
                                endpoint,
                                protocol_version,
                            },
                            address: self.address.clone(),
                        }
                        .invoke_remote(command, payload)
                        .await
                        .map(RouteDecision::Remote),
                    }
                }
            }
        }
    }

    pub enum RouteDecision {
        Local {
            reservation: Option<OwnedSemaphorePermit>,
            resolution: Option<tokio::sync::OwnedMutexGuard<()>>,
        },
        Remote(RemotePayload),
    }

    pub enum RemotePayload {
        Success(Vec<u8>),
        HandlerError(Vec<u8>),
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

    pub struct PeerService<S> {
        pub runtime: Arc<RuntimeInner<S>>,
    }

    #[tonic::async_trait]
    impl<S> peer_protocol::peer_server::Peer for PeerService<S>
    where
        S: Send + Sync + 'static,
    {
        async fn invoke(
            &self,
            request: Request<peer_protocol::InvokeRequest>,
        ) -> Result<Response<peer_protocol::InvokeResponse>, Status> {
            let request = request.into_inner();
            let outcome = if request.protocol_version != self.runtime.peer_protocol_version {
                runtime_failure(RuntimeError::RemoteProtocol)
            } else if let Some(registration) =
                self.runtime.registrations.get(request.actor_type.as_str())
            {
                if let Some(factory) = registration.remote_commands.get(request.command.as_str()) {
                    match factory(request.payload) {
                        Ok(invocation) => {
                            let actor_ref = ActorRef {
                                target: ActorRefTarget::Local(Arc::downgrade(&self.runtime)),
                                address: ActorAddress::new(
                                    registration.name,
                                    ActorId::new(request.actor_id),
                                ),
                            };
                            let local_resolution =
                                if let Some(distributed) = &self.runtime.distributed {
                                    distributed
                                        .resolve_local(&actor_ref.address, &self.runtime.capacity)
                                        .await
                                } else {
                                    Ok(LocalResolution {
                                        reservation: None,
                                        guard: None,
                                    })
                                };
                            let local_resolution = match local_resolution {
                                Ok(resolution) => resolution,
                                Err(error) => {
                                    return Ok(Response::new(peer_protocol::InvokeResponse {
                                        outcome: runtime_failure(error),
                                    }));
                                }
                            };
                            if let Err(error) = actor_ref.send_with_reservation(
                                invocation.command,
                                local_resolution.reservation,
                            ) {
                                runtime_failure(error)
                            } else {
                                drop(local_resolution.guard);
                                use peer_protocol::invoke_response::Outcome;
                                let reply = invocation.reply.await;
                                if !self.runtime.has_authority() {
                                    runtime_failure(RuntimeError::NodeFenced)
                                } else {
                                    match reply {
                                        Ok(bytes) => Some(Outcome::Success(bytes)),
                                        Err(RemoteReplyError::Handler(bytes)) => {
                                            Some(Outcome::HandlerError(bytes))
                                        }
                                        Err(RemoteReplyError::Runtime(error)) => {
                                            runtime_failure(error)
                                        }
                                    }
                                }
                            }
                        }
                        Err(error) => runtime_failure(error),
                    }
                } else {
                    runtime_failure(RuntimeError::CommandNotRegistered)
                }
            } else {
                runtime_failure(RuntimeError::ActorTypeNotRegistered)
            };
            Ok(Response::new(peer_protocol::InvokeResponse { outcome }))
        }
    }

    fn runtime_failure(error: RuntimeError) -> Option<peer_protocol::invoke_response::Outcome> {
        Some(peer_protocol::invoke_response::Outcome::RuntimeFailure(
            error.to_wire(),
        ))
    }

    struct Spawned<S> {
        sender: CommandSender<S>,
        shutdown: watch::Sender<bool>,
        abort: tokio::task::AbortHandle,
        completed: watch::Receiver<bool>,
    }

    fn try_send<S: 'static>(
        sender: &CommandSender<S>,
        command: Command<S>,
    ) -> Result<(), RuntimeError> {
        sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(command) => {
                command.fail(RuntimeError::MailboxFull);
                RuntimeError::MailboxFull
            }
            mpsc::error::TrySendError::Closed(command) => {
                command.fail(RuntimeError::ActorStopped);
                RuntimeError::ActorStopped
            }
        })
    }

    fn spawn_actor<S>(
        runtime: Arc<RuntimeInner<S>>,
        address: ActorAddress,
        generation: u64,
    ) -> Spawned<S>
    where
        S: Send + Sync + 'static,
    {
        let registration = runtime
            .registrations
            .get(address.actor_type())
            .expect("Actor Type registration disappeared");
        let mailbox_capacity = registration
            .mailbox_capacity
            .expect("mailbox capacity was not configured");
        let create = registration.create;
        let activate = registration.activate;
        let deactivate = registration.deactivate;
        let idle_timeout = registration
            .idle_timeout
            .expect("idle timeout was not configured");
        let mut actor = create(address.actor_id().clone(), runtime.state.clone());
        let (sender, mut receiver) = mpsc::channel::<Command<S>>(mailbox_capacity);
        let task_sender = sender.clone();
        let (shutdown, mut shutdown_receiver) = watch::channel(false);
        let (completion_sender, completed) = watch::channel(false);
        let task = tokio::spawn(async move {
            let _task_guard = ActorTaskGuard {
                runtime: runtime.clone(),
                address: address.clone(),
                generation,
                completion: completion_sender,
            };
            tracing::debug!(
                actor_type = address.actor_type(),
                actor_id = ?address.actor_id(),
                lifecycle = "activation",
                error_category = "None",
                "Actor activation started"
            );
            match std::panic::AssertUnwindSafe(activate(actor.as_mut()))
                .catch_unwind()
                .await
            {
                Ok(Ok(())) => {
                    tracing::debug!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        lifecycle = "activation",
                        error_category = "None",
                        "Actor activation completed"
                    );
                }
                Ok(Err(error)) => {
                    tracing::error!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        lifecycle = "activation",
                        error_category = "ActivationFailed",
                        error = %error,
                        "Actor activation failed"
                    );
                    receiver.close();
                    remove_route(&runtime, &address, generation);
                    while let Ok(command) = receiver.try_recv() {
                        command.fail(RuntimeError::ActivationFailed);
                    }
                    return;
                }
                Err(_) => {
                    tracing::error!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        lifecycle = "activation",
                        error_category = "ActorStopped",
                        "Actor activation panicked"
                    );
                    receiver.close();
                    remove_route(&runtime, &address, generation);
                    while let Ok(command) = receiver.try_recv() {
                        command.fail(RuntimeError::ActorStopped);
                    }
                    return;
                }
            }
            loop {
                let command = tokio::select! {
                    biased;
                    changed = shutdown_receiver.changed() => {
                        if changed.is_ok() && *shutdown_receiver.borrow() {
                            receiver.close();
                            while let Some(command) = receiver.recv().await {
                                if !execute_command(
                                    command,
                                    actor.as_mut(),
                                    &runtime,
                                    &address,
                                    generation,
                                    &mut receiver,
                                ).await {
                                    return;
                                }
                            }
                            tracing::debug!(
                                actor_type = address.actor_type(),
                                actor_id = ?address.actor_id(),
                                lifecycle = "deactivation",
                                error_category = "None",
                                reason = "Shutdown",
                                "Actor shutdown deactivation started"
                            );
                            match std::panic::AssertUnwindSafe(deactivate(
                                actor.as_mut(),
                                DeactivationReason::Shutdown,
                            ))
                            .catch_unwind()
                            .await
                            {
                                Ok(()) => tracing::debug!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "None",
                                    reason = "Shutdown",
                                    "Actor shutdown deactivation completed"
                                ),
                                Err(_) => tracing::error!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "ActorStopped",
                                    reason = "Shutdown",
                                    "Actor shutdown deactivation panicked"
                                ),
                            }
                            remove_route(&runtime, &address, generation);
                            return;
                        }
                        continue;
                    }
                    received = tokio::time::timeout(idle_timeout, receiver.recv()) => {
                        match received {
                            Ok(Some(command)) => command,
                            Ok(None) => {
                                remove_route(&runtime, &address, generation);
                                return;
                            }
                            Err(_) => {
                                if !begin_deactivation(&runtime, &address, generation, &task_sender) {
                                    continue;
                                }
                                tracing::debug!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "None",
                                    reason = "Idle",
                                    "Actor idle deactivation started"
                                );
                                match tokio::time::timeout(
                                    runtime.deactivation_timeout,
                                    std::panic::AssertUnwindSafe(deactivate(
                                        actor.as_mut(),
                                        DeactivationReason::Idle,
                                    ))
                                    .catch_unwind(),
                                )
                                .await
                                {
                                    Err(_) => tracing::warn!(
                                        actor_type = address.actor_type(),
                                        actor_id = ?address.actor_id(),
                                        lifecycle = "deactivation",
                                        error_category = "DeactivationTimedOut",
                                        "Actor deactivation timed out"
                                    ),
                                    Ok(Ok(())) => tracing::debug!(
                                        actor_type = address.actor_type(),
                                        actor_id = ?address.actor_id(),
                                        lifecycle = "deactivation",
                                        error_category = "None",
                                        reason = "Idle",
                                        "Actor idle deactivation completed"
                                    ),
                                    Ok(Err(_)) => tracing::error!(
                                        actor_type = address.actor_type(),
                                        actor_id = ?address.actor_id(),
                                        lifecycle = "deactivation",
                                        error_category = "ActorStopped",
                                        reason = "Idle",
                                        "Actor idle deactivation panicked"
                                    ),
                                }
                                remove_route(&runtime, &address, generation);
                                return;
                            }
                        }
                    }
                };
                if !execute_command(
                    command,
                    actor.as_mut(),
                    &runtime,
                    &address,
                    generation,
                    &mut receiver,
                )
                .await
                {
                    return;
                }
            }
        });
        Spawned {
            sender,
            shutdown,
            abort: task.abort_handle(),
            completed,
        }
    }

    struct ActorTaskGuard<S> {
        runtime: Arc<RuntimeInner<S>>,
        address: ActorAddress,
        generation: u64,
        completion: watch::Sender<bool>,
    }

    impl<S> Drop for ActorTaskGuard<S> {
        fn drop(&mut self) {
            remove_route(&self.runtime, &self.address, self.generation);
            let _ = self.completion.send(true);
        }
    }

    async fn execute_command<S>(
        command: Command<S>,
        actor: &mut (dyn Any + Send),
        runtime: &RuntimeInner<S>,
        address: &ActorAddress,
        generation: u64,
        receiver: &mut mpsc::Receiver<Command<S>>,
    ) -> bool
    where
        S: Send + Sync + 'static,
    {
        let context = CommandContext {
            address: address.clone(),
        };
        let CommandOutcome::Panicked(fail_current) = command.execute(actor, context).await else {
            if runtime
                .authority
                .as_ref()
                .is_some_and(|authority| !authority.is_valid())
            {
                receiver.close();
                remove_route(runtime, address, generation);
                while let Ok(command) = receiver.try_recv() {
                    command.fail(RuntimeError::NodeFenced);
                }
                return false;
            }
            return true;
        };

        tracing::error!(
            actor_type = address.actor_type(),
            actor_id = ?address.actor_id(),
            lifecycle = "command",
            error_category = "ActorStopped",
            "Actor command handler panicked"
        );
        receiver.close();
        remove_route(runtime, address, generation);
        while let Ok(command) = receiver.try_recv() {
            command.fail(RuntimeError::ActorStopped);
        }
        fail_current();
        false
    }

    fn begin_deactivation<S>(
        runtime: &RuntimeInner<S>,
        address: &ActorAddress,
        generation: u64,
        sender: &CommandSender<S>,
    ) -> bool {
        let mut actors = runtime.actors.lock();
        let Some(route) = actors.get_mut(address) else {
            return false;
        };
        if route.generation != generation || sender.capacity() != sender.max_capacity() {
            return false;
        }
        route.state = RouteState::Deactivating;
        true
    }

    fn remove_route<S>(runtime: &RuntimeInner<S>, address: &ActorAddress, generation: u64) {
        let mut actors = runtime.actors.lock();
        if actors
            .get(address)
            .is_some_and(|route| route.generation == generation)
        {
            actors.remove(address);
        }
    }

    impl<S> RuntimeInner<S>
    where
        S: Send + Sync + 'static,
    {
        fn has_authority(&self) -> bool {
            self.status.load(Ordering::Acquire) != FENCED
                && self
                    .authority
                    .as_ref()
                    .is_none_or(|authority| authority.is_valid())
        }

        pub async fn shutdown(self: &Arc<Self>) {
            tracing::debug!(
                lifecycle = "shutdown",
                error_category = "None",
                "CoActor runtime shutdown started"
            );
            let (mut completions, aborts) = {
                let actors = self.actors.lock();
                if self
                    .status
                    .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::AcqRel, Ordering::Acquire)
                    .is_err()
                {
                    return;
                }

                let mut completions = Vec::with_capacity(actors.len());
                let mut aborts = Vec::with_capacity(actors.len());
                for route in actors.values() {
                    let _ = route.shutdown.send(true);
                    completions.push(route.completed.clone());
                    aborts.push(route.abort.clone());
                }
                (completions, aborts)
            };

            let wait = async {
                for completion in &mut completions {
                    if !*completion.borrow() {
                        let _ = completion.wait_for(|completed| *completed).await;
                    }
                }
            };
            if tokio::time::timeout(self.shutdown_timeout, wait)
                .await
                .is_err()
            {
                tracing::warn!(
                    lifecycle = "shutdown",
                    error_category = "ShutdownTimedOut",
                    "CoActor runtime shutdown timed out"
                );
                for abort in aborts {
                    abort.abort();
                }
                tokio::task::yield_now().await;
                self.actors.lock().clear();
            }
            self.status.store(STOPPED, Ordering::Release);
            tracing::debug!(
                lifecycle = "shutdown",
                error_category = "None",
                "CoActor runtime shutdown completed"
            );
        }

        pub async fn fence(self: &Arc<Self>) {
            let (completions, aborts) = {
                let actors = self.actors.lock();
                self.status.store(FENCED, Ordering::Release);
                let mut completions = Vec::with_capacity(actors.len());
                let mut aborts = Vec::with_capacity(actors.len());
                for route in actors.values() {
                    route.abort.abort();
                    completions.push(route.completed.clone());
                    aborts.push(route.abort.clone());
                }
                (completions, aborts)
            };
            for abort in aborts {
                abort.abort();
            }
            tokio::task::yield_now().await;
            for mut completion in completions {
                if !*completion.borrow() {
                    let _ = completion.wait_for(|completed| *completed).await;
                }
            }
            self.actors.lock().clear();
        }
    }
}
