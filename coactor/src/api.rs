use super::*;

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

    pub(crate) fn build_with_authority(
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

    pub(crate) fn spawn_peer(&self, listener: tokio::net::TcpListener) -> __private::PeerTask {
        __private::spawn_peer(self.inner.clone(), listener)
    }

    pub(crate) fn with_distributed_tasks(
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
pub mod testing {
    use super::*;

    pub fn with_peer_protocol_version<S>(
        mut builder: RuntimeBuilder<S>,
        version: u32,
    ) -> RuntimeBuilder<S> {
        builder.peer_protocol_version = version;
        builder
    }

    pub fn remote_actor_ref<S, A>(
        runtime: &Runtime<S>,
        actor_id: ActorId,
        endpoint: impl Into<String>,
    ) -> Result<A::Ref, ActorRefError>
    where
        S: Send + Sync + 'static,
        A: __private::ActorType<S>,
    {
        if !runtime.inner.registrations.contains_key(A::NAME) {
            return Err(ActorRefError::ActorTypeNotRegistered(A::NAME));
        }
        Ok(A::make_ref(__private::ActorRef {
            target: __private::ActorRefTarget::Remote {
                endpoint: endpoint.into(),
                protocol_version: runtime.inner.peer_protocol_version,
            },
            address: ActorAddress::new(A::NAME, actor_id),
        }))
    }

    pub async fn serve_peer<S>(
        runtime: &Runtime<S>,
        address: SocketAddr,
    ) -> std::io::Result<PeerServer>
    where
        S: Send + Sync + 'static,
    {
        let listener = tokio::net::TcpListener::bind(address).await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        let peer = __private::spawn_peer(runtime.inner.clone(), listener);
        Ok(PeerServer {
            endpoint,
            shutdown: Some(peer.shutdown),
            task: peer.task,
        })
    }

    pub struct PeerServer {
        endpoint: String,
        shutdown: Option<oneshot::Sender<()>>,
        task: tokio::task::JoinHandle<()>,
    }

    impl PeerServer {
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
}
