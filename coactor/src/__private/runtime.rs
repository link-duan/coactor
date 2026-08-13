use super::*;

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
    OwnershipUnavailable,
    OutcomeUnknown,
    NotOwner,
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
            Self::OwnershipUnavailable => RuntimeFailure::OwnershipUnavailable,
            Self::OutcomeUnknown => {
                unreachable!("unknown outcomes are classified only by the caller")
            }
            Self::NotOwner => RuntimeFailure::NotOwner,
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
    peer_connect_timeout: Duration,
    resolutions: tokio::sync::Mutex<HashMap<ActorAddress, Arc<tokio::sync::Mutex<()>>>>,
    resolved: tokio::sync::Mutex<HashMap<ActorAddress, CachedOwner>>,
}

impl DistributedContext {
    pub fn new(
        storage: Arc<dyn OwnershipStorage>,
        node_id: String,
        session_id: NodeSessionId,
        operation_timeout: Duration,
        peer_connect_timeout: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            storage,
            node_id,
            session_id,
            operation_timeout,
            peer_connect_timeout,
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
            .map_err(|_| RuntimeError::OwnershipUnavailable)?
            .map_err(|_| RuntimeError::OwnershipUnavailable)?;
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
                    .map_err(|_| RuntimeError::OwnershipUnavailable)?
                    .map_err(|_| RuntimeError::OwnershipUnavailable)?;
                    if let Some(lease) = lease {
                        if lease.lease.expires_at_unix_ms > wall_time_millis() {
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
                    tracing::info!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        prior_epoch = current.record.ownership_epoch,
                        lifecycle = "availability_failover",
                        "Actor Owner Node Lease is absent or expired; attempting empty-state takeover"
                    );
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
            .map_err(|_| RuntimeError::OwnershipUnavailable)?
            .map_err(|_| RuntimeError::OwnershipUnavailable)?;
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
                    return Err(RuntimeError::OwnershipUnavailable);
                }
            }
        }
        Err(RuntimeError::OwnershipUnavailable)
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
            ResolvedOwner::Remote { .. } => Err(RuntimeError::NotOwner),
        }
    }

    async fn invalidate(&self, address: &ActorAddress) {
        self.resolved.lock().await.remove(address);
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

    async fn invoke_endpoint(
        &self,
        endpoint: String,
        protocol_version: u32,
        command: &'static str,
        payload: Vec<u8>,
        connect_timeout: Option<Duration>,
    ) -> Result<RemotePayload, RuntimeError> {
        let connect = peer_protocol::peer_client::PeerClient::connect(endpoint);
        let mut client = match connect_timeout {
            Some(timeout) => tokio::time::timeout(timeout, connect)
                .await
                .map_err(|_| RuntimeError::RemoteUnavailable)?
                .map_err(|_| RuntimeError::RemoteUnavailable)?,
            None => connect.await.map_err(|_| RuntimeError::RemoteUnavailable)?,
        };
        let response = client
            .invoke(peer_protocol::InvokeRequest {
                protocol_version,
                actor_type: self.address.actor_type().to_owned(),
                actor_id: self.address.actor_id().as_bytes().to_vec(),
                command: command.to_owned(),
                payload,
            })
            .await
            .map_err(|_| RuntimeError::OutcomeUnknown)?
            .into_inner();
        use peer_protocol::invoke_response::Outcome;
        match response.outcome {
            Some(Outcome::Success(bytes)) => Ok(RemotePayload::Success(bytes)),
            Some(Outcome::HandlerError(bytes)) => Ok(RemotePayload::HandlerError(bytes)),
            Some(Outcome::RuntimeFailure(failure)) => Err(RuntimeError::from_wire(failure)),
            None => Err(RuntimeError::RemoteProtocol),
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
        self.invoke_endpoint(endpoint.clone(), *protocol_version, command, payload, None)
            .await
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
                for attempt in 0..=1 {
                    match distributed
                        .resolve(&self.address, &runtime.capacity)
                        .await?
                    {
                        ResolvedOwner::Local { reservation, guard } => {
                            return Ok(RouteDecision::Local {
                                reservation,
                                resolution: Some(guard),
                            });
                        }
                        ResolvedOwner::Remote {
                            endpoint,
                            protocol_version,
                        } => match self
                            .invoke_endpoint(
                                endpoint,
                                protocol_version,
                                command,
                                payload.clone(),
                                Some(distributed.peer_connect_timeout),
                            )
                            .await
                        {
                            Ok(remote) => return Ok(RouteDecision::Remote(remote)),
                            Err(RuntimeError::RemoteUnavailable | RuntimeError::NotOwner)
                                if attempt == 0 =>
                            {
                                distributed.invalidate(&self.address).await;
                            }
                            Err(error) => return Err(error),
                        },
                    }
                }
                Err(RuntimeError::RemoteUnavailable)
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
                        let local_resolution = if let Some(distributed) = &self.runtime.distributed
                        {
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
                        if let Err(error) = actor_ref
                            .send_with_reservation(invocation.command, local_resolution.reservation)
                        {
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
                                    Err(RemoteReplyError::Runtime(error)) => runtime_failure(error),
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
