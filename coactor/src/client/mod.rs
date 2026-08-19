//! Caller runtime: resolves Actor ownership and opens direct Sessions to Servers.

pub(crate) mod session;

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use self::session::Session;
use crate::cluster::default_placement;
use crate::transport::grpc::GrpcTransport;
use crate::transport::{ClientTransport, Endpoint, TransportSender};
use crate::transport_protocol::{Envelope, envelope, session_opened_ack};
use crate::{
    ActorAddress, ActorOwnerReader, ClientBuildError, NodeDirectory, NodeSessionId, OpenError,
    PlacementCandidate, PlacementContext, PlacementStrategy, SendError, TRANSPORT_PROTOCOL_VERSION,
};

pub(crate) const SESSION_RECEIVER_CAPACITY: usize = 64;
const DEFAULT_OPEN_TIMEOUT: Duration = Duration::from_secs(3);
const DEFAULT_MAX_OPEN_ATTEMPTS: usize = 3;
const DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT: usize = 4;
const DEFAULT_TRANSPORT_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
const PLACEMENT_CACHE_TTL: Duration = Duration::from_secs(3);

pub(crate) const RUNNING: u8 = 0;
pub(crate) const STOPPED: u8 = 1;

/// Builds a caller-only [`Client`] from read-only Coordination capabilities.
pub struct ClientBuilder<C> {
    transport: Arc<dyn ClientTransport>,
    coordination: C,
    open_timeout: Duration,
    transport_connect_timeout: Duration,
    max_open_attempts: usize,
    max_connections_per_endpoint: usize,
    placement: Arc<dyn PlacementStrategy>,
}

impl Client {
    /// Creates a Client builder without performing Coordination I/O.
    pub fn builder<C>(coordination: C) -> ClientBuilder<C>
    where
        C: NodeDirectory + ActorOwnerReader,
    {
        ClientBuilder {
            transport: Arc::new(GrpcTransport::new(DEFAULT_TRANSPORT_CONNECT_TIMEOUT)),
            coordination,
            open_timeout: DEFAULT_OPEN_TIMEOUT,
            transport_connect_timeout: DEFAULT_TRANSPORT_CONNECT_TIMEOUT,
            max_open_attempts: DEFAULT_MAX_OPEN_ATTEMPTS,
            max_connections_per_endpoint: DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT,
            placement: default_placement(),
        }
    }
}

impl<C> ClientBuilder<C>
where
    C: NodeDirectory + ActorOwnerReader,
{
    /// Sets the total deadline for ownership resolution, Placement and Session open.
    pub fn open_timeout(mut self, timeout: Duration) -> Self {
        self.open_timeout = timeout;
        self
    }

    /// Sets the maximum time to establish one Transport Connection.
    pub fn transport_connect_timeout(mut self, timeout: Duration) -> Self {
        self.transport_connect_timeout = timeout;
        self.transport = Arc::new(GrpcTransport::new(timeout));
        self
    }

    /// Sets the maximum number of SessionOpen messages sent to target Servers by one `open()`.
    /// Connection failures before a SessionOpen is sent do not consume this limit.
    pub fn max_open_attempts(mut self, attempts: usize) -> Self {
        self.max_open_attempts = attempts;
        self
    }

    /// Sets the maximum Transport Connections retained for each Server endpoint.
    pub fn max_connections_per_endpoint(mut self, maximum: usize) -> Self {
        self.max_connections_per_endpoint = maximum;
        self
    }

    /// Replaces the default p2c Placement Strategy.
    pub fn placement_strategy(mut self, placement: Arc<dyn PlacementStrategy>) -> Self {
        self.placement = placement;
        self
    }

    #[cfg(test)]
    pub(crate) fn with_transport(mut self, transport: Arc<dyn ClientTransport>) -> Self {
        self.transport = transport;
        self
    }

    /// Validates the configuration and constructs the Client without Coordination I/O.
    pub fn build(self) -> Result<Client, ClientBuildError> {
        if self.open_timeout.is_zero() {
            return Err(ClientBuildError::InvalidOpenTimeout);
        }
        if self.transport_connect_timeout.is_zero() {
            return Err(ClientBuildError::InvalidTransportConnectTimeout);
        }
        if self.max_open_attempts == 0 {
            return Err(ClientBuildError::InvalidMaxOpenAttempts);
        }
        if self.max_connections_per_endpoint == 0 {
            return Err(ClientBuildError::InvalidMaxConnectionsPerEndpoint);
        }
        Ok(Client::from_parts(
            self.transport,
            Arc::new(self.coordination),
            self.open_timeout,
            self.max_open_attempts,
            self.max_connections_per_endpoint,
            self.placement,
        ))
    }
}

/// Caller runtime that directly resolves and connects to Actor Owners.
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

struct ConnectionPool {
    connections: AsyncMutex<Vec<Arc<TransportConnection>>>,
}

impl ConnectionPool {
    fn new() -> Self {
        Self {
            connections: AsyncMutex::new(Vec::new()),
        }
    }
}

pub(crate) struct TransportConnection {
    id: u64,
    endpoint: Endpoint,
    sender: Arc<dyn TransportSender>,
    sessions: Mutex<HashSet<crate::SessionId>>,
    bound_sessions: AtomicUsize,
    closed: AtomicBool,
}

impl TransportConnection {
    fn bind(&self, session_id: crate::SessionId) {
        if self.sessions.lock().insert(session_id) {
            self.bound_sessions.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn unbind(&self, session_id: &crate::SessionId) {
        if self.sessions.lock().remove(session_id) {
            self.bound_sessions.fetch_sub(1, Ordering::Relaxed);
        }
    }

    pub(crate) fn try_send(&self, envelope: Envelope) -> Result<(), SendError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(SendError::RemoteUnavailable);
        }
        self.sender
            .try_send(envelope)
            .map_err(|_| SendError::RemoteUnavailable)
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.sender.close();
        }
    }
}

struct PendingOpen {
    connection_id: u64,
    complete: oneshot::Sender<Result<(), SendError>>,
}

pub(crate) struct ClientInner {
    pub(crate) transport: Arc<dyn ClientTransport>,
    pub(crate) directory: Arc<dyn NodeDirectory>,
    pub(crate) owners: Arc<dyn ActorOwnerReader>,
    pub(crate) open_timeout: Duration,
    pub(crate) max_open_attempts: usize,
    pub(crate) max_connections_per_endpoint: usize,
    pub(crate) placement: Arc<dyn PlacementStrategy>,
    pub(crate) sessions: Arc<session::CallerRegistry>,
    pools: Mutex<HashMap<String, Arc<ConnectionPool>>>,
    pending_opens: Mutex<HashMap<crate::SessionId, PendingOpen>>,
    pub(crate) inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    placement_cache: AsyncMutex<Option<(Vec<PlacementCandidate>, tokio::time::Instant)>>,
    next_connection_id: AtomicU64,
    pub(crate) status: AtomicU8,
}

impl Client {
    fn from_parts<C>(
        transport: Arc<dyn ClientTransport>,
        coordination: Arc<C>,
        open_timeout: Duration,
        max_open_attempts: usize,
        max_connections_per_endpoint: usize,
        placement: Arc<dyn PlacementStrategy>,
    ) -> Self
    where
        C: NodeDirectory + ActorOwnerReader,
    {
        Self {
            inner: Arc::new(ClientInner {
                transport,
                directory: coordination.clone(),
                owners: coordination,
                open_timeout,
                max_open_attempts,
                max_connections_per_endpoint,
                placement,
                sessions: session::CallerRegistry::new(),
                pools: Mutex::new(HashMap::new()),
                pending_opens: Mutex::new(HashMap::new()),
                inbound_tasks: Mutex::new(Vec::new()),
                placement_cache: AsyncMutex::new(None),
                next_connection_id: AtomicU64::new(1),
                status: AtomicU8::new(RUNNING),
            }),
        }
    }

    pub(crate) fn with_transport<C>(
        transport: Arc<dyn ClientTransport>,
        coordination: Arc<C>,
    ) -> Self
    where
        C: NodeDirectory + ActorOwnerReader,
    {
        Self::from_parts(
            transport,
            coordination,
            DEFAULT_OPEN_TIMEOUT,
            DEFAULT_MAX_OPEN_ATTEMPTS,
            DEFAULT_MAX_CONNECTIONS_PER_ENDPOINT,
            default_placement(),
        )
    }

    /// Opens a bidirectional Session directly to the current or newly claimed Owner.
    pub async fn open(&self, address: &ActorAddress) -> Result<Session, OpenError> {
        let client = self.inner.clone();
        if client.status.load(Ordering::Acquire) != RUNNING {
            return Err(OpenError::RuntimeStopped);
        }
        let deadline = tokio::time::Instant::now() + client.open_timeout;
        let session_id = crate::SessionId::new();
        let (event_sender, receiver) = mpsc::channel(SESSION_RECEIVER_CAPACITY);
        client.sessions.register_local(session_id, event_sender);
        let mut attempts = 0;
        let mut excluded = HashSet::new();
        let mut failed_live_owner: Option<(String, NodeSessionId)> = None;

        let result = loop {
            let resolution = match client.resolve_owner(address, deadline).await {
                Ok(resolution) => resolution,
                Err(error) => break Err(error.into()),
            };
            let (endpoint, placement_attempt, live_owner) = match resolution {
                OwnerResolution::Live {
                    endpoint,
                    node_id,
                    session_id,
                } => {
                    if failed_live_owner
                        .as_ref()
                        .is_some_and(|failed| failed == &(node_id.clone(), session_id.clone()))
                    {
                        break Err(OpenError::RemoteUnavailable);
                    }
                    (endpoint, false, Some((node_id, session_id)))
                }
                OwnerResolution::Unowned => {
                    let endpoint = match client
                        .select_placement_candidate(address, &excluded, deadline)
                        .await
                    {
                        Ok(endpoint) => endpoint,
                        Err(error) => break Err(error.into()),
                    };
                    (endpoint, true, None)
                }
            };

            let connection = match client
                .acquire_connection(&endpoint, session_id, deadline)
                .await
            {
                Ok(connection) => connection,
                Err(_) if placement_attempt => {
                    excluded.insert(endpoint.as_str().to_owned());
                    client.placement.on_placement_failed(endpoint.as_str());
                    continue;
                }
                Err(_) => {
                    failed_live_owner = live_owner;
                    continue;
                }
            };
            if attempts >= client.max_open_attempts {
                connection.unbind(&session_id);
                if placement_attempt {
                    client.placement.on_placement_failed(endpoint.as_str());
                }
                break Err(OpenError::AttemptsExhausted);
            }
            let (sent, outcome) = client
                .send_session_open(address, session_id, &connection, deadline)
                .await;
            if sent {
                attempts += 1;
            }
            match outcome {
                Ok(()) => {
                    break Ok(Session {
                        client: Arc::downgrade(&client),
                        address: address.clone(),
                        session_id,
                        receiver,
                        registry: client.sessions.clone(),
                        connection,
                    });
                }
                Err(error) => {
                    let _ = connection.try_send(envelope_session_close(address, session_id));
                    connection.unbind(&session_id);
                    client.pending_opens.lock().remove(&session_id);
                    if placement_attempt {
                        client.placement.on_placement_failed(endpoint.as_str());
                    }
                    match error {
                        SendError::NotOwner if attempts >= client.max_open_attempts => {
                            break Err(OpenError::AttemptsExhausted);
                        }
                        SendError::NotOwner => continue,
                        SendError::RuntimeAtCapacity
                            if placement_attempt && attempts >= client.max_open_attempts =>
                        {
                            break Err(OpenError::AttemptsExhausted);
                        }
                        SendError::RuntimeAtCapacity if placement_attempt => {
                            excluded.insert(endpoint.as_str().to_owned());
                            continue;
                        }
                        SendError::RemoteUnavailable
                            if placement_attempt && attempts >= client.max_open_attempts =>
                        {
                            break Err(OpenError::AttemptsExhausted);
                        }
                        SendError::RemoteUnavailable if placement_attempt => {
                            excluded.insert(endpoint.as_str().to_owned());
                            continue;
                        }
                        SendError::RemoteUnavailable if attempts >= client.max_open_attempts => {
                            break Err(OpenError::AttemptsExhausted);
                        }
                        SendError::RemoteUnavailable => {
                            failed_live_owner = live_owner;
                            continue;
                        }
                        terminal => break Err(terminal.into()),
                    }
                }
            }
        };

        if result.is_err() {
            client.pending_opens.lock().remove(&session_id);
            client.sessions.unregister_local(&session_id);
        }
        result
    }

    /// Stops the Client and terminates all Sessions owned by it.
    pub async fn shutdown(self) {
        if self.inner.status.swap(STOPPED, Ordering::AcqRel) == STOPPED {
            return;
        }
        let pools = self
            .inner
            .pools
            .lock()
            .drain()
            .map(|(_, pool)| pool)
            .collect::<Vec<_>>();
        for pool in pools {
            for connection in pool.connections.lock().await.drain(..) {
                connection.close();
            }
        }
        for handle in self.inner.inbound_tasks.lock().drain(..) {
            handle.abort();
        }
        self.inner.pending_opens.lock().clear();
        self.inner.sessions.terminate_all(SendError::RuntimeStopped);
    }
}

enum OwnerResolution {
    Live {
        endpoint: Endpoint,
        node_id: String,
        session_id: NodeSessionId,
    },
    Unowned,
}

impl ClientInner {
    async fn timeout_at<T>(
        deadline: tokio::time::Instant,
        future: impl std::future::Future<Output = Result<T, crate::CoordinationError>>,
        error: SendError,
    ) -> Result<T, SendError> {
        tokio::time::timeout_at(deadline, future)
            .await
            .map_err(|_| error.clone())?
            .map_err(|_| error)
    }

    async fn resolve_owner(
        &self,
        address: &ActorAddress,
        deadline: tokio::time::Instant,
    ) -> Result<OwnerResolution, SendError> {
        let Some(current) = Self::timeout_at(
            deadline,
            self.owners.read_actor_owner(address),
            SendError::OwnershipUnavailable,
        )
        .await?
        else {
            return Ok(OwnerResolution::Unowned);
        };
        let Some(owner) = current.record.owner else {
            return Ok(OwnerResolution::Unowned);
        };
        let Some(node) = Self::timeout_at(
            deadline,
            self.directory.read_node(&owner.node_id),
            SendError::DirectoryUnavailable,
        )
        .await?
        else {
            return Ok(OwnerResolution::Unowned);
        };
        if node.session_id != owner.session_id {
            return Ok(OwnerResolution::Unowned);
        }
        if node.protocol_version != TRANSPORT_PROTOCOL_VERSION
            || node.advertised_endpoint.trim().is_empty()
        {
            return Err(SendError::RemoteUnavailable);
        }
        Ok(OwnerResolution::Live {
            endpoint: Endpoint::new(node.advertised_endpoint),
            node_id: owner.node_id,
            session_id: owner.session_id,
        })
    }

    async fn placement_candidates(
        &self,
        deadline: tokio::time::Instant,
    ) -> Result<Vec<PlacementCandidate>, SendError> {
        let now = tokio::time::Instant::now();
        {
            let cache = self.placement_cache.lock().await;
            if let Some((candidates, fetched_at)) = cache.as_ref() {
                if now.duration_since(*fetched_at) < PLACEMENT_CACHE_TTL {
                    return Ok(candidates.clone());
                }
            }
        }
        let nodes = Self::timeout_at(
            deadline,
            self.directory.list_nodes(),
            SendError::DirectoryUnavailable,
        )
        .await?;
        let candidates = nodes
            .into_iter()
            .filter(|node| {
                node.protocol_version == TRANSPORT_PROTOCOL_VERSION
                    && !node.advertised_endpoint.trim().is_empty()
                    && !node.pressured
                    && !node.draining
                    && node.active_actor_count < node.max_actor_count
            })
            .map(|node| PlacementCandidate {
                endpoint: node.advertised_endpoint,
                active_actor_count: node.active_actor_count,
                max_actor_count: node.max_actor_count,
            })
            .collect::<Vec<_>>();
        *self.placement_cache.lock().await = Some((candidates.clone(), now));
        Ok(candidates)
    }

    async fn select_placement_candidate(
        &self,
        address: &ActorAddress,
        excluded: &HashSet<String>,
        deadline: tokio::time::Instant,
    ) -> Result<Endpoint, SendError> {
        let candidates = self
            .placement_candidates(deadline)
            .await?
            .into_iter()
            .filter(|candidate| !excluded.contains(&candidate.endpoint))
            .collect::<Vec<_>>();
        if candidates.is_empty() {
            return Err(SendError::RuntimeAtCapacity);
        }
        let valid = candidates
            .iter()
            .map(|candidate| candidate.endpoint.clone())
            .collect::<HashSet<_>>();
        let ordered = self
            .placement
            .candidates(address, &PlacementContext { candidates });
        ordered
            .into_iter()
            .find(|endpoint| valid.contains(endpoint))
            .map(Endpoint::new)
            .ok_or(SendError::RuntimeAtCapacity)
    }

    fn pool(&self, endpoint: &Endpoint) -> Arc<ConnectionPool> {
        self.pools
            .lock()
            .entry(endpoint.as_str().to_owned())
            .or_insert_with(|| Arc::new(ConnectionPool::new()))
            .clone()
    }

    async fn acquire_connection(
        self: &Arc<Self>,
        endpoint: &Endpoint,
        session_id: crate::SessionId,
        deadline: tokio::time::Instant,
    ) -> Result<Arc<TransportConnection>, SendError> {
        let pool = self.pool(endpoint);
        let mut connections = tokio::time::timeout_at(deadline, pool.connections.lock())
            .await
            .map_err(|_| SendError::RemoteUnavailable)?;
        connections.retain(|connection| !connection.closed.load(Ordering::Acquire));
        if connections.len() < self.max_connections_per_endpoint {
            let stream = tokio::time::timeout_at(deadline, self.transport.connect(endpoint))
                .await
                .map_err(|_| SendError::RemoteUnavailable)?
                .map_err(|_| SendError::RemoteUnavailable)?;
            let connection = Arc::new(TransportConnection {
                id: self.next_connection_id.fetch_add(1, Ordering::Relaxed),
                endpoint: endpoint.clone(),
                sender: stream.sender(),
                sessions: Mutex::new(HashSet::new()),
                bound_sessions: AtomicUsize::new(0),
                closed: AtomicBool::new(false),
            });
            connections.push(connection.clone());
            connection.bind(session_id);
            self.spawn_receive_loop(stream, connection.clone());
            return Ok(connection);
        }
        let connection = connections
            .iter()
            .min_by_key(|connection| connection.bound_sessions.load(Ordering::Relaxed))
            .cloned()
            .ok_or(SendError::RemoteUnavailable)?;
        connection.bind(session_id);
        Ok(connection)
    }

    fn spawn_receive_loop(
        self: &Arc<Self>,
        mut stream: Box<dyn crate::transport::TransportStream>,
        connection: Arc<TransportConnection>,
    ) {
        let client = self.clone();
        let handle = tokio::spawn(async move {
            while let Some(envelope) = stream.recv().await {
                client.handle_envelope(envelope, &connection).await;
            }
            connection.closed.store(true, Ordering::Release);
            let pool = {
                client
                    .pools
                    .lock()
                    .get(connection.endpoint.as_str())
                    .cloned()
            };
            if let Some(pool) = pool {
                pool.connections
                    .lock()
                    .await
                    .retain(|candidate| candidate.id != connection.id);
            }
            let session_ids = connection.sessions.lock().drain().collect::<Vec<_>>();
            connection.bound_sessions.store(0, Ordering::Relaxed);
            for session_id in session_ids {
                let pending = {
                    let mut pending_opens = client.pending_opens.lock();
                    if pending_opens
                        .get(&session_id)
                        .is_some_and(|pending| pending.connection_id == connection.id)
                    {
                        pending_opens.remove(&session_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.complete.send(Err(SendError::RemoteUnavailable));
                } else {
                    client
                        .sessions
                        .terminate(&session_id, SendError::RemoteUnavailable);
                }
            }
            client.retain_inbound_tasks();
        });
        self.register_inbound_task(handle.abort_handle());
    }

    fn retain_inbound_tasks(&self) {
        self.inbound_tasks.lock().retain(|task| !task.is_finished());
    }

    fn register_inbound_task(&self, handle: tokio::task::AbortHandle) {
        self.retain_inbound_tasks();
        self.inbound_tasks.lock().push(handle);
    }

    async fn send_session_open(
        &self,
        address: &ActorAddress,
        session_id: crate::SessionId,
        connection: &TransportConnection,
        deadline: tokio::time::Instant,
    ) -> (bool, Result<(), SendError>) {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_opens.lock().insert(
            session_id,
            PendingOpen {
                connection_id: connection.id,
                complete: ack_tx,
            },
        );
        if let Err(error) = connection.try_send(envelope_session_open(address, session_id)) {
            return (false, Err(error));
        }
        let result = match tokio::time::timeout_at(deadline, ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) | Err(_) => Err(SendError::RemoteUnavailable),
        };
        (true, result)
    }

    async fn handle_envelope(&self, envelope: Envelope, connection: &TransportConnection) {
        let Some(kind) = envelope.kind else { return };
        let Some(session_id) = crate::SessionId::from_bytes(&envelope.session_id) else {
            return;
        };
        match kind {
            envelope::Kind::Event(event) => {
                if connection.sessions.lock().contains(&session_id) {
                    if let Some(sender) = self.sessions.receiver(&session_id) {
                        let _ = sender.try_send(Ok(event.payload));
                    }
                }
            }
            envelope::Kind::SessionError(error) => {
                if connection.sessions.lock().contains(&session_id) {
                    if let Some(sender) = self.sessions.receiver(&session_id) {
                        let _ = sender.try_send(Err(SendError::from_wire(error.failure)));
                    }
                }
            }
            envelope::Kind::SessionOpenedAck(ack) => {
                let result = match ack.outcome {
                    Some(session_opened_ack::Outcome::Ok(_)) => Ok(()),
                    Some(session_opened_ack::Outcome::Failure(failure)) => {
                        Err(SendError::from_wire(failure))
                    }
                    None => Err(SendError::RemoteProtocol(
                        crate::RemoteProtocolError::MalformedMessage,
                    )),
                };
                let pending = {
                    let mut pending_opens = self.pending_opens.lock();
                    if pending_opens
                        .get(&session_id)
                        .is_some_and(|pending| pending.connection_id == connection.id)
                    {
                        pending_opens.remove(&session_id)
                    } else {
                        None
                    }
                };
                if let Some(pending) = pending {
                    let _ = pending.complete.send(result);
                }
            }
            _ => {}
        }
    }
}

fn envelope_session_open(address: &ActorAddress, session_id: crate::SessionId) -> Envelope {
    Envelope {
        protocol_version: TRANSPORT_PROTOCOL_VERSION,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        kind: Some(envelope::Kind::SessionOpen(
            crate::transport_protocol::SessionOpen {},
        )),
    }
}

pub(crate) fn envelope_action(
    address: &ActorAddress,
    session_id: crate::SessionId,
    payload: Vec<u8>,
) -> Envelope {
    Envelope {
        protocol_version: TRANSPORT_PROTOCOL_VERSION,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        kind: Some(envelope::Kind::Action(
            crate::transport_protocol::ActionMessage { payload },
        )),
    }
}

pub(crate) fn envelope_session_close(
    address: &ActorAddress,
    session_id: crate::SessionId,
) -> Envelope {
    Envelope {
        protocol_version: TRANSPORT_PROTOCOL_VERSION,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        kind: Some(envelope::Kind::SessionClose(
            crate::transport_protocol::SessionClose {
                reason: crate::transport_protocol::CloseReason::CallerDropped as i32,
            },
        )),
    }
}
