//! Caller runtime: opens Sessions through a read-only Node Directory.

pub(crate) mod session;

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};

use self::session::Session;
use crate::peer_protocol::{Envelope, envelope, session_opened_ack};
use crate::transport::grpc::GrpcTransport;
use crate::transport::{ClientTransport, Endpoint, PeerSender};
use crate::{
    ActorAddress, ClientBuildError, NodeDirectory, OpenError, PEER_PROTOCOL_VERSION, SendError,
};

pub(crate) const SESSION_RECEIVER_CAPACITY: usize = 64;
const DEFAULT_OPEN_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_OPEN_GATEWAY_RETRIES: usize = 1;
const DEFAULT_PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const RUNNING: u8 = 0;
pub(crate) const STOPPED: u8 = 1;

/// Builds a caller-only [`Client`] from a read-only Node Directory.
pub struct ClientBuilder<D> {
    transport: Arc<dyn ClientTransport>,
    directory: D,
    open_timeout: Duration,
    peer_connect_timeout: Duration,
}

impl Client {
    /// Creates a Client builder without performing directory I/O.
    pub fn builder<D: NodeDirectory>(directory: D) -> ClientBuilder<D> {
        ClientBuilder {
            transport: Arc::new(GrpcTransport::new(DEFAULT_PEER_CONNECT_TIMEOUT)),
            directory,
            open_timeout: DEFAULT_OPEN_TIMEOUT,
            peer_connect_timeout: DEFAULT_PEER_CONNECT_TIMEOUT,
        }
    }
}

impl<D: NodeDirectory> ClientBuilder<D> {
    /// Sets the maximum time to wait for a Session-open acknowledgement.
    pub fn open_timeout(mut self, timeout: Duration) -> Self {
        self.open_timeout = timeout;
        self
    }
    /// Sets the maximum time to establish a peer connection.
    pub fn peer_connect_timeout(mut self, timeout: Duration) -> Self {
        self.peer_connect_timeout = timeout;
        self.transport = Arc::new(GrpcTransport::new(timeout));
        self
    }
    /// Validates the configuration and constructs the Client without directory I/O.
    pub fn build(self) -> Result<Client, ClientBuildError> {
        if self.open_timeout.is_zero() {
            return Err(ClientBuildError::InvalidOpenTimeout);
        }
        if self.peer_connect_timeout.is_zero() {
            return Err(ClientBuildError::InvalidPeerConnectTimeout);
        }
        Ok(Client::from_parts(
            self.transport,
            Arc::new(self.directory),
            self.open_timeout,
        ))
    }
}

/// Caller runtime that discovers Gateways and opens Sessions to Actors.
pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

#[derive(Default)]
pub(crate) struct Pool {
    endpoints: Vec<Endpoint>,
    next: usize,
    refresh_at: Option<tokio::time::Instant>,
}

pub(crate) struct ClientInner {
    pub(crate) transport: Arc<dyn ClientTransport>,
    pub(crate) directory: Arc<dyn NodeDirectory>,
    pub(crate) open_timeout: Duration,
    pub(crate) refresh_interval: Duration,
    pub(crate) refresh_lock: AsyncMutex<()>,
    pub(crate) pool: Mutex<Pool>,
    pub(crate) sessions: Arc<session::CallerRegistry>,
    pub(crate) channels: Mutex<HashMap<String, Arc<dyn PeerSender>>>,
    pub(crate) pending_opens:
        Mutex<HashMap<crate::SessionId, oneshot::Sender<Result<(), SendError>>>>,
    pub(crate) inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    pub(crate) status: AtomicU8,
}

impl Client {
    pub(crate) fn from_parts(
        transport: Arc<dyn ClientTransport>,
        directory: Arc<dyn NodeDirectory>,
        open_timeout: Duration,
    ) -> Self {
        Self {
            inner: Arc::new(ClientInner {
                transport,
                directory,
                open_timeout,
                refresh_interval: Duration::ZERO,
                refresh_lock: AsyncMutex::new(()),
                pool: Mutex::new(Pool::default()),
                sessions: session::CallerRegistry::new(),
                channels: Mutex::new(HashMap::new()),
                pending_opens: Mutex::new(HashMap::new()),
                inbound_tasks: Mutex::new(Vec::new()),
                status: AtomicU8::new(RUNNING),
            }),
        }
    }

    pub(crate) fn with_transport(
        transport: Arc<dyn ClientTransport>,
        directory: Arc<dyn NodeDirectory>,
    ) -> Self {
        Self::from_parts(transport, directory, DEFAULT_OPEN_TIMEOUT)
    }

    /// Opens a bidirectional Session to the supplied Actor Address.
    pub async fn open(&self, address: &ActorAddress) -> Result<Session, OpenError> {
        let client = self.inner.clone();
        if client.status.load(Ordering::Acquire) != RUNNING {
            return Err(OpenError::RuntimeStopped);
        }
        let session_id = crate::SessionId::new();
        let (sender, receiver) = mpsc::channel(SESSION_RECEIVER_CAPACITY);
        client.sessions.register_local(session_id, sender);
        let mut attempts = 0;
        let (endpoint, channel) = loop {
            let endpoint = match client.pick_endpoint().await {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    client.sessions.unregister_local(&session_id);
                    return Err(error.into());
                }
            };
            match client.ensure_channel(&endpoint).await {
                Ok(channel) => break (endpoint, channel),
                Err(SendError::RemoteUnavailable) if attempts < MAX_OPEN_GATEWAY_RETRIES => {
                    client.evict_endpoint(&endpoint);
                    attempts += 1;
                }
                Err(error) => {
                    client.sessions.unregister_local(&session_id);
                    return Err(error.into());
                }
            }
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        client.pending_opens.lock().insert(session_id, ack_tx);
        let envelope = envelope_session_open(address, session_id, PEER_PROTOCOL_VERSION);
        if channel.try_send(envelope).is_err() {
            client.pending_opens.lock().remove(&session_id);
            client.sessions.unregister_local(&session_id);
            return Err(OpenError::RemoteUnavailable);
        }
        let outcome = tokio::time::timeout(client.open_timeout, ack_rx)
            .await
            .map_err(|_| OpenError::RemoteUnavailable)?
            .map_err(|_| OpenError::RemoteUnavailable)?;
        if let Err(error) = outcome {
            client.pending_opens.lock().remove(&session_id);
            client.sessions.unregister_local(&session_id);
            return Err(error.into());
        }
        Ok(Session {
            client: Arc::downgrade(&client),
            address: address.clone(),
            session_id,
            receiver,
            registry: client.sessions.clone(),
            owner_endpoint: Some(endpoint),
        })
    }

    /// Stops the Client and terminates all Sessions owned by it.
    pub async fn shutdown(self) {
        self.inner.status.store(STOPPED, Ordering::Release);
        for handle in self.inner.inbound_tasks.lock().drain(..) {
            handle.abort();
        }
        self.inner.channels.lock().clear();
        self.inner.pending_opens.lock().clear();
        self.inner.pool.lock().endpoints.clear();
        self.inner.sessions.terminate_all(SendError::RuntimeStopped);
    }
}

impl ClientInner {
    fn next_directory_refresh(&self) -> tokio::time::Instant {
        let jitter = 0.8 + (rand::random::<u16>() % 401) as f64 / 1000.0;
        tokio::time::Instant::now() + self.refresh_interval.mul_f64(jitter)
    }

    /// 从 Node Directory 刷新 Gateway Pool；到期刷新单飞，失败时保留旧池。
    async fn refresh_directory_if_due(&self) -> Result<(), SendError> {
        let due = {
            let pool = self.pool.lock();
            pool.endpoints.is_empty()
                || pool
                    .refresh_at
                    .is_none_or(|refresh_at| tokio::time::Instant::now() >= refresh_at)
        };
        if !due {
            return Ok(());
        }

        let _guard = self.refresh_lock.lock().await;
        let due = {
            let pool = self.pool.lock();
            pool.endpoints.is_empty()
                || pool
                    .refresh_at
                    .is_none_or(|refresh_at| tokio::time::Instant::now() >= refresh_at)
        };
        if !due {
            return Ok(());
        }

        let nodes = match self.directory.list_nodes().await {
            Ok(nodes) => nodes,
            Err(_) => {
                let mut pool = self.pool.lock();
                if pool.endpoints.is_empty() {
                    return Err(SendError::DirectoryUnavailable);
                }
                pool.refresh_at = Some(self.next_directory_refresh());
                return Ok(());
            }
        };
        let mut endpoints: Vec<Endpoint> = nodes
            .into_iter()
            .filter(|node| {
                node.protocol_version == PEER_PROTOCOL_VERSION
                    && !node.draining
                    && !node.advertised_endpoint.trim().is_empty()
            })
            .map(|node| Endpoint::new(node.advertised_endpoint))
            .collect();
        endpoints.sort();
        endpoints.dedup();

        let mut pool = self.pool.lock();
        pool.endpoints = endpoints;
        let endpoint_count = pool.endpoints.len().max(1);
        pool.next %= endpoint_count;
        pool.refresh_at = Some(self.next_directory_refresh());
        Ok(())
    }

    /// Gateway Pool 中按 round-robin 选择节点。
    pub(crate) async fn pick_endpoint(&self) -> Result<Endpoint, SendError> {
        self.refresh_directory_if_due().await?;
        let mut pool = self.pool.lock();
        if pool.endpoints.is_empty() {
            return Err(SendError::NoAvailableGateway);
        }
        let endpoint = pool.endpoints[pool.next % pool.endpoints.len()].clone();
        pool.next += 1;
        Ok(endpoint)
    }

    /// 网关节点失效：从池中驱逐（下次 open 触发重发现）。
    pub(crate) fn evict_endpoint(&self, endpoint: &Endpoint) {
        let mut pool = self.pool.lock();
        pool.endpoints.retain(|candidate| candidate != endpoint);
    }

    /// 到网关节点的 bidi 流发送端（node-pair 复用）；建流后 spawn 收包循环。
    pub(crate) async fn ensure_channel(
        self: &Arc<Self>,
        endpoint: &Endpoint,
    ) -> Result<Arc<dyn PeerSender>, SendError> {
        let key = endpoint.as_str().to_owned();
        if let Some(sender) = self.channels.lock().get(&key) {
            return Ok(sender.clone());
        }
        let stream = self
            .transport
            .connect(endpoint)
            .await
            .map_err(|_| SendError::RemoteUnavailable)?;
        let sender = stream.sender();
        let client = self.clone();
        let closed_key = key.clone();
        let handle = tokio::spawn(async move {
            let mut stream = stream;
            while let Some(envelope) = stream.recv().await {
                client.handle_envelope(envelope).await;
            }
            client.channels.lock().remove(&closed_key);
            client.evict_endpoint(&Endpoint::new(closed_key));
        });
        self.inbound_tasks.lock().push(handle.abort_handle());
        self.channels.lock().insert(key, sender.clone());
        Ok(sender)
    }

    /// caller 侧收到的 Envelope：Event/SessionError 投递到 Session 接收流，
    /// SessionOpenedAck 解 pending_opens；其余（Action/SessionOpen 等）忽略。
    async fn handle_envelope(&self, envelope: Envelope) {
        let Some(kind) = envelope.kind else { return };
        let Some(session_id) = crate::SessionId::from_bytes(&envelope.session_id) else {
            return;
        };
        match kind {
            envelope::Kind::Event(event) => {
                if let Some(sender) = self.sessions.receiver(&session_id) {
                    let _ = sender.try_send(Ok(event.payload));
                }
            }
            envelope::Kind::SessionError(error) => {
                if let Some(sender) = self.sessions.receiver(&session_id) {
                    let _ = sender.try_send(Err(SendError::from_wire(error.failure)));
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
                if let Some(sender) = self.pending_opens.lock().remove(&session_id) {
                    let _ = sender.send(result);
                }
            }
            _ => {}
        }
    }
}

fn envelope_session_open(
    address: &ActorAddress,
    session_id: crate::SessionId,
    version: u32,
) -> Envelope {
    Envelope {
        protocol_version: version,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(envelope::Kind::SessionOpen(
            crate::peer_protocol::SessionOpen {
                caller_endpoint: String::new(),
            },
        )),
    }
}

pub(crate) fn envelope_action(
    address: &ActorAddress,
    session_id: crate::SessionId,
    payload: Vec<u8>,
) -> Envelope {
    Envelope {
        protocol_version: PEER_PROTOCOL_VERSION,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(envelope::Kind::Action(
            crate::peer_protocol::ActionMessage { payload },
        )),
    }
}
