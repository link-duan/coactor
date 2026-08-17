//! caller 侧 runtime：只调用不宿主（ADR-0008、ADR-0010）。
//!
//! `Client` 从 Coordination Store 的 Node Directory 获取 live Server 节点，维护
//! Gateway Pool 并按会话 round-robin 分配网关；会话经网关中继到 Owner。

pub(crate) mod session;

use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
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
    ActorAddress, ActorId, CoordinationConfig, LeaseTiming, NodeDirectory, PEER_PROTOCOL_VERSION,
    SendError,
};

pub(crate) const SESSION_RECEIVER_CAPACITY: usize = 64;
const OPEN_TIMEOUT: Duration = Duration::from_secs(3);
/// open() 传输层重试次数（共 pick 次数 = retries + 1）。
const MAX_OPEN_GATEWAY_RETRIES: usize = 1;
const PEER_CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const RUNNING: u8 = 0;
pub(crate) const STOPPED: u8 = 1;

/// 公开 Client 配置：Coordination Store 提供只读 Node Directory。
#[derive(Clone)]
pub struct ClientConfig {
    pub coordination: CoordinationConfig,
}

pub struct ClientBuilder {
    transport: Arc<dyn ClientTransport>,
    directory: Arc<dyn NodeDirectory>,
    refresh_interval: Duration,
}

impl ClientBuilder {
    /// 公开构造：gRPC transport + Coordination Store Node Directory。
    pub fn new(config: ClientConfig) -> Self {
        let stores = config.coordination.build();
        Self {
            transport: Arc::new(GrpcTransport::new(PEER_CONNECT_TIMEOUT)),
            directory: stores.directory,
            refresh_interval: LeaseTiming::default().renewal_interval,
        }
    }

    /// crate 内部：注入 transport 与 Node Directory（inmem 测试路径）。
    pub(crate) fn with_transport(
        transport: Arc<dyn ClientTransport>,
        directory: Arc<dyn NodeDirectory>,
    ) -> Self {
        Self {
            transport,
            directory,
            refresh_interval: LeaseTiming::default().renewal_interval,
        }
    }

    pub fn directory_refresh_interval(mut self, interval: Duration) -> Self {
        self.refresh_interval = interval;
        self
    }

    pub fn start(self) -> Client {
        Client {
            inner: Arc::new(ClientInner {
                transport: self.transport,
                directory: self.directory,
                refresh_interval: self.refresh_interval,
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
}

pub struct Client {
    pub(crate) inner: Arc<ClientInner>,
}

/// Gateway Pool：最新 Node Directory 结果 + round-robin 游标。
#[derive(Default)]
pub(crate) struct Pool {
    endpoints: Vec<Endpoint>,
    next: usize,
    refresh_at: Option<tokio::time::Instant>,
}

pub(crate) struct ClientInner {
    pub(crate) transport: Arc<dyn ClientTransport>,
    pub(crate) directory: Arc<dyn NodeDirectory>,
    pub(crate) refresh_interval: Duration,
    pub(crate) refresh_lock: AsyncMutex<()>,
    pub(crate) pool: Mutex<Pool>,
    pub(crate) sessions: Arc<session::CallerRegistry>,
    /// 到各网关节点的 bidi stream 发送端（node-pair 复用）。
    pub(crate) channels: Mutex<HashMap<String, Arc<dyn PeerSender>>>,
    /// 远程 SessionOpen 的 ack 等待表。
    pub(crate) pending_opens:
        Mutex<HashMap<crate::SessionId, oneshot::Sender<Result<(), SendError>>>>,
    pub(crate) inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    pub(crate) status: AtomicU8,
}

impl Client {
    /// 按 Actor Type 名字 + Actor ID 获取通用地址句柄（纯字符串 API）。
    pub fn actor(&self, actor_type: &str, actor_id: ActorId) -> ActorRef {
        ActorRef {
            client: Arc::downgrade(&self.inner),
            address: ActorAddress::new(actor_type, actor_id),
        }
    }

    pub async fn shutdown(&self) {
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

/// 通用稳定地址句柄（非泛型）。
pub struct ActorRef {
    pub(crate) client: Weak<ClientInner>,
    pub(crate) address: ActorAddress,
}

impl Clone for ActorRef {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            address: self.address.clone(),
        }
    }
}

impl ActorRef {
    /// 建立与 Actor 的持久 Session：池中选网关 → 经 transport 发 SessionOpen →
    /// 等 ack → 返回。失败时清理本地注册。
    pub async fn open(&self) -> Result<Session, SendError> {
        let client = self.client.upgrade().ok_or(SendError::RuntimeStopped)?;
        if client.status.load(Ordering::Acquire) != RUNNING {
            return Err(SendError::RuntimeStopped);
        }
        let session_id = crate::SessionId::new();
        let (sender, receiver) = mpsc::channel(SESSION_RECEIVER_CAPACITY);
        client.sessions.register_local(session_id, sender);
        let mut attempts = 0;

        // 传输层有界重试：网关不可达（connect 失败/超时）→ 驱逐 + 换下一个；
        // 放置层失败（ack 错误）不重试，原样返回。
        let (endpoint, channel) = loop {
            let endpoint = match client.pick_endpoint().await {
                Ok(endpoint) => endpoint,
                Err(error) => {
                    client.sessions.unregister_local(&session_id);
                    return Err(error);
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
                    return Err(error);
                }
            }
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        client.pending_opens.lock().insert(session_id, ack_tx);
        let envelope = envelope_session_open(&self.address, session_id, PEER_PROTOCOL_VERSION);
        if channel.try_send(envelope).is_err() {
            client.pending_opens.lock().remove(&session_id);
            client.sessions.unregister_local(&session_id);
            return Err(SendError::RemoteUnavailable);
        }
        let outcome = tokio::time::timeout(OPEN_TIMEOUT, ack_rx)
            .await
            .map_err(|_| SendError::RemoteUnavailable)?
            .map_err(|_| SendError::RemoteUnavailable)?;
        if let Err(error) = outcome {
            client.pending_opens.lock().remove(&session_id);
            client.sessions.unregister_local(&session_id);
            return Err(error);
        }

        Ok(Session {
            client: Arc::downgrade(&client),
            address: self.address.clone(),
            session_id,
            receiver,
            registry: client.sessions.clone(),
            owner_endpoint: Some(endpoint),
        })
    }

    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
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
                    && !node.advertised_address.trim().is_empty()
            })
            .map(|node| Endpoint::new(node.advertised_address))
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
