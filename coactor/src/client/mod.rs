//! caller 侧 runtime：只调用不宿主（ADR-0008）。
//!
//! `Client` 经 transport（gRPC 或 inmem）向 Server 节点发起 Session，经网关
//! 中继消息；不持有 AppState、不 claim ownership、无 lease/self-fence。

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
use tokio::sync::{mpsc, oneshot};

use crate::peer_protocol::{Envelope, envelope, session_opened_ack};
use crate::transport::{ClientTransport, Endpoint, PeerSender};
use crate::{ActorAddress, ActorId, OwnershipBackend, PEER_PROTOCOL_VERSION, SendError};

use self::session::Session;

pub(crate) const SESSION_RECEIVER_CAPACITY: usize = 64;
const OPEN_TIMEOUT: Duration = Duration::from_secs(3);

pub(crate) const RUNNING: u8 = 0;
pub(crate) const STOPPED: u8 = 1;

/// 路由模式：`Direct` 把全部 Session 发往单一端点（本地测试/单后端）；
/// `Authority` 经 ownership authority 只读解析 owner（分布式，不 claim）。
/// Authority 变体由 cluster 测试与分布式 Client 使用。
#[allow(dead_code)]
pub(crate) enum RouteMode {
    Direct(Endpoint),
    Authority(Arc<dyn OwnershipBackend>),
}

pub struct ClientBuilder {
    pub(crate) transport: Arc<dyn ClientTransport>,
    pub(crate) route: RouteMode,
}

impl ClientBuilder {
    pub(crate) fn new(transport: Arc<dyn ClientTransport>, route: RouteMode) -> Self {
        Self { transport, route }
    }

    pub fn start(self) -> Client {
        Client {
            inner: Arc::new(ClientInner {
                transport: self.transport,
                route: self.route,
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

pub(crate) struct ClientInner {
    pub(crate) transport: Arc<dyn ClientTransport>,
    pub(crate) route: RouteMode,
    pub(crate) sessions: Arc<session::CallerRegistry>,
    /// 到各远端 Server 的 bidi stream 发送端（node-pair 复用）。
    pub(crate) channels: Mutex<HashMap<String, Arc<dyn PeerSender>>>,
    /// 远程 SessionOpen 的 ack 等待表。
    pub(crate) pending_opens: Mutex<HashMap<crate::SessionId, oneshot::Sender<Result<(), SendError>>>>,
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
        self.inner
            .status
            .store(STOPPED, Ordering::Release);
        for handle in self.inner.inbound_tasks.lock().drain(..) {
            handle.abort();
        }
        self.inner.channels.lock().clear();
        self.inner.pending_opens.lock().clear();
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
    /// 建立与 Actor 的持久 Session：resolve owner → 经 transport 发 SessionOpen →
    /// 等 ack → 返回。失败时清理本地注册。
    pub async fn open(&self) -> Result<Session, SendError> {
        let client = self.client.upgrade().ok_or(SendError::RuntimeStopped)?;
        if client.status.load(Ordering::Acquire) != RUNNING {
            return Err(SendError::RuntimeStopped);
        }
        let session_id = crate::SessionId::new();
        let (sender, receiver) = mpsc::channel(SESSION_RECEIVER_CAPACITY);
        client.sessions.register_local(session_id, sender);

        let cleanup = || {
            client.sessions.unregister_local(&session_id);
        };

        let endpoint = match resolve_owner(&client, &self.address).await {
            Ok(endpoint) => endpoint,
            Err(error) => {
                cleanup();
                return Err(error);
            }
        };

        let channel = match client.ensure_channel(&endpoint).await {
            Ok(channel) => channel,
            Err(error) => {
                cleanup();
                return Err(error);
            }
        };
        let (ack_tx, ack_rx) = oneshot::channel();
        client.pending_opens.lock().insert(session_id, ack_tx);
        let envelope = envelope_session_open(&self.address, session_id, PEER_PROTOCOL_VERSION);
        if channel.try_send(envelope).is_err() {
            client.pending_opens.lock().remove(&session_id);
            cleanup();
            return Err(SendError::RemoteUnavailable);
        }
        let outcome = tokio::time::timeout(OPEN_TIMEOUT, ack_rx)
            .await
            .map_err(|_| SendError::RemoteUnavailable)?
            .map_err(|_| SendError::RemoteUnavailable)?;
        if let Err(error) = outcome {
            client.pending_opens.lock().remove(&session_id);
            cleanup();
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

/// 解析 owner 端点：`Direct` 恒返回配置端点；`Authority` 只读解析，未拥有/stale
/// 时返回 `OwnershipUnavailable`（client 永不 claim）。
async fn resolve_owner(client: &Arc<ClientInner>, address: &ActorAddress) -> Result<Endpoint, SendError> {
    match &client.route {
        RouteMode::Direct(endpoint) => Ok(endpoint.clone()),
        RouteMode::Authority(backend) => session::resolve_owner_endpoint(backend.as_ref(), address)
            .await?
            .ok_or(SendError::OwnershipUnavailable),
    }
}

impl ClientInner {
    /// 到远端节点的 bidi 流发送端（node-pair 复用）；建流后 spawn 收包循环。
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

fn envelope_session_open(address: &ActorAddress, session_id: crate::SessionId, version: u32) -> Envelope {
    Envelope {
        protocol_version: version,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(envelope::Kind::SessionOpen(crate::peer_protocol::SessionOpen {
            caller_endpoint: String::new(),
        })),
    }
}

pub(crate) fn envelope_action(address: &ActorAddress, session_id: crate::SessionId, payload: Vec<u8>) -> Envelope {
    Envelope {
        protocol_version: PEER_PROTOCOL_VERSION,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(envelope::Kind::Action(crate::peer_protocol::ActionMessage {
            payload,
        })),
    }
}
