use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::cluster::ResolvedOwner;
use crate::{ActorAddress, SendError};

use super::core::RuntimeInner;

/// 本节点 Session 接收流的容量（出站 Event 背压上界）。
pub(crate) const SESSION_RECEIVER_CAPACITY: usize = 64;

/// Session 的唯一标识，由 caller 建立 Session 时生成。
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct SessionId(Uuid);

impl SessionId {
    pub(crate) fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub(crate) fn as_bytes(&self) -> Vec<u8> {
        self.0.as_bytes().to_vec()
    }

    pub(crate) fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let bytes: [u8; 16] = bytes.try_into().ok()?;
        Some(Self(Uuid::from_bytes(bytes)))
    }
}

/// Session 的 Event 回传路径（owner 节点视角）。
#[derive(Clone)]
pub(crate) enum EventSink {
    /// caller 位于本节点：Event 经 receivers 注册表投递。
    Local,
    /// caller 位于远端节点：Event 经该节点对间的 bidi stream 发送端回传。
    Remote {
        sender: tokio::sync::mpsc::Sender<crate::peer_protocol::Envelope>,
    },
}

/// 本节点的 Session 注册表，供 Event 投递、passivation 计数与关闭清理。
/// 独立于 `RuntimeInner` 的泛型参数，因此 `SessionHandle` 可以是非泛型的。
type EventSender = mpsc::Sender<Result<Vec<u8>, SendError>>;

pub(crate) struct SessionRegistry {
    receivers: Mutex<HashMap<SessionId, EventSender>>,
    by_actor: Mutex<HashMap<ActorAddress, HashMap<SessionId, EventSink>>>,
}

impl SessionRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            receivers: Mutex::new(HashMap::new()),
            by_actor: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn register_local(&self, session_id: SessionId, sender: mpsc::Sender<Result<Vec<u8>, SendError>>) {
        self.receivers.lock().insert(session_id, sender);
    }

    pub(crate) fn unregister_local(&self, session_id: &SessionId) {
        self.receivers.lock().remove(session_id);
    }

    /// owner 侧注册一个 Actor 的存活 Session；重复注册返回 false。
    pub(crate) fn register_actor(&self, address: &ActorAddress, session_id: SessionId, sink: EventSink) -> bool {
        self.by_actor
            .lock()
            .entry(address.clone())
            .or_default()
            .insert(session_id, sink)
            .is_none()
    }

    pub(crate) fn unregister_actor(&self, address: &ActorAddress, session_id: &SessionId) -> bool {
        let mut by_actor = self.by_actor.lock();
        let removed = by_actor
            .get_mut(address)
            .is_some_and(|sessions| sessions.remove(session_id).is_some());
        if removed && by_actor.get(address).is_some_and(|sessions| sessions.is_empty()) {
            by_actor.remove(address);
        }
        removed
    }

    /// 某 Actor 的存活 Session 数量（passivation 判断）。
    pub(crate) fn session_count(&self, address: &ActorAddress) -> usize {
        self.by_actor
            .lock()
            .get(address)
            .map_or(0, |sessions| sessions.len())
    }

    pub(crate) fn by_actor_snapshot(&self, address: &ActorAddress) -> Vec<SessionId> {
        self.by_actor
            .lock()
            .get(address)
            .map(|sessions| sessions.keys().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) fn by_actor_addresses(&self) -> Vec<ActorAddress> {
        self.by_actor.lock().keys().cloned().collect()
    }

    pub(crate) fn sink(&self, address: &ActorAddress, session_id: &SessionId) -> Option<EventSink> {
        self.by_actor
            .lock()
            .get(address)
            .and_then(|sessions| sessions.get(session_id).cloned())
    }

    /// 向一个 Session 投递 Event；尽力而为。
    pub(crate) async fn deliver_event(
        &self,
        address: &ActorAddress,
        session_id: SessionId,
        payload: Vec<u8>,
    ) -> Result<(), SendError> {
        match self.sink(address, &session_id) {
            Some(EventSink::Local) => self
                .receivers
                .lock()
                .get(&session_id)
                .ok_or(SendError::ActorStopped)?
                .try_send(Ok(payload))
                .map_err(|_| SendError::ActorStopped),
            Some(EventSink::Remote { sender }) => sender
                .try_send(crate::peer_protocol::Envelope {
                    protocol_version: 0,
                    actor_type: String::new(),
                    actor_id: Vec::new(),
                    session_id: session_id.as_bytes(),
                    from_node: String::new(),
                    kind: Some(
                        crate::peer_protocol::envelope::Kind::Event(
                            crate::peer_protocol::EventMessage { payload },
                        ),
                    ),
                })
                .map_err(|_| SendError::RemoteUnavailable),
            None => Err(SendError::ActorStopped),
        }
    }

    /// 以终止错误结束一个 Session（owner 存活时主动通知）。
    pub(crate) async fn terminate(
        &self,
        address: &ActorAddress,
        session_id: &SessionId,
        error: SendError,
    ) {
        let sink = self.sink(address, session_id);
        self.unregister_actor(address, session_id);
        match sink {
            Some(EventSink::Local) => {
                let sender = self.receivers.lock().get(session_id).cloned();
                if let Some(sender) = sender {
                    let _ = sender.send(Err(error)).await;
                }
                self.unregister_local(session_id);
            }
            Some(EventSink::Remote { sender }) => {
                let _ = sender.try_send(crate::peer_protocol::Envelope {
                    protocol_version: 0,
                    actor_type: String::new(),
                    actor_id: Vec::new(),
                    session_id: session_id.as_bytes(),
                    from_node: String::new(),
                    kind: Some(
                        crate::peer_protocol::envelope::Kind::SessionError(
                            crate::peer_protocol::SessionError {
                                failure: error.to_wire(),
                            },
                        ),
                    ),
                });
            }
            None => {}
        }
    }

    /// 本节点 Session 接收端（供远程 Event/错误投递）。
    pub(crate) fn receiver(
        &self,
        session_id: &SessionId,
    ) -> Option<mpsc::Sender<Result<Vec<u8>, SendError>>> {
        self.receivers.lock().get(session_id).cloned()
    }

    /// 终止某 Actor 的全部存活 Session。
    pub(crate) async fn terminate_all(&self, address: &ActorAddress, error: SendError) {
        let session_ids: Vec<SessionId> = {
            let by_actor = self.by_actor.lock();
            by_actor
                .get(address)
                .map(|sessions| sessions.keys().copied().collect())
                .unwrap_or_default()
        };
        for session_id in session_ids {
            self.terminate(address, &session_id, error.clone()).await;
        }
    }
}

/// Actor 侧持有的 Session 出站句柄：向该 Session 的 caller 定向推送 Event。
/// 非泛型：仅引用注册表 + 地址 + Session ID。
#[derive(Clone)]
pub struct SessionHandle {
    pub(crate) registry: Arc<SessionRegistry>,
    pub(crate) address: ActorAddress,
    pub(crate) session_id: SessionId,
}

impl SessionHandle {
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), SendError> {
        self.registry
            .deliver_event(&self.address, self.session_id, msg)
            .await
    }
}

/// caller 侧持有的持久 Session：入站发送 Action，出站接收 Event 流。
pub struct Session<S>
where
    S: Send + Sync + 'static,
{
    pub(crate) inner: Weak<RuntimeInner<S>>,
    pub(crate) address: ActorAddress,
    pub(crate) session_id: SessionId,
    pub(crate) receiver: mpsc::Receiver<Result<Vec<u8>, SendError>>,
    pub(crate) registry: Arc<SessionRegistry>,
    /// 远程 owner 的端点；`None` 表示 owner 就是本节点。
    pub(crate) owner_endpoint: Option<String>,
}

impl<S> Session<S>
where
    S: Send + Sync + 'static,
{
    /// 入站 fire-and-forget：同步返回投递状态，进入 mailbox 后不再确认。
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), SendError> {
        let runtime = self.inner.upgrade().ok_or(SendError::RuntimeStopped)?;
        // 惰性 failover 检测：resolve 到的当前 owner 与 Session 建立时不同，说明
        // Session 已随旧 owner 消亡（选 c 语义），返回可感知的错误，caller 应重新 open。
        if let Some(cluster) = &runtime.cluster {
            match cluster.resolve(&self.address, &runtime.capacity).await {
                Ok(ResolvedOwner::Remote { endpoint, .. }) => {
                    if self.owner_endpoint.as_deref() != Some(endpoint.as_str()) {
                        return Err(SendError::RemoteUnavailable);
                    }
                }
                Ok(ResolvedOwner::Local { .. }) => {
                    if self.owner_endpoint.is_some() {
                        return Err(SendError::RemoteUnavailable);
                    }
                }
                Err(error) => return Err(error),
            }
        }
        runtime
            .dispatch_action(&self.address, self.session_id, msg)
            .await
    }

    /// 出站 Event 流：`Some(Ok(bytes))` 为 Event，`Some(Err(e))` 为带原因的终止，
    /// `None` 表示 Session 已终止（failover 等无法通知的场景），应重新 `open()`。
    pub async fn recv(&mut self) -> Option<Result<Vec<u8>, SendError>> {
        self.receiver.recv().await
    }

    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl<S> Drop for Session<S>
where
    S: Send + Sync + 'static,
{
    fn drop(&mut self) {
        self.registry.unregister_local(&self.session_id);
        if let Some(endpoint) = self.owner_endpoint.clone() {
            let session_id = self.session_id;
            let inner = self.inner.clone();
            tokio::spawn(async move {
                if let Some(runtime) = inner.upgrade() {
                    let _ = runtime.notify_session_closed(&endpoint, session_id).await;
                }
            });
        } else if let Some(runtime) = self.inner.upgrade() {
            let address = self.address.clone();
            let session_id = self.session_id;
            let registry = self.registry.clone();
            tokio::spawn(async move {
                runtime
                    .close_local_session(&address, &session_id, &registry)
                    .await;
            });
        }
    }
}
