use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use uuid::Uuid;

use crate::transport::PeerSender;
use crate::{ActorAddress, SendError};

/// 本节点 Session 接收流的容量（出站 Event 背压上界）。
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

/// Session 的 Event 回传路径（owner 节点视角）：经该节点对间的 bidi stream 发送端回传。
#[derive(Clone)]
pub(crate) enum EventSink {
    Remote { sender: Arc<dyn PeerSender> },
}

/// Server 侧 Session 注册表：actor → 存活 Session 的回传路径。
/// 独立于 `ServerInner` 的泛型参数，因此 `SessionHandle` 可以是非泛型的。
pub(crate) struct SessionRegistry {
    by_actor: Mutex<HashMap<ActorAddress, HashMap<SessionId, EventSink>>>,
}

impl SessionRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            by_actor: Mutex::new(HashMap::new()),
        })
    }

    /// owner 侧注册一个 Actor 的存活 Session；重复注册返回 false。
    pub(crate) fn register_actor(
        &self,
        address: &ActorAddress,
        session_id: SessionId,
        sink: EventSink,
    ) -> bool {
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
        if removed
            && by_actor
                .get(address)
                .is_some_and(|sessions| sessions.is_empty())
        {
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
            Some(EventSink::Remote { sender }) => sender
                .try_send(crate::peer_protocol::Envelope {
                    protocol_version: 0,
                    actor_type: String::new(),
                    actor_id: Vec::new(),
                    session_id: session_id.as_bytes(),
                    from_node: String::new(),
                    kind: Some(crate::peer_protocol::envelope::Kind::Event(
                        crate::peer_protocol::EventMessage { payload },
                    )),
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
            Some(EventSink::Remote { sender }) => {
                let _ = sender.try_send(crate::peer_protocol::Envelope {
                    protocol_version: 0,
                    actor_type: String::new(),
                    actor_id: Vec::new(),
                    session_id: session_id.as_bytes(),
                    from_node: String::new(),
                    kind: Some(crate::peer_protocol::envelope::Kind::SessionError(
                        crate::peer_protocol::SessionError {
                            failure: error.to_wire(),
                        },
                    )),
                });
            }
            None => {}
        }
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
