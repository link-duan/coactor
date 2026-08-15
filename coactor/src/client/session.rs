//! caller 侧 Session：入站 Action、出站 Event 流，非泛型。

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{ClientInner, envelope_action};
use crate::peer_protocol::Envelope;
use crate::{ActorAddress, SendError, SessionId, transport::Endpoint};

type EventSender = mpsc::Sender<Result<Vec<u8>, SendError>>;

/// caller 侧的 Session 注册表：session_id → 本地接收流（Event 投递、清理）。
pub(crate) struct CallerRegistry {
    receivers: Mutex<HashMap<SessionId, EventSender>>,
}

impl CallerRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            receivers: Mutex::new(HashMap::new()),
        })
    }

    pub(crate) fn register_local(&self, session_id: SessionId, sender: EventSender) {
        self.receivers.lock().insert(session_id, sender);
    }

    pub(crate) fn unregister_local(&self, session_id: &SessionId) {
        self.receivers.lock().remove(session_id);
    }

    pub(crate) fn receiver(&self, session_id: &SessionId) -> Option<EventSender> {
        self.receivers.lock().get(session_id).cloned()
    }

    /// 以终止错误结束全部 Session（shutdown 用）。
    pub(crate) fn terminate_all(&self, error: SendError) {
        let senders: Vec<EventSender> = self.receivers.lock().values().cloned().collect();
        self.receivers.lock().clear();
        for sender in senders {
            let _ = sender.try_send(Err(error.clone()));
        }
    }
}

/// caller 侧持有的持久 Session：入站发送 Action，出站接收 Event 流。
pub struct Session {
    pub(crate) client: Weak<ClientInner>,
    pub(crate) address: ActorAddress,
    pub(crate) session_id: SessionId,
    pub(crate) receiver: mpsc::Receiver<Result<Vec<u8>, SendError>>,
    pub(crate) registry: Arc<CallerRegistry>,
    /// 建立时的 owner 端点（Direct 模式恒为配置端点）。
    pub(crate) owner_endpoint: Option<Endpoint>,
}

impl Session {
    /// 入站 fire-and-forget：同步返回传输投递状态，进入网关通道后不再确认；
    /// server 侧拒绝（如 MailboxFull）经 SessionError 异步通知。
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), SendError> {
        let client = self.client.upgrade().ok_or(SendError::RuntimeStopped)?;
        let endpoint = self
            .owner_endpoint
            .clone()
            .ok_or(SendError::RemoteUnavailable)?;
        let channel = client.ensure_channel(&endpoint).await?;
        channel
            .try_send(envelope_action(&self.address, self.session_id, msg))
            .map_err(|_| SendError::RemoteUnavailable)
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

impl Drop for Session {
    fn drop(&mut self) {
        self.registry.unregister_local(&self.session_id);
        let Some(endpoint) = self.owner_endpoint.clone() else { return };
        let client = self.client.clone();
        let address = self.address.clone();
        let session_id = self.session_id;
        tokio::spawn(async move {
            let Some(client) = client.upgrade() else { return };
            let Ok(channel) = client.ensure_channel(&endpoint).await else {
                return;
            };
            let _ = channel.try_send(Envelope {
                protocol_version: 0,
                actor_type: address.actor_type().to_owned(),
                actor_id: address.actor_id().as_bytes().to_vec(),
                session_id: session_id.as_bytes(),
                from_node: String::new(),
                kind: Some(crate::peer_protocol::envelope::Kind::SessionClose(
                    crate::peer_protocol::SessionClose {
                        reason: crate::peer_protocol::CloseReason::CallerDropped as i32,
                    },
                )),
            });
        });
    }
}
