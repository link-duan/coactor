//! caller 侧 Session：入站 Action、出站 Event 流，非泛型。

use std::{
    collections::HashMap,
    sync::{Arc, Weak, atomic::Ordering},
};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{ClientInner, TransportConnection, envelope_action, envelope_session_close};
use crate::{ActorAddress, SendError, SessionId};

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

    pub(crate) fn terminate(&self, session_id: &SessionId, error: SendError) {
        if let Some(sender) = self.receivers.lock().remove(session_id) {
            let _ = sender.try_send(Err(error));
        }
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

/// A caller-side Session for sending Actions and receiving Events.
pub struct Session {
    pub(crate) client: Weak<ClientInner>,
    pub(crate) address: ActorAddress,
    pub(crate) session_id: SessionId,
    pub(crate) receiver: mpsc::Receiver<Result<Vec<u8>, SendError>>,
    pub(crate) registry: Arc<CallerRegistry>,
    pub(crate) connection: Arc<TransportConnection>,
}

impl Session {
    /// Sends a fire-and-forget Action.
    ///
    /// Success confirms transport admission, not Actor processing.
    /// A Server-side rejection may arrive asynchronously through [`Session::recv`].
    pub async fn send(&self, msg: Vec<u8>) -> Result<(), SendError> {
        let client = self.client.upgrade().ok_or(SendError::RuntimeStopped)?;
        if client.status.load(Ordering::Acquire) != super::RUNNING {
            return Err(SendError::RuntimeStopped);
        }
        self.connection
            .try_send(envelope_action(&self.address, self.session_id, msg))
    }

    /// Receives the next Event or terminal Session error.
    ///
    /// `None` means that the Session ended without a more specific error, for example
    /// after a failure that prevented the remote runtime from notifying the caller.
    /// Open a new Session to continue.
    pub async fn recv(&mut self) -> Option<Result<Vec<u8>, SendError>> {
        self.receiver.recv().await
    }

    /// Returns the identifier assigned when this Session was opened.
    pub fn session_id(&self) -> SessionId {
        self.session_id
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        self.registry.unregister_local(&self.session_id);
        self.connection.unbind(&self.session_id);
        let _ = self
            .connection
            .try_send(envelope_session_close(&self.address, self.session_id));
    }
}
