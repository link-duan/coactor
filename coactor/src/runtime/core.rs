use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};

use super::actor::{ActorRuntime, MessageContext};
use super::lifecycle::spawn_actor;
use super::message::{MailboxMessage, Registration};
use super::route::{Route, RouteState, try_send};
use super::session::{EventSink, SESSION_RECEIVER_CAPACITY, Session, SessionId, SessionRegistry};
use crate::cluster::{ClusterRouter, NodeAuthority, ResolvedOwner};
use crate::{ActorAddress, ActorId, SendError};

pub(crate) const RUNNING: u8 = 0;
pub(crate) const SHUTTING_DOWN: u8 = 1;
pub(crate) const STOPPED: u8 = 2;
pub(crate) const FENCED: u8 = 3;

pub(crate) struct RuntimeInner<S> {
    pub state: Arc<S>,
    pub registrations: HashMap<&'static str, Registration<S>>,
    pub actors: Mutex<HashMap<ActorAddress, Route>>,
    pub sessions: Arc<SessionRegistry>,
    pub capacity: Arc<Semaphore>,
    pub max_active_actors: usize,
    pub deactivation_timeout: Duration,
    pub next_generation: AtomicU64,
    pub status: AtomicU8,
    pub shutdown_timeout: Duration,
    pub peer_protocol_version: u32,
    pub authority: Option<Arc<NodeAuthority>>,
    pub cluster: Option<Arc<ClusterRouter>>,
    /// 到各远端 Node 的 bidi stream 发送端（node-pair 复用）。
    pub channels: Mutex<HashMap<String, tokio::sync::mpsc::Sender<crate::peer_protocol::Envelope>>>,
    /// 入站连接的接收 task（shutdown 时中止，确保连接关闭）。
    pub inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    /// 远程 SessionOpen 的 ack 等待表。
    pub pending_opens: Mutex<HashMap<SessionId, tokio::sync::oneshot::Sender<Result<(), SendError>>>>,
}

pub struct ActorRef<S> {
    pub(crate) runtime: Weak<RuntimeInner<S>>,
    pub(crate) address: ActorAddress,
}

impl<S> Clone for ActorRef<S> {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            address: self.address.clone(),
        }
    }
}

impl<S> ActorRef<S>
where
    S: Send + Sync + 'static,
{
    /// 建立与 Actor 的持久 Session：激活 Actor（若未激活）→ 注册 Session →
    /// 执行 `on_session_opened` → 返回。失败时同步返回对应错误并清理注册。
    pub async fn open(&self) -> Result<Session<S>, SendError> {
        let runtime = self.runtime.upgrade().ok_or(SendError::RuntimeStopped)?;
        let session_id = SessionId::new();
        let (sender, receiver) = mpsc::channel(SESSION_RECEIVER_CAPACITY);
        runtime.sessions.register_local(session_id, sender);

        let cleanup = |runtime: &Arc<RuntimeInner<S>>| {
            runtime.sessions.unregister_local(&session_id);
            runtime.sessions.unregister_actor(&self.address, &session_id);
        };

        let owner_endpoint = match &runtime.cluster {
            None => match runtime
                .open_local_session(&self.address, session_id, EventSink::Local, None, None)
                .await
            {
                Ok(Ok(())) => None,
                Ok(Err(error)) | Err(error) => {
                    cleanup(&runtime);
                    return Err(error);
                }
            },
            Some(cluster) => {
                let caller_endpoint = match runtime.local_node_endpoint() {
                    Some(endpoint) => endpoint,
                    None => {
                        cleanup(&runtime);
                        return Err(SendError::OwnershipUnavailable);
                    }
                };
                match cluster.resolve(&self.address, &runtime.capacity).await {
                    Ok(ResolvedOwner::Local { reservation, guard }) => match runtime
                        .open_local_session(
                            &self.address,
                            session_id,
                            EventSink::Local,
                            reservation,
                            Some(guard),
                        )
                        .await
                    {
                        Ok(Ok(())) => None,
                        Ok(Err(error)) | Err(error) => {
                            cleanup(&runtime);
                            return Err(error);
                        }
                    },
                    Ok(ResolvedOwner::Remote { endpoint, .. }) => {
                        match runtime
                            .open_remote_session(
                                &self.address,
                                session_id,
                                caller_endpoint,
                                &endpoint,
                            )
                            .await
                        {
                            Ok(Ok(())) => Some(endpoint),
                            Ok(Err(error)) | Err(error) => {
                                cleanup(&runtime);
                                return Err(error);
                            }
                        }
                    }
                    Err(error) => {
                        cleanup(&runtime);
                        return Err(error);
                    }
                }
            }
        };

        Ok(Session {
            inner: self.runtime.clone(),
            address: self.address.clone(),
            session_id,
            receiver,
            registry: runtime.sessions.clone(),
            owner_endpoint,
        })
    }

    pub fn actor_address(&self) -> &ActorAddress {
        &self.address
    }
}

impl<S> RuntimeInner<S>
where
    S: Send + Sync + 'static,
{
    pub(crate) fn local_node_endpoint(&self) -> Option<String> {
        self.cluster.as_ref().map(|cluster| cluster.local_node_endpoint())
    }

    pub(crate) async fn broadcast_event(&self, address: &ActorAddress, payload: Vec<u8>) {
        let session_ids: Vec<SessionId> = self.sessions.by_actor_snapshot(address);
        for session_id in session_ids {
            if let Err(error) = self
                .sessions
                .deliver_event(address, session_id, payload.clone())
                .await
            {
                tracing::debug!(
                    actor_type = address.actor_type(),
                    actor_id = ?address.actor_id(),
                    session_id = ?session_id,
                    error_category = "EventDeliveryFailed",
                    error = ?error,
                    "broadcast Event delivery skipped"
                );
            }
        }
    }

    /// 本地投递一条 Action：检查 Session 存活后进入 mailbox（含懒激活）。
    pub(crate) async fn dispatch_action(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        payload: Vec<u8>,
    ) -> Result<(), SendError> {
        match &self.cluster {
            None => {
                if self.sessions.sink(address, &session_id).is_none() {
                    return Err(SendError::ActorStopped);
                }
                self.dispatch_message(
                    address,
                    MailboxMessage::Action { session_id, payload },
                    None,
                    None,
                )
            }
            Some(cluster) => match cluster.resolve(address, &self.capacity).await {
                Ok(ResolvedOwner::Local { reservation, guard }) => {
                    if self.sessions.sink(address, &session_id).is_none() {
                        return Err(SendError::ActorStopped);
                    }
                    self.dispatch_message(
                        address,
                        MailboxMessage::Action { session_id, payload },
                        reservation,
                        Some(guard),
                    )
                }
                Ok(ResolvedOwner::Remote { endpoint, .. }) => {
                    let channel = self.ensure_channel(&endpoint).await?;
                    channel
                        .try_send(envelope_action(
                            address,
                            session_id,
                            payload,
                            self.peer_protocol_version,
                        ))
                        .map_err(|_| SendError::RemoteUnavailable)
                }
                Err(error) => Err(error),
            },
        }
    }

    pub(crate) fn dispatch_message(
        self: &Arc<Self>,
        address: &ActorAddress,
        mut message: MailboxMessage,
        reservation: Option<OwnedSemaphorePermit>,
        guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<(), SendError> {
        if self
            .authority
            .as_ref()
            .is_some_and(|authority| !authority.is_valid())
        {
            return Err(SendError::NodeFenced);
        }
        match self.status.load(Ordering::Acquire) {
            RUNNING => {}
            SHUTTING_DOWN => return Err(SendError::RuntimeShuttingDown),
            FENCED => return Err(SendError::NodeFenced),
            _ => return Err(SendError::RuntimeStopped),
        }
        let mut actors = self.actors.lock();
        if let Some(route) = actors.get(address) {
            match &route.state {
                RouteState::Active => match route.task.sender.try_send(message) {
                    Ok(()) => return Ok(()),
                    Err(mpsc::error::TrySendError::Full(message)) => {
                        if let MailboxMessage::SessionOpened { complete, .. } = message {
                            let _ = complete.send(Err(SendError::MailboxFull));
                        }
                        return Err(SendError::MailboxFull);
                    }
                    Err(mpsc::error::TrySendError::Closed(returned)) => {
                        let generation = route.generation;
                        actors.remove(address);
                        message = returned;
                        tracing::debug!(
                            actor_type = address.actor_type(),
                            actor_id = ?address.actor_id(),
                            generation,
                            lifecycle = "routing",
                            error_category = "ClosedRouteReplaced",
                            "Replacing a closed Actor route"
                        );
                    }
                },
                RouteState::Deactivating => return Err(SendError::ActorDeactivating),
            }
        }

        let permit = match reservation {
            Some(permit) => permit,
            None => match self.capacity.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    self.fail_message(message, SendError::RuntimeAtCapacity);
                    return Err(SendError::RuntimeAtCapacity);
                }
            },
        };
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let spawned = spawn_actor(self.clone(), address.clone(), generation);
        let result = try_send(&spawned.sender, message);
        actors.insert(
            address.clone(),
            Route {
                generation,
                state: RouteState::Active,
                _capacity: permit,
                task: spawned,
            },
        );
        drop(guard);
        result
    }

    /// 仅投递到已激活的 Actor；未激活时静默丢弃（用于 SessionClosed 等控制消息）。
    pub(crate) fn dispatch_message_if_active(
        &self,
        address: &ActorAddress,
        message: MailboxMessage,
    ) -> Result<(), SendError> {
        let actors = self.actors.lock();
        if let Some(route) = actors.get(address) {
            if let RouteState::Active = &route.state {
                return route.task.sender.try_send(message).map_err(|error| match error {
                    mpsc::error::TrySendError::Full(_) => SendError::MailboxFull,
                    mpsc::error::TrySendError::Closed(_) => SendError::ActorStopped,
                });
            }
        }
        Ok(())
    }

    fn fail_message(&self, message: MailboxMessage, error: SendError) {
        if let MailboxMessage::SessionOpened { complete, .. } = message {
            let _ = complete.send(Err(error));
        }
    }

    pub(crate) async fn close_local_session(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: &SessionId,
        registry: &Arc<SessionRegistry>,
    ) {
        if registry.unregister_actor(address, session_id) {
            let _ = self.dispatch_message_if_active(
                address,
                MailboxMessage::SessionClosed {
                    session_id: *session_id,
                },
            );
        }
    }

    pub(crate) async fn open_local_session(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        sink: EventSink,
        reservation: Option<OwnedSemaphorePermit>,
        guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    ) -> Result<Result<(), SendError>, SendError> {
        if !self.sessions.register_actor(address, session_id, sink.clone()) {
            return Ok(Err(SendError::ActorStopped));
        }
        let (complete, receive) = oneshot::channel();
        let message = MailboxMessage::SessionOpened {
            session_id,
            complete,
        };
        self.dispatch_message(address, message, reservation, guard)?;
        let outcome = receive.await.map_err(|_| SendError::ActorStopped)?;
        Ok(outcome)
    }

    /// owner 侧处理远程 SessionOpen：以入站流的 outbound 作为回传路径 → 注册 → 激活 → `on_session_opened`。
    pub(crate) async fn dispatch_remote_open(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        sink: EventSink,
    ) -> Result<Result<(), SendError>, SendError> {
        let outcome = self
            .open_local_session(address, session_id, sink, None, None)
            .await?;
        Ok(outcome)
    }

    /// owner 侧处理远程 Action / SessionClose。
    pub(crate) async fn dispatch_remote_message(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        payload: Option<Vec<u8>>,
    ) -> Result<(), SendError> {
        match payload {
            Some(payload) => {
                if self.sessions.sink(address, &session_id).is_none() {
                    return Err(SendError::ActorStopped);
                }
                self.dispatch_message(
                    address,
                    MailboxMessage::Action { session_id, payload },
                    None,
                    None,
                )
            }
            None => {
                self.close_local_session(address, &session_id, &self.sessions)
                    .await;
                Ok(())
            }
        }
    }

    pub(crate) async fn notify_channel_closed(&self, endpoint: &str) {
        self.channels.lock().remove(endpoint);
        if let Some(cluster) = &self.cluster {
            cluster.invalidate_endpoint(endpoint).await;
        }
    }

    pub(crate) fn register_inbound_task(&self, handle: tokio::task::AbortHandle) {
        self.inbound_tasks.lock().push(handle);
    }

    pub(crate) fn abort_inbound_tasks(&self) {
        for handle in self.inbound_tasks.lock().drain(..) {
            handle.abort();
        }
    }

    pub(crate) async fn ensure_channel(
        self: &Arc<Self>,
        endpoint: &str,
    ) -> Result<tokio::sync::mpsc::Sender<crate::peer_protocol::Envelope>, SendError> {
        if let Some(sender) = self.channels.lock().get(endpoint) {
            return Ok(sender.clone());
        }
        let sender = crate::cluster::connect_channel(self, endpoint).await?;
        self.channels.lock().insert(endpoint.to_owned(), sender.clone());
        Ok(sender)
    }

    /// caller 侧远程 open：发 SessionOpen → 等待 owner 的 ack（激活 + `on_session_opened` 完成）。
    pub(crate) async fn open_remote_session(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        caller_endpoint: String,
        endpoint: &str,
    ) -> Result<Result<(), SendError>, SendError> {
        let channel = self.ensure_channel(endpoint).await?;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_opens.lock().insert(session_id, ack_tx);
        let envelope = envelope_session_open(address, session_id, caller_endpoint, self.peer_protocol_version);
        if channel.try_send(envelope).is_err() {
            self.pending_opens.lock().remove(&session_id);
            return Err(SendError::RemoteUnavailable);
        }
        let timeout = self
            .cluster
            .as_ref()
            .map_or(Duration::from_secs(3), |cluster| cluster.peer_connect_timeout);
        match tokio::time::timeout(timeout, ack_rx).await {
            Ok(Ok(result)) => Ok(result),
            Ok(Err(_)) => Err(SendError::RemoteUnavailable),
            Err(_) => Err(SendError::RemoteUnavailable),
        }
    }

    pub(crate) async fn notify_session_closed(self: &Arc<Self>, endpoint: &str, session_id: SessionId) {
        let Ok(channel) = self.ensure_channel(endpoint).await else {
            return;
        };
        let _ = channel.try_send(envelope_session_close(session_id));
    }

    /// 处理节点对间收到的 Envelope；`reply` 为入站流的 outbound（owner 侧回发 ack/Event 用）。
    pub(crate) async fn dispatch_inbound(
        self: &Arc<Self>,
        envelope: crate::peer_protocol::Envelope,
        reply: Option<tokio::sync::mpsc::Sender<crate::peer_protocol::Envelope>>,
    ) {
        use crate::peer_protocol::envelope::Kind;
        let Some(kind) = envelope.kind else {
            return;
        };
        let Some(session_id) = SessionId::from_bytes(&envelope.session_id) else {
            return;
        };
        let address = ActorAddress::new(envelope.actor_type, ActorId::new(envelope.actor_id));
        let version_ok = envelope.protocol_version == self.peer_protocol_version;
        match kind {
            Kind::Action(action) => {
                if !version_ok {
                    return;
                }
                let _ = self
                    .dispatch_remote_message(&address, session_id, Some(action.payload))
                    .await;
            }
            Kind::SessionOpen(_open) => {
                let result = if version_ok {
                    match reply.clone() {
                        Some(sender) => self
                            .dispatch_remote_open(
                                &address,
                                session_id,
                                EventSink::Remote { sender },
                            )
                            .await,
                        None => Ok(Err(SendError::RemoteUnavailable)),
                    }
                } else {
                    Ok(Err(SendError::RemoteProtocol(
                        crate::RemoteProtocolError::VersionMismatch,
                    )))
                };
                if let Some(sender) = reply {
                    let outcome = match result {
                        Ok(Ok(())) => Some(
                            crate::peer_protocol::session_opened_ack::Outcome::Ok(Vec::new()),
                        ),
                        Ok(Err(error)) | Err(error) => Some(
                            crate::peer_protocol::session_opened_ack::Outcome::Failure(
                                error.to_wire(),
                            ),
                        ),
                    };
                    let _ = sender.try_send(crate::peer_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        from_node: String::new(),
                        kind: Some(Kind::SessionOpenedAck(
                            crate::peer_protocol::SessionOpenedAck { outcome },
                        )),
                    });
                }
            }
            Kind::SessionClose(_) => {
                let _ = self
                    .dispatch_remote_message(&address, session_id, None)
                    .await;
            }
            Kind::Event(event) => {
                if let Some(sender) = self.sessions.receiver(&session_id) {
                    let _ = sender.try_send(Ok(event.payload));
                }
            }
            Kind::SessionError(error) => {
                if let Some(sender) = self.sessions.receiver(&session_id) {
                    let _ = sender.try_send(Err(SendError::from_wire(error.failure)));
                }
            }
            Kind::SessionOpenedAck(ack) => {
                let result = match ack.outcome {
                    Some(crate::peer_protocol::session_opened_ack::Outcome::Ok(_)) => Ok(()),
                    Some(crate::peer_protocol::session_opened_ack::Outcome::Failure(failure)) => {
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
        }
    }
}

impl<S> RuntimeInner<S>
where
    S: Send + Sync + 'static,
{
    pub(crate) fn make_message_context(&self, address: &ActorAddress, session_id: SessionId) -> MessageContext {
        MessageContext {
            address: address.clone(),
            session: super::session::SessionHandle {
                registry: self.sessions.clone(),
                address: address.clone(),
                session_id,
            },
        }
    }

    pub(crate) fn make_actor_runtime(self: &Arc<Self>, address: &ActorAddress) -> ActorRuntime<S> {
        ActorRuntime {
            address: address.clone(),
            state: self.state.clone(),
            runtime: Arc::downgrade(self),
        }
    }
}

fn envelope_action(
    address: &ActorAddress,
    session_id: SessionId,
    payload: Vec<u8>,
    protocol_version: u32,
) -> crate::peer_protocol::Envelope {
    crate::peer_protocol::Envelope {
        protocol_version,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(crate::peer_protocol::envelope::Kind::Action(
            crate::peer_protocol::ActionMessage { payload },
        )),
    }
}

fn envelope_session_open(
    address: &ActorAddress,
    session_id: SessionId,
    caller_endpoint: String,
    protocol_version: u32,
) -> crate::peer_protocol::Envelope {
    crate::peer_protocol::Envelope {
        protocol_version,
        actor_type: address.actor_type().to_owned(),
        actor_id: address.actor_id().as_bytes().to_vec(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(crate::peer_protocol::envelope::Kind::SessionOpen(
            crate::peer_protocol::SessionOpen { caller_endpoint },
        )),
    }
}

fn envelope_session_close(session_id: SessionId) -> crate::peer_protocol::Envelope {
    crate::peer_protocol::Envelope {
        protocol_version: 0,
        actor_type: String::new(),
        actor_id: Vec::new(),
        session_id: session_id.as_bytes(),
        from_node: String::new(),
        kind: Some(crate::peer_protocol::envelope::Kind::SessionClose(
            crate::peer_protocol::SessionClose {
                reason: crate::peer_protocol::CloseReason::CallerDropped as i32,
            },
        )),
    }
}
