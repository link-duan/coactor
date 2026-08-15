use std::{
    collections::HashMap,
    sync::{
        Arc,
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
use super::session::{EventSink, SessionId, SessionRegistry};
use crate::cluster::{ClusterRouter, NodeAuthority, ResolvedOwner};
use crate::transport::{ClientTransport, Endpoint, PeerSender};
use crate::{ActorAddress, ActorId, SendError};

pub(crate) const RUNNING: u8 = 0;
pub(crate) const SHUTTING_DOWN: u8 = 1;
pub(crate) const STOPPED: u8 = 2;
pub(crate) const FENCED: u8 = 3;

pub(crate) struct ServerInner<S> {
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
    pub channels: Mutex<HashMap<String, Arc<dyn PeerSender>>>,
    /// 入站连接的接收 task（shutdown 时中止，确保连接关闭）。
    pub inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
    /// 远程 SessionOpen 的 ack 等待表。
    pub pending_opens: Mutex<HashMap<SessionId, tokio::sync::oneshot::Sender<Result<(), SendError>>>>,
    /// 出站连接用的 client transport（网关转发）；None = 本地单节点。
    pub client_transport: Option<Arc<dyn ClientTransport>>,
    /// 网关转发的中继表：session_id → (owner 端点, caller 回传 sender)。
    pub relays: Mutex<HashMap<SessionId, Relay>>,
}

/// 网关中继条目：owner 侧转发目标 + caller 侧回传路径。
#[derive(Clone)]
pub(crate) struct Relay {
    pub owner: String,
    pub client: Arc<dyn PeerSender>,
}


impl<S> ServerInner<S>
where
    S: Send + Sync + 'static,
{
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
        if !self.registrations.contains_key(address.actor_type()) {
            return Ok(Err(SendError::ActorTypeNotRegistered(
                address.actor_type().to_owned(),
            )));
        }
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
        // 网关：指向该端点的 relay 会话失效，通知 caller 重开。
        let stale: Vec<(SessionId, Relay)> = self
            .relays
            .lock()
            .iter()
            .filter(|(_, relay)| relay.owner == endpoint)
            .map(|(id, relay)| (*id, relay.clone()))
            .collect();
        drop(self.relays.lock());
        for (session_id, relay) in stale {
            self.relays.lock().remove(&session_id);
            let _ = relay.client.try_send(crate::peer_protocol::Envelope {
                protocol_version: 0,
                actor_type: String::new(),
                actor_id: Vec::new(),
                session_id: session_id.as_bytes(),
                from_node: String::new(),
                kind: Some(crate::peer_protocol::envelope::Kind::SessionError(
                    crate::peer_protocol::SessionError {
                        failure: SendError::RemoteUnavailable.to_wire(),
                    },
                )),
            });
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
    ) -> Result<Arc<dyn PeerSender>, SendError> {
        if let Some(sender) = self.channels.lock().get(endpoint) {
            return Ok(sender.clone());
        }
        let Some(transport) = &self.client_transport else {
            return Err(SendError::RemoteUnavailable);
        };
        let stream = transport
            .connect(&Endpoint::new(endpoint))
            .await
            .map_err(|_| SendError::RemoteUnavailable)?;
        let sender = stream.sender();
        let runtime = self.clone();
        let closed_endpoint = endpoint.to_owned();
        let loop_sender = sender.clone();
        tokio::spawn(async move {
            let mut stream = stream;
            while let Some(envelope) = stream.recv().await {
                runtime
                    .handle_owner_envelope(envelope, Some(loop_sender.clone()))
                    .await;
            }
            runtime.notify_channel_closed(&closed_endpoint).await;
        });
        self.channels.lock().insert(endpoint.to_owned(), sender.clone());
        Ok(sender)
    }

    /// outbound channel 收包：owner 侧完整处理器。
    /// 处理转发来的 SessionOpen/Action/SessionClose（宿主）与 owner 回复
    /// （ack/Event/SessionError，中继或投递本地 receiver）。
    /// 不含转发路径（不调用 ensure_channel），避免与网关 accept 侧形成循环依赖。
    async fn handle_owner_envelope(
        self: &Arc<Self>,
        envelope: crate::peer_protocol::Envelope,
        reply: Option<Arc<dyn PeerSender>>,
    ) {
        use crate::peer_protocol::envelope::Kind;
        let Some(kind) = envelope.kind else { return };
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
                if let Err(error) = self
                    .dispatch_remote_message(&address, session_id, Some(action.payload))
                    .await
                {
                    if let Some(sender) = reply {
                        let _ = sender.try_send(crate::peer_protocol::Envelope {
                            protocol_version: 0,
                            actor_type: String::new(),
                            actor_id: Vec::new(),
                            session_id: envelope.session_id,
                            from_node: String::new(),
                            kind: Some(Kind::SessionError(crate::peer_protocol::SessionError {
                                failure: error.to_wire(),
                            })),
                        });
                    }
                }
            }
            Kind::SessionOpen(_) => {
                let result = if version_ok {
                    match reply.clone() {
                        Some(sender) => {
                            // 转发来的会话：确认自己是 owner 后宿主；属他人则拒绝（不二次转发，防环）。
                            let outcome = if let Some(cluster) = &self.cluster {
                                match cluster.resolve(&address, &self.capacity).await {
                                    Ok(ResolvedOwner::Local { reservation, guard }) => {
                                        self.open_local_session(
                                            &address,
                                            session_id,
                                            EventSink::Remote { sender },
                                            reservation,
                                            Some(guard),
                                        )
                                        .await
                                    }
                                    _ => Ok(Err(SendError::NotOwner)),
                                }
                            } else {
                                self.open_local_session(
                                    &address,
                                    session_id,
                                    EventSink::Remote { sender },
                                    None,
                                    None,
                                )
                                .await
                            };
                            outcome
                        }
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
                self.close_local_session(&address, &session_id, &self.sessions)
                    .await;
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
            Kind::Event(event) => {
                let relay = self.relays.lock().get(&session_id).cloned();
                if let Some(relay) = relay {
                    let _ = relay.client.try_send(crate::peer_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        from_node: String::new(),
                        kind: Some(Kind::Event(event)),
                    });
                }
            }
            Kind::SessionError(error) => {
                let relay = self.relays.lock().get(&session_id).cloned();
                if let Some(relay) = relay {
                    let _ = relay.client.try_send(crate::peer_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        from_node: String::new(),
                        kind: Some(Kind::SessionError(error)),
                    });
                }
            }
        }
    }

    /// 网关：把 SessionOpen 转发给 owner，等 ack 后回传 caller；失败则清理中继。
    pub(crate) async fn forward_session_open(
        self: &Arc<Self>,
        address: &ActorAddress,
        session_id: SessionId,
        owner: String,
        client: Arc<dyn PeerSender>,
    ) -> Result<Result<(), SendError>, SendError> {
        let channel = self.ensure_channel(&owner).await?;
        let (ack_tx, ack_rx) = oneshot::channel();
        self.pending_opens.lock().insert(session_id, ack_tx);
        self.relays.lock().insert(
            session_id,
            Relay {
                owner: owner.clone(),
                client,
            },
        );
        let envelope = envelope_session_open(
            address,
            session_id,
            String::new(),
            self.peer_protocol_version,
        );
        if channel.try_send(envelope).is_err() {
            self.pending_opens.lock().remove(&session_id);
            self.relays.lock().remove(&session_id);
            return Ok(Err(SendError::RemoteUnavailable));
        }
        let result = match tokio::time::timeout(Duration::from_secs(3), ack_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) | Err(_) => {
                self.relays.lock().remove(&session_id);
                Err(SendError::RemoteUnavailable)
            }
        };
        Ok(result)
    }

    /// 经网关 channel 发送 Envelope 到 owner。
    pub(crate) async fn relay_send(
        self: &Arc<Self>,
        endpoint: &str,
        envelope: crate::peer_protocol::Envelope,
    ) -> Result<(), SendError> {
        let channel = self.ensure_channel(endpoint).await;
        let channel = channel?;
        channel
            .try_send(envelope)
            .map_err(|_| SendError::RemoteUnavailable)
    }

    /// caller 侧入站流断开：终结经由该流中转的 relay（向 owner 发 SessionClose）。
    pub(crate) async fn close_relays_for_sender(self: &Arc<Self>, sender: &Arc<dyn PeerSender>) {
        let stale: Vec<(SessionId, Relay)> = self
            .relays
            .lock()
            .iter()
            .filter(|(_, relay)| Arc::ptr_eq(&relay.client, sender))
            .map(|(id, relay)| (*id, relay.clone()))
            .collect();
        drop(self.relays.lock());
        for (session_id, relay) in stale {
            self.relays.lock().remove(&session_id);
            let _ = self
                .relay_send(relay.owner.as_str(), envelope_session_close(session_id))
                .await;
        }
    }

    /// 处理节点对间收到的 Envelope；`reply` 为入站流的 outbound（owner 侧回发 ack/Event 用）。
    pub(crate) async fn dispatch_inbound(
        self: &Arc<Self>,
        envelope: crate::peer_protocol::Envelope,
        reply: Option<Arc<dyn PeerSender>>,
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
                let relay = self.relays.lock().get(&session_id).cloned();
                if let Some(relay) = relay {
                    // 网关：Action 转发给 owner
                    let _ = self
                        .relay_send(
                            relay.owner.as_str(),
                            crate::client::envelope_action(&address, session_id, action.payload),
                        )
                        .await;
                    return;
                }
                if let Err(error) = self
                    .dispatch_remote_message(&address, session_id, Some(action.payload))
                    .await
                {
                    // 网关模型：send() 只反映传输投递；server 侧拒绝（MailboxFull 等）
                    // 经 SessionError 异步通知 caller，不终止 Session。
                    if let Some(reply) = reply {
                        let _ = reply.try_send(crate::peer_protocol::Envelope {
                            protocol_version: 0,
                            actor_type: String::new(),
                            actor_id: Vec::new(),
                            session_id: envelope.session_id,
                            from_node: String::new(),
                            kind: Some(crate::peer_protocol::envelope::Kind::SessionError(
                                crate::peer_protocol::SessionError {
                                    failure: error.to_wire(),
                                },
                            )),
                        });
                    }
                }
            }
            Kind::SessionOpen(_open) => {
                let result = if version_ok {
                    match reply.clone() {
                        Some(sender) => {
                            // 网关语义：入站会话先 resolve——未拥有/stale 就地 claim，
                            // 属他人则建中继转发给 owner；无 router 的单节点（TestServer）直接宿主。
                            let outcome = if let Some(cluster) = &self.cluster {
                                match cluster.resolve(&address, &self.capacity).await {
                                    Ok(ResolvedOwner::Local { reservation, guard }) => {
                                        self.open_local_session(
                                            &address,
                                            session_id,
                                            EventSink::Remote { sender },
                                            reservation,
                                            Some(guard),
                                        )
                                        .await
                                    }
                                    Ok(ResolvedOwner::Remote { endpoint, .. }) => {
                                        self.forward_session_open(
                                            &address,
                                            session_id,
                                            endpoint,
                                            sender,
                                        )
                                        .await
                                    }
                                    Err(error) => Ok(Err(error)),
                                }
                            } else {
                                self.open_local_session(
                                    &address,
                                    session_id,
                                    EventSink::Remote { sender },
                                    None,
                                    None,
                                )
                                .await
                            };
                            outcome
                        }
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
                let relay = self.relays.lock().remove(&session_id);
                if let Some(relay) = relay {
                    let _ = self
                        .relay_send(relay.owner.as_str(), envelope_session_close(session_id))
                        .await;
                } else {
                    let _ = self
                        .dispatch_remote_message(&address, session_id, None)
                        .await;
                }
            }
            Kind::Event(event) => {
                let relay = self.relays.lock().get(&session_id).cloned();
                if let Some(relay) = relay {
                    // owner → 网关 → caller
                    let _ = relay.client.try_send(crate::peer_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        from_node: String::new(),
                        kind: Some(crate::peer_protocol::envelope::Kind::Event(event)),
                    });
                }
            }
            Kind::SessionError(error) => {
                let relay = self.relays.lock().get(&session_id).cloned();
                if let Some(relay) = relay {
                    let _ = relay.client.try_send(crate::peer_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        from_node: String::new(),
                        kind: Some(crate::peer_protocol::envelope::Kind::SessionError(error)),
                    });
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

impl<S> ServerInner<S>
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

