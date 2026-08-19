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
use crate::transport::TransportSender;
use crate::{ActorAddress, SendError};

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
    pub transport_protocol_version: u32,
    pub authority: Option<Arc<NodeAuthority>>,
    pub cluster: Option<Arc<ClusterRouter>>,
    /// Inbound Transport Connection receive tasks, aborted during shutdown.
    pub inbound_tasks: Mutex<Vec<tokio::task::AbortHandle>>,
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

    /// Delivers only to an existing Active Actor; inactive addresses are ignored.
    pub(crate) fn dispatch_message_if_active(
        &self,
        address: &ActorAddress,
        message: MailboxMessage,
    ) -> Result<(), SendError> {
        let actors = self.actors.lock();
        if let Some(route) = actors.get(address) {
            if let RouteState::Active = &route.state {
                return route
                    .task
                    .sender
                    .try_send(message)
                    .map_err(|error| match error {
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
        if !self.sessions.register_actor(address, session_id, sink) {
            return Ok(Err(SendError::ActorStopped));
        }
        let (complete, receive) = oneshot::channel();
        let message = MailboxMessage::SessionOpened {
            session_id,
            complete,
        };
        if let Err(error) = self.dispatch_message(address, message, reservation, guard) {
            self.sessions.unregister_actor(address, &session_id);
            return Err(error);
        }
        let outcome = match receive.await {
            Ok(outcome) => outcome,
            Err(_) => {
                self.sessions.unregister_actor(address, &session_id);
                return Err(SendError::ActorStopped);
            }
        };
        if outcome.is_err() {
            self.sessions.unregister_actor(address, &session_id);
        }
        Ok(outcome)
    }

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
                    MailboxMessage::Action {
                        session_id,
                        payload,
                    },
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

    pub(crate) fn register_inbound_task(&self, handle: tokio::task::AbortHandle) {
        let mut tasks = self.inbound_tasks.lock();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    pub(crate) fn retain_inbound_tasks(&self) {
        self.inbound_tasks.lock().retain(|task| !task.is_finished());
    }

    pub(crate) fn abort_inbound_tasks(&self) {
        for handle in self.inbound_tasks.lock().drain(..) {
            handle.abort();
        }
    }

    /// A direct Client Transport Connection closed; close only Sessions bound to it.
    pub(crate) async fn close_sessions_for_sender(
        self: &Arc<Self>,
        sender: &Arc<dyn TransportSender>,
    ) {
        for (address, session_id) in self.sessions.by_sender_snapshot(sender) {
            self.close_local_session(&address, &session_id, &self.sessions)
                .await;
        }
    }

    /// Handles one direct Client Envelope. Servers never forward to another Server.
    pub(crate) async fn dispatch_inbound(
        self: &Arc<Self>,
        envelope: crate::transport_protocol::Envelope,
        reply: Option<Arc<dyn TransportSender>>,
    ) {
        use crate::transport_protocol::envelope::Kind;
        let Some(kind) = envelope.kind else { return };
        let Some(session_id) = SessionId::from_bytes(&envelope.session_id) else {
            return;
        };
        let Some(address) = actor_address_from_envelope(envelope.actor_type, envelope.actor_id)
        else {
            return;
        };
        let version_ok = envelope.protocol_version == self.transport_protocol_version;
        match kind {
            Kind::Action(action) => {
                if !version_ok {
                    return;
                }
                if !reply
                    .as_ref()
                    .is_some_and(|sender| self.sessions.is_bound_to(&address, &session_id, sender))
                {
                    send_session_error(reply, envelope.session_id, SendError::ActorStopped);
                    return;
                }
                if let Err(error) = self
                    .dispatch_remote_message(&address, session_id, Some(action.payload))
                    .await
                {
                    send_session_error(reply, envelope.session_id, error);
                }
            }
            Kind::SessionOpen(_) => {
                let result = if !version_ok {
                    Ok(Err(SendError::RemoteProtocol(
                        crate::RemoteProtocolError::VersionMismatch,
                    )))
                } else if let Some(sender) = reply.clone() {
                    if let Some(cluster) = &self.cluster {
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
                            Ok(ResolvedOwner::Remote) => Ok(Err(SendError::NotOwner)),
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
                    }
                } else {
                    Ok(Err(SendError::RemoteUnavailable))
                };
                if let Some(sender) = reply {
                    let outcome = match result {
                        Ok(Ok(())) => Some(
                            crate::transport_protocol::session_opened_ack::Outcome::Ok(Vec::new()),
                        ),
                        Ok(Err(error)) | Err(error) => Some(
                            crate::transport_protocol::session_opened_ack::Outcome::Failure(
                                error.to_wire(),
                            ),
                        ),
                    };
                    let _ = sender.try_send(crate::transport_protocol::Envelope {
                        protocol_version: 0,
                        actor_type: String::new(),
                        actor_id: Vec::new(),
                        session_id: envelope.session_id,
                        kind: Some(Kind::SessionOpenedAck(
                            crate::transport_protocol::SessionOpenedAck { outcome },
                        )),
                    });
                }
            }
            Kind::SessionClose(_) => {
                if reply
                    .as_ref()
                    .is_some_and(|sender| self.sessions.is_bound_to(&address, &session_id, sender))
                {
                    self.close_local_session(&address, &session_id, &self.sessions)
                        .await;
                }
            }
            Kind::Event(_) | Kind::SessionError(_) | Kind::SessionOpenedAck(_) => {}
        }
    }

    pub(crate) fn make_message_context(
        &self,
        address: &ActorAddress,
        session_id: SessionId,
    ) -> MessageContext {
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

fn send_session_error(
    reply: Option<Arc<dyn TransportSender>>,
    session_id: Vec<u8>,
    error: SendError,
) {
    if let Some(reply) = reply {
        let _ = reply.try_send(crate::transport_protocol::Envelope {
            protocol_version: 0,
            actor_type: String::new(),
            actor_id: Vec::new(),
            session_id,
            kind: Some(crate::transport_protocol::envelope::Kind::SessionError(
                crate::transport_protocol::SessionError {
                    failure: error.to_wire(),
                },
            )),
        });
    }
}

fn actor_address_from_envelope(actor_type: String, actor_id: Vec<u8>) -> Option<ActorAddress> {
    let actor_id = String::from_utf8(actor_id).ok()?;
    ActorAddress::new(actor_type, actor_id).ok()
}
