use std::{
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

use parking_lot::Mutex;

use super::command::{
    Command, DispatchOutcome, Registration, RemoteCall, RemotePayload, RemoteReplyError,
    RuntimeError,
};
use super::lifecycle::spawn_actor;
use super::route::{Route, RouteState, try_send};
use crate::cluster::{ClusterRouter, LocalResolution, NodeAuthority, ResolvedOwner, invoke_peer};
use crate::{ActorAddress, ActorId, SendError};

pub(crate) const RUNNING: u8 = 0;
pub(crate) const SHUTTING_DOWN: u8 = 1;
pub(crate) const STOPPED: u8 = 2;
pub(crate) const FENCED: u8 = 3;

pub(crate) struct RuntimeInner<S> {
    pub state: Arc<S>,
    pub registrations: HashMap<&'static str, Registration<S>>,
    pub actors: Mutex<HashMap<ActorAddress, Route<S>>>,
    pub capacity: Arc<Semaphore>,
    pub max_active_actors: usize,
    pub deactivation_timeout: Duration,
    pub next_generation: AtomicU64,
    pub status: AtomicU8,
    pub shutdown_timeout: Duration,
    pub peer_protocol_version: u32,
    pub authority: Option<Arc<NodeAuthority>>,
    pub cluster: Option<Arc<ClusterRouter>>,
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
    pub async fn dispatch<F>(
        &self,
        remote: Option<RemoteCall>,
        make_local: F,
    ) -> Result<DispatchOutcome, RuntimeError>
    where
        F: FnOnce() -> Command<S>,
    {
        let Some(remote) = remote else {
            self.send(make_local())?;
            return Ok(DispatchOutcome::Local);
        };
        match self
            .route_remote_command(remote.command, remote.payload)
            .await?
        {
            RouteDecision::Remote(reply) => Ok(DispatchOutcome::Remote(reply)),
            RouteDecision::Local {
                reservation,
                resolution,
            } => {
                self.send_with_reservation(make_local(), reservation)?;
                drop(resolution);
                Ok(DispatchOutcome::Local)
            }
        }
    }

    pub fn send(&self, command: Command<S>) -> Result<(), RuntimeError> {
        self.send_with_reservation(command, None)
    }

    pub fn send_with_reservation(
        &self,
        mut command: Command<S>,
        reservation: Option<OwnedSemaphorePermit>,
    ) -> Result<(), RuntimeError> {
        let Some(runtime) = self.runtime.upgrade() else {
            command.fail(RuntimeError::RuntimeStopped);
            return Err(RuntimeError::RuntimeStopped);
        };

        let mut actors = runtime.actors.lock();
        if runtime
            .authority
            .as_ref()
            .is_some_and(|authority| !authority.is_valid())
        {
            command.fail(RuntimeError::NodeFenced);
            return Err(RuntimeError::NodeFenced);
        }
        match runtime.status.load(Ordering::Acquire) {
            RUNNING => {}
            SHUTTING_DOWN => {
                command.fail(RuntimeError::RuntimeShuttingDown);
                return Err(RuntimeError::RuntimeShuttingDown);
            }
            FENCED => {
                command.fail(RuntimeError::NodeFenced);
                return Err(RuntimeError::NodeFenced);
            }
            _ => {
                command.fail(RuntimeError::RuntimeStopped);
                return Err(RuntimeError::RuntimeStopped);
            }
        }
        if let Some(route) = actors.get(&self.address) {
            match &route.state {
                RouteState::Active => match route.task.sender.try_send(command) {
                    Ok(()) => return Ok(()),
                    Err(mpsc::error::TrySendError::Full(command)) => {
                        command.fail(RuntimeError::MailboxFull);
                        return Err(RuntimeError::MailboxFull);
                    }
                    Err(mpsc::error::TrySendError::Closed(returned)) => {
                        let generation = route.generation;
                        actors.remove(&self.address);
                        command = returned;
                        tracing::debug!(
                            actor_type = self.address.actor_type(),
                            actor_id = ?self.address.actor_id(),
                            generation,
                            lifecycle = "routing",
                            error_category = "ClosedRouteReplaced",
                            "Replacing a closed Actor route"
                        );
                    }
                },
                RouteState::Deactivating => {
                    command.fail(RuntimeError::ActorDeactivating);
                    return Err(RuntimeError::ActorDeactivating);
                }
            }
        }

        let permit = match reservation {
            Some(permit) => permit,
            None => match runtime.capacity.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    command.fail(RuntimeError::RuntimeAtCapacity);
                    return Err(RuntimeError::RuntimeAtCapacity);
                }
            },
        };
        let generation = runtime.next_generation.fetch_add(1, Ordering::Relaxed);
        let spawned = spawn_actor(runtime.clone(), self.address.clone(), generation);
        let result = try_send(&spawned.sender, command);
        actors.insert(
            self.address.clone(),
            Route {
                generation,
                state: RouteState::Active,
                _capacity: permit,
                task: spawned,
            },
        );
        result
    }

    pub fn reply_channel_closed_error<E>(&self) -> SendError<E> {
        if self
            .runtime
            .upgrade()
            .is_some_and(|runtime| runtime.status.load(Ordering::Acquire) == FENCED)
        {
            SendError::NodeFenced
        } else {
            SendError::ActorStopped
        }
    }

    pub fn ensure_reply_authority<E>(&self) -> Result<(), SendError<E>> {
        let Some(runtime) = self.runtime.upgrade() else {
            return Err(SendError::RuntimeStopped);
        };
        if runtime.status.load(Ordering::Acquire) == FENCED
            || runtime
                .authority
                .as_ref()
                .is_some_and(|authority| !authority.is_valid())
        {
            Err(SendError::NodeFenced)
        } else {
            Ok(())
        }
    }

    async fn invoke_endpoint(
        &self,
        endpoint: String,
        protocol_version: u32,
        command: &'static str,
        payload: Vec<u8>,
        connect_timeout: Option<Duration>,
    ) -> Result<RemotePayload, RuntimeError> {
        invoke_peer(
            &self.address,
            endpoint,
            protocol_version,
            command,
            payload,
            connect_timeout,
        )
        .await
    }

    pub async fn route_remote_command(
        &self,
        command: &'static str,
        payload: Vec<u8>,
    ) -> Result<RouteDecision, RuntimeError> {
        let runtime = self.runtime.upgrade().ok_or(RuntimeError::RuntimeStopped)?;
        let Some(cluster) = &runtime.cluster else {
            return Ok(RouteDecision::Local {
                reservation: None,
                resolution: None,
            });
        };
        for attempt in 0..=1 {
            match cluster.resolve(&self.address, &runtime.capacity).await {
                Err(RuntimeError::RuntimeAtCapacity) => {
                    let candidates = cluster
                        .placement_candidates(runtime.peer_protocol_version)
                        .await?;
                    for (candidate_index, (endpoint, protocol_version)) in
                        candidates.into_iter().enumerate()
                    {
                        match self
                            .invoke_endpoint(
                                endpoint,
                                protocol_version,
                                command,
                                payload.clone(),
                                Some(cluster.peer_connect_timeout),
                            )
                            .await
                        {
                            Ok(remote) => return Ok(RouteDecision::Remote(remote)),
                            Err(RuntimeError::RuntimeAtCapacity) if candidate_index == 0 => {}
                            Err(error) => return Err(error),
                        }
                    }
                    return Err(RuntimeError::RuntimeAtCapacity);
                }
                Err(error) => return Err(error),
                Ok(resolved) => match resolved {
                    ResolvedOwner::Local { reservation, guard } => {
                        return Ok(RouteDecision::Local {
                            reservation,
                            resolution: Some(guard),
                        });
                    }
                    ResolvedOwner::Remote {
                        endpoint,
                        protocol_version,
                    } => match self
                        .invoke_endpoint(
                            endpoint,
                            protocol_version,
                            command,
                            payload.clone(),
                            Some(cluster.peer_connect_timeout),
                        )
                        .await
                    {
                        Ok(remote) => return Ok(RouteDecision::Remote(remote)),
                        Err(RuntimeError::RemoteUnavailable | RuntimeError::NotOwner)
                            if attempt == 0 =>
                        {
                            cluster.invalidate(&self.address).await;
                        }
                        Err(error) => return Err(error),
                    },
                },
            }
        }
        Err(RuntimeError::RemoteUnavailable)
    }
}

pub enum RouteDecision {
    Local {
        reservation: Option<OwnedSemaphorePermit>,
        resolution: Option<tokio::sync::OwnedMutexGuard<()>>,
    },
    Remote(RemotePayload),
}

impl<S> RuntimeInner<S>
where
    S: Send + Sync + 'static,
{
    pub(crate) async fn dispatch_peer(
        self: &Arc<Self>,
        actor_type: &str,
        actor_id: Vec<u8>,
        command: &str,
        payload: Vec<u8>,
    ) -> Result<RemotePayload, RuntimeError> {
        let registration = self
            .registrations
            .get(actor_type)
            .ok_or(RuntimeError::ActorTypeNotRegistered)?;
        let factory = registration
            .remote_commands
            .get(command)
            .ok_or(RuntimeError::CommandNotRegistered)?;
        let invocation = factory(payload)?;
        let actor_ref = ActorRef {
            runtime: Arc::downgrade(self),
            address: ActorAddress::new(registration.name, ActorId::new(actor_id)),
        };
        let resolution = if let Some(cluster) = &self.cluster {
            cluster
                .resolve_local(&actor_ref.address, &self.capacity)
                .await?
        } else {
            LocalResolution {
                reservation: None,
                guard: None,
            }
        };
        actor_ref.send_with_reservation(invocation.command, resolution.reservation)?;
        drop(resolution.guard);
        let reply = invocation.reply.await;
        if !self.has_authority() {
            return Err(RuntimeError::NodeFenced);
        }
        match reply {
            Ok(bytes) => Ok(RemotePayload::Success(bytes)),
            Err(RemoteReplyError::Handler(bytes)) => Ok(RemotePayload::HandlerError(bytes)),
            Err(RemoteReplyError::Runtime(error)) => Err(error),
        }
    }
}
