use std::{
    any::Any,
    collections::HashMap,
    sync::{
        Arc, Weak,
        atomic::{AtomicU8, AtomicU64, Ordering},
    },
    time::Duration,
};

use futures_util::FutureExt;
use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, watch};

use super::command::*;

use crate::cluster::{
    ClusterRouter, LocalResolution, NodeAuthority, ResolvedOwner, invoke_peer, wall_time_millis,
};
use crate::{ActorAddress, ActorId, CommandContext, DeactivationReason, NodeLease, SendError};

pub(crate) const RUNNING: u8 = 0;
const SHUTTING_DOWN: u8 = 1;
const STOPPED: u8 = 2;
const FENCED: u8 = 3;

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

pub struct Route<S> {
    generation: u64,
    state: RouteState<S>,
    _capacity: OwnedSemaphorePermit,
    shutdown: watch::Sender<bool>,
    abort: tokio::task::AbortHandle,
    completed: watch::Receiver<bool>,
}

enum RouteState<S> {
    Active(CommandSender<S>),
    Deactivating,
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
                RouteState::Active(sender) => match sender.try_send(command) {
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
                state: RouteState::Active(spawned.sender),
                _capacity: permit,
                shutdown: spawned.shutdown,
                abort: spawned.abort,
                completed: spawned.completed,
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

struct Spawned<S> {
    sender: CommandSender<S>,
    shutdown: watch::Sender<bool>,
    abort: tokio::task::AbortHandle,
    completed: watch::Receiver<bool>,
}

fn try_send<S: 'static>(
    sender: &CommandSender<S>,
    command: Command<S>,
) -> Result<(), RuntimeError> {
    sender.try_send(command).map_err(|error| match error {
        mpsc::error::TrySendError::Full(command) => {
            command.fail(RuntimeError::MailboxFull);
            RuntimeError::MailboxFull
        }
        mpsc::error::TrySendError::Closed(command) => {
            command.fail(RuntimeError::ActorStopped);
            RuntimeError::ActorStopped
        }
    })
}

fn spawn_actor<S>(
    runtime: Arc<RuntimeInner<S>>,
    address: ActorAddress,
    generation: u64,
) -> Spawned<S>
where
    S: Send + Sync + 'static,
{
    let registration = runtime
        .registrations
        .get(address.actor_type())
        .expect("Actor Type registration disappeared");
    let mailbox_capacity = registration
        .mailbox_capacity
        .expect("mailbox capacity was not configured");
    let create = registration.create;
    let activate = registration.activate;
    let deactivate = registration.deactivate;
    let idle_timeout = registration
        .idle_timeout
        .expect("idle timeout was not configured");
    let mut actor = create(address.actor_id().clone(), runtime.state.clone());
    let (sender, mut receiver) = mpsc::channel::<Command<S>>(mailbox_capacity);
    let task_sender = sender.clone();
    let (shutdown, mut shutdown_receiver) = watch::channel(false);
    let (completion_sender, completed) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _task_guard = ActorTaskGuard {
            runtime: runtime.clone(),
            address: address.clone(),
            generation,
            completion: completion_sender,
        };
        tracing::debug!(
            actor_type = address.actor_type(),
            actor_id = ?address.actor_id(),
            lifecycle = "activation",
            error_category = "None",
            "Actor activation started"
        );
        match std::panic::AssertUnwindSafe(activate(actor.as_mut()))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => {
                tracing::debug!(
                    actor_type = address.actor_type(),
                    actor_id = ?address.actor_id(),
                    lifecycle = "activation",
                    error_category = "None",
                    "Actor activation completed"
                );
            }
            Ok(Err(error)) => {
                tracing::error!(
                    actor_type = address.actor_type(),
                    actor_id = ?address.actor_id(),
                    lifecycle = "activation",
                    error_category = "ActivationFailed",
                    error = %error,
                    "Actor activation failed"
                );
                receiver.close();
                remove_route(&runtime, &address, generation);
                while let Ok(command) = receiver.try_recv() {
                    command.fail(RuntimeError::ActivationFailed);
                }
                return;
            }
            Err(_) => {
                tracing::error!(
                    actor_type = address.actor_type(),
                    actor_id = ?address.actor_id(),
                    lifecycle = "activation",
                    error_category = "ActorStopped",
                    "Actor activation panicked"
                );
                receiver.close();
                remove_route(&runtime, &address, generation);
                while let Ok(command) = receiver.try_recv() {
                    command.fail(RuntimeError::ActorStopped);
                }
                return;
            }
        }
        loop {
            let command = tokio::select! {
                biased;
                changed = shutdown_receiver.changed() => {
                    if changed.is_ok() && *shutdown_receiver.borrow() {
                        receiver.close();
                        while let Some(command) = receiver.recv().await {
                            if !execute_command(
                                command,
                                actor.as_mut(),
                                &runtime,
                                &address,
                                generation,
                                &mut receiver,
                            ).await {
                                return;
                            }
                        }
                        tracing::debug!(
                            actor_type = address.actor_type(),
                            actor_id = ?address.actor_id(),
                            lifecycle = "deactivation",
                            error_category = "None",
                            reason = "Shutdown",
                            "Actor shutdown deactivation started"
                        );
                        match std::panic::AssertUnwindSafe(deactivate(
                            actor.as_mut(),
                            DeactivationReason::Shutdown,
                        ))
                        .catch_unwind()
                        .await
                        {
                            Ok(()) => tracing::debug!(
                                actor_type = address.actor_type(),
                                actor_id = ?address.actor_id(),
                                lifecycle = "deactivation",
                                error_category = "None",
                                reason = "Shutdown",
                                "Actor shutdown deactivation completed"
                            ),
                            Err(_) => tracing::error!(
                                actor_type = address.actor_type(),
                                actor_id = ?address.actor_id(),
                                lifecycle = "deactivation",
                                error_category = "ActorStopped",
                                reason = "Shutdown",
                                "Actor shutdown deactivation panicked"
                            ),
                        }
                        remove_route(&runtime, &address, generation);
                        return;
                    }
                    continue;
                }
                received = tokio::time::timeout(idle_timeout, receiver.recv()) => {
                    match received {
                        Ok(Some(command)) => command,
                        Ok(None) => {
                            remove_route(&runtime, &address, generation);
                            return;
                        }
                        Err(_) => {
                            if !begin_deactivation(&runtime, &address, generation, &task_sender) {
                                continue;
                            }
                            tracing::debug!(
                                actor_type = address.actor_type(),
                                actor_id = ?address.actor_id(),
                                lifecycle = "deactivation",
                                error_category = "None",
                                reason = "Idle",
                                "Actor idle deactivation started"
                            );
                            match tokio::time::timeout(
                                runtime.deactivation_timeout,
                                std::panic::AssertUnwindSafe(deactivate(
                                    actor.as_mut(),
                                    DeactivationReason::Idle,
                                ))
                                .catch_unwind(),
                            )
                            .await
                            {
                                Err(_) => tracing::warn!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "DeactivationTimedOut",
                                    "Actor deactivation timed out"
                                ),
                                Ok(Ok(())) => tracing::debug!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "None",
                                    reason = "Idle",
                                    "Actor idle deactivation completed"
                                ),
                                Ok(Err(_)) => tracing::error!(
                                    actor_type = address.actor_type(),
                                    actor_id = ?address.actor_id(),
                                    lifecycle = "deactivation",
                                    error_category = "ActorStopped",
                                    reason = "Idle",
                                    "Actor idle deactivation panicked"
                                ),
                            }
                            drop(actor);
                            if let Some(cluster) = &runtime.cluster {
                                if cluster.release_local_owner(&address).await.is_err() {
                                    tracing::warn!(
                                        actor_type = address.actor_type(),
                                        actor_id = ?address.actor_id(),
                                        lifecycle = "ownership_release",
                                        error_category = "OwnershipUnavailable",
                                        "Actor Owner release could not be confirmed"
                                    );
                                }
                            }
                            remove_route(&runtime, &address, generation);
                            return;
                        }
                    }
                }
            };
            if !execute_command(
                command,
                actor.as_mut(),
                &runtime,
                &address,
                generation,
                &mut receiver,
            )
            .await
            {
                return;
            }
        }
    });
    Spawned {
        sender,
        shutdown,
        abort: task.abort_handle(),
        completed,
    }
}

struct ActorTaskGuard<S> {
    runtime: Arc<RuntimeInner<S>>,
    address: ActorAddress,
    generation: u64,
    completion: watch::Sender<bool>,
}

impl<S> Drop for ActorTaskGuard<S> {
    fn drop(&mut self) {
        remove_route(&self.runtime, &self.address, self.generation);
        let _ = self.completion.send(true);
    }
}

async fn execute_command<S>(
    command: Command<S>,
    actor: &mut (dyn Any + Send),
    runtime: &RuntimeInner<S>,
    address: &ActorAddress,
    generation: u64,
    receiver: &mut mpsc::Receiver<Command<S>>,
) -> bool
where
    S: Send + Sync + 'static,
{
    let context = CommandContext {
        address: address.clone(),
    };
    let CommandOutcome::Panicked(fail_current) = command.execute(actor, context).await else {
        if runtime
            .authority
            .as_ref()
            .is_some_and(|authority| !authority.is_valid())
        {
            receiver.close();
            remove_route(runtime, address, generation);
            while let Ok(command) = receiver.try_recv() {
                command.fail(RuntimeError::NodeFenced);
            }
            return false;
        }
        return true;
    };

    tracing::error!(
        actor_type = address.actor_type(),
        actor_id = ?address.actor_id(),
        lifecycle = "command",
        error_category = "ActorStopped",
        "Actor command handler panicked"
    );
    receiver.close();
    remove_route(runtime, address, generation);
    while let Ok(command) = receiver.try_recv() {
        command.fail(RuntimeError::ActorStopped);
    }
    fail_current();
    false
}

fn begin_deactivation<S>(
    runtime: &RuntimeInner<S>,
    address: &ActorAddress,
    generation: u64,
    sender: &CommandSender<S>,
) -> bool {
    let mut actors = runtime.actors.lock();
    let Some(route) = actors.get_mut(address) else {
        return false;
    };
    if route.generation != generation || sender.capacity() != sender.max_capacity() {
        return false;
    }
    route.state = RouteState::Deactivating;
    true
}

fn remove_route<S>(runtime: &RuntimeInner<S>, address: &ActorAddress, generation: u64) {
    let mut actors = runtime.actors.lock();
    if actors
        .get(address)
        .is_some_and(|route| route.generation == generation)
    {
        actors.remove(address);
    }
}

impl<S> RuntimeInner<S>
where
    S: Send + Sync + 'static,
{
    fn has_authority(&self) -> bool {
        self.status.load(Ordering::Acquire) != FENCED
            && self
                .authority
                .as_ref()
                .is_none_or(|authority| authority.is_valid())
    }

    pub(crate) fn update_capacity_sample(&self, lease: &mut NodeLease) {
        let available = self.capacity.available_permits();
        lease.sampled_at_unix_ms = wall_time_millis();
        lease.active_actor_count = self.max_active_actors.saturating_sub(available);
        lease.max_actor_count = self.max_active_actors;
        lease.pressured = available == 0;
        lease.draining = self.status.load(Ordering::Acquire) != RUNNING;
    }

    pub async fn shutdown(self: &Arc<Self>) {
        tracing::debug!(
            lifecycle = "shutdown",
            error_category = "None",
            "CoActor runtime shutdown started"
        );
        let (mut completions, aborts) = {
            let actors = self.actors.lock();
            if self
                .status
                .compare_exchange(RUNNING, SHUTTING_DOWN, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                return;
            }

            let mut completions = Vec::with_capacity(actors.len());
            let mut aborts = Vec::with_capacity(actors.len());
            for route in actors.values() {
                let _ = route.shutdown.send(true);
                completions.push(route.completed.clone());
                aborts.push(route.abort.clone());
            }
            (completions, aborts)
        };

        let wait = async {
            for completion in &mut completions {
                if !*completion.borrow() {
                    let _ = completion.wait_for(|completed| *completed).await;
                }
            }
        };
        if tokio::time::timeout(self.shutdown_timeout, wait)
            .await
            .is_err()
        {
            tracing::warn!(
                lifecycle = "shutdown",
                error_category = "ShutdownTimedOut",
                "CoActor runtime shutdown timed out"
            );
            for abort in aborts {
                abort.abort();
            }
            tokio::task::yield_now().await;
            self.actors.lock().clear();
        }
        self.status.store(STOPPED, Ordering::Release);
        tracing::debug!(
            lifecycle = "shutdown",
            error_category = "None",
            "CoActor runtime shutdown completed"
        );
    }

    pub async fn fence(self: &Arc<Self>) {
        let (completions, aborts) = {
            let actors = self.actors.lock();
            self.status.store(FENCED, Ordering::Release);
            let mut completions = Vec::with_capacity(actors.len());
            let mut aborts = Vec::with_capacity(actors.len());
            for route in actors.values() {
                route.abort.abort();
                completions.push(route.completed.clone());
                aborts.push(route.abort.clone());
            }
            (completions, aborts)
        };
        for abort in aborts {
            abort.abort();
        }
        tokio::task::yield_now().await;
        for mut completion in completions {
            if !*completion.borrow() {
                let _ = completion.wait_for(|completed| *completed).await;
            }
        }
        self.actors.lock().clear();
    }
}
