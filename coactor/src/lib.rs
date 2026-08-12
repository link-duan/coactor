extern crate self as coactor;

use std::{
    any::Any,
    collections::HashMap,
    convert::Infallible,
    fmt,
    sync::{Arc, Weak},
    time::Duration,
};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::{Semaphore, mpsc, watch};

pub use coactor_macros::{actor, command};

#[derive(Clone, PartialEq, Eq, Hash)]
pub struct ActorId(Arc<[u8]>);

impl ActorId {
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into().into())
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl From<&str> for ActorId {
    fn from(value: &str) -> Self {
        Self::new(value.as_bytes())
    }
}

impl fmt::Debug for ActorId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.debug_tuple("ActorId").field(&self.0).finish()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ActorAddress {
    actor_type: Arc<str>,
    actor_id: ActorId,
}

impl ActorAddress {
    pub fn new(actor_type: impl Into<Arc<str>>, actor_id: ActorId) -> Self {
        Self {
            actor_type: actor_type.into(),
            actor_id,
        }
    }

    pub fn actor_type(&self) -> &str {
        &self.actor_type
    }

    pub fn actor_id(&self) -> &ActorId {
        &self.actor_id
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let name = self.actor_type.as_bytes();
        let mut bytes = Vec::with_capacity(4 + name.len() + self.actor_id.as_bytes().len());
        bytes.extend_from_slice(&(name.len() as u32).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(self.actor_id.as_bytes());
        bytes
    }
}

pub struct ActorContext<'a, S> {
    address: &'a ActorAddress,
    state: &'a S,
}

impl<'a, S> ActorContext<'a, S> {
    pub fn actor_id(&self) -> &ActorId {
        self.address.actor_id()
    }

    pub fn actor_address(&self) -> &ActorAddress {
        self.address
    }

    pub fn state(&self) -> &S {
        self.state
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum BuildError {
    #[error("Actor Type `{0}` was registered more than once")]
    DuplicateActorType(&'static str),
    #[error("mailbox capacity must be greater than zero")]
    InvalidMailboxCapacity,
    #[error("max_active_actors must be greater than zero")]
    InvalidMaxActiveActors,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActorRefError {
    #[error("Actor Type `{0}` is not registered")]
    ActorTypeNotRegistered(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ActorTypeConfig {
    mailbox_capacity: Option<usize>,
    idle_timeout: Option<Duration>,
}

impl ActorTypeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeactivationReason {
    Idle,
    Shutdown,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SendError<E = Infallible> {
    #[error("handler failed: {0:?}")]
    HandlerError(E),
    #[error("the Active Actor mailbox is full")]
    MailboxFull,
    #[error("the Active Actor failed to activate")]
    ActivationFailed,
    #[error("the Active Actor is deactivating")]
    ActorDeactivating,
    #[error("the runtime has reached its Active Actor limit")]
    RuntimeAtCapacity,
    #[error("the runtime is shutting down")]
    RuntimeShuttingDown,
    #[error("the Active Actor stopped")]
    ActorStopped,
    #[error("the CoActor runtime stopped")]
    RuntimeStopped,
}

pub struct RuntimeBuilder<S> {
    state: S,
    registrations: Vec<__private::Registration<S>>,
    mailbox_capacity: usize,
    max_active_actors: usize,
    idle_timeout: Duration,
    deactivation_timeout: Duration,
    shutdown_timeout: Duration,
}

impl<S> RuntimeBuilder<S>
where
    S: Send + Sync + 'static,
{
    pub fn new(state: S) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            mailbox_capacity: 32,
            max_active_actors: 10_000,
            idle_timeout: Duration::from_secs(60),
            deactivation_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
        }
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
        self
    }

    pub fn max_active_actors(mut self, maximum: usize) -> Self {
        self.max_active_actors = maximum;
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn deactivation_timeout(mut self, timeout: Duration) -> Self {
        self.deactivation_timeout = timeout;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn register<A>(mut self) -> Self
    where
        A: __private::ActorType<S>,
    {
        self.registrations.push(__private::Registration::of::<A>());
        self
    }

    pub fn register_with<A>(mut self, config: ActorTypeConfig) -> Self
    where
        A: __private::ActorType<S>,
    {
        let mut registration = __private::Registration::of::<A>();
        registration.mailbox_capacity = config.mailbox_capacity;
        registration.idle_timeout = config.idle_timeout;
        self.registrations.push(registration);
        self
    }

    pub fn build(self) -> Result<Runtime<S>, BuildError> {
        if self.mailbox_capacity == 0 {
            return Err(BuildError::InvalidMailboxCapacity);
        }
        if self.max_active_actors == 0 {
            return Err(BuildError::InvalidMaxActiveActors);
        }
        let mut registrations = HashMap::new();
        for mut registration in self.registrations {
            if registration.mailbox_capacity == Some(0) {
                return Err(BuildError::InvalidMailboxCapacity);
            }
            if registration.mailbox_capacity.is_none() {
                registration.mailbox_capacity = Some(self.mailbox_capacity);
            }
            if registration.idle_timeout.is_none() {
                registration.idle_timeout = Some(self.idle_timeout);
            }
            let name = registration.name;
            if registrations.insert(name, registration).is_some() {
                return Err(BuildError::DuplicateActorType(name));
            }
        }
        Ok(Runtime {
            inner: Arc::new(__private::RuntimeInner {
                state: Arc::new(self.state),
                registrations,
                actors: Mutex::new(HashMap::new()),
                capacity: Arc::new(Semaphore::new(self.max_active_actors)),
                deactivation_timeout: self.deactivation_timeout,
                next_generation: std::sync::atomic::AtomicU64::new(1),
                status: std::sync::atomic::AtomicU8::new(__private::RUNNING),
                shutdown_timeout: self.shutdown_timeout,
            }),
        })
    }
}

pub struct Runtime<S> {
    inner: Arc<__private::RuntimeInner<S>>,
}

impl<S> Runtime<S>
where
    S: Send + Sync + 'static,
{
    pub fn actor_ref<A>(&self, actor_id: ActorId) -> Result<A::Ref, ActorRefError>
    where
        A: __private::ActorType<S>,
    {
        if !self.inner.registrations.contains_key(A::NAME) {
            return Err(ActorRefError::ActorTypeNotRegistered(A::NAME));
        }
        Ok(A::make_ref(__private::ActorRef {
            runtime: Arc::downgrade(&self.inner),
            address: ActorAddress::new(A::NAME, actor_id),
        }))
    }

    pub async fn shutdown(self) {
        self.inner.shutdown().await;
    }
}

#[doc(hidden)]
pub mod __private {
    use super::*;
    use std::{
        future::Future,
        marker::PhantomData,
        pin::Pin,
        sync::atomic::{AtomicU8, AtomicU64, Ordering},
    };
    use tokio::sync::OwnedSemaphorePermit;

    pub use futures_util::FutureExt;
    pub use tokio;

    pub const RUNNING: u8 = 0;
    const SHUTTING_DOWN: u8 = 1;
    const STOPPED: u8 = 2;

    pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

    pub trait ErasedCommand<S>: Send + 'static {
        fn execute<'a>(
            self: Box<Self>,
            actor: &'a mut (dyn Any + Send),
            context: ActorContext<'a, S>,
        ) -> BoxFuture<'a, CommandOutcome>;

        fn fail(self: Box<Self>, error: RuntimeError);
    }

    type Command<S> = Box<dyn ErasedCommand<S>>;
    type CommandSender<S> = mpsc::Sender<Command<S>>;
    type Activate<S> = for<'a> fn(
        &'a mut (dyn Any + Send),
        ActorContext<'a, S>,
    ) -> BoxFuture<'a, Result<(), String>>;
    type Deactivate<S> = for<'a> fn(
        &'a mut (dyn Any + Send),
        ActorContext<'a, S>,
        DeactivationReason,
    ) -> BoxFuture<'a, ()>;

    pub enum CommandOutcome {
        Completed,
        Panicked(Box<dyn FnOnce() + Send>),
    }

    #[derive(Clone, Copy)]
    pub enum RuntimeError {
        ActorStopped,
        RuntimeStopped,
        MailboxFull,
        ActivationFailed,
        ActorDeactivating,
        RuntimeAtCapacity,
        RuntimeShuttingDown,
    }

    impl<E> From<RuntimeError> for SendError<E> {
        fn from(value: RuntimeError) -> Self {
            match value {
                RuntimeError::ActorStopped => Self::ActorStopped,
                RuntimeError::RuntimeStopped => Self::RuntimeStopped,
                RuntimeError::MailboxFull => Self::MailboxFull,
                RuntimeError::ActivationFailed => Self::ActivationFailed,
                RuntimeError::ActorDeactivating => Self::ActorDeactivating,
                RuntimeError::RuntimeAtCapacity => Self::RuntimeAtCapacity,
                RuntimeError::RuntimeShuttingDown => Self::RuntimeShuttingDown,
            }
        }
    }

    pub trait ActorType<S>: Send + 'static {
        const NAME: &'static str;
        type Ref;

        fn create(actor_id: ActorId) -> Self;
        fn activate<'a>(
            actor: &'a mut (dyn Any + Send),
            context: ActorContext<'a, S>,
        ) -> BoxFuture<'a, Result<(), String>>;
        fn deactivate<'a>(
            actor: &'a mut (dyn Any + Send),
            context: ActorContext<'a, S>,
            reason: DeactivationReason,
        ) -> BoxFuture<'a, ()>;
        fn make_ref(inner: ActorRef<S>) -> Self::Ref;
    }

    pub struct Registration<S> {
        pub name: &'static str,
        create: fn(ActorId) -> Box<dyn Any + Send>,
        activate: Activate<S>,
        deactivate: Deactivate<S>,
        pub mailbox_capacity: Option<usize>,
        pub idle_timeout: Option<Duration>,
        marker: PhantomData<fn(S)>,
    }

    impl<S> Registration<S> {
        pub fn of<A>() -> Self
        where
            A: ActorType<S>,
        {
            Self {
                name: A::NAME,
                create: |actor_id| Box::new(A::create(actor_id)),
                activate: A::activate,
                deactivate: A::deactivate,
                mailbox_capacity: None,
                idle_timeout: None,
                marker: PhantomData,
            }
        }
    }

    pub struct RuntimeInner<S> {
        pub state: Arc<S>,
        pub registrations: HashMap<&'static str, Registration<S>>,
        pub actors: Mutex<HashMap<ActorAddress, Route<S>>>,
        pub capacity: Arc<Semaphore>,
        pub deactivation_timeout: Duration,
        pub next_generation: AtomicU64,
        pub status: AtomicU8,
        pub shutdown_timeout: Duration,
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
        pub runtime: Weak<RuntimeInner<S>>,
        pub address: ActorAddress,
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
        pub fn send(&self, mut command: Command<S>) -> Result<(), RuntimeError> {
            let Some(runtime) = self.runtime.upgrade() else {
                command.fail(RuntimeError::RuntimeStopped);
                return Err(RuntimeError::RuntimeStopped);
            };

            let mut actors = runtime.actors.lock();
            match runtime.status.load(Ordering::Acquire) {
                RUNNING => {}
                SHUTTING_DOWN => {
                    command.fail(RuntimeError::RuntimeShuttingDown);
                    return Err(RuntimeError::RuntimeShuttingDown);
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

            let permit = match runtime.capacity.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    command.fail(RuntimeError::RuntimeAtCapacity);
                    return Err(RuntimeError::RuntimeAtCapacity);
                }
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
        let mut actor = create(address.actor_id().clone());
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
            let activation_context = ActorContext {
                address: &address,
                state: runtime.state.as_ref(),
            };
            tracing::debug!(
                actor_type = address.actor_type(),
                actor_id = ?address.actor_id(),
                lifecycle = "activation",
                error_category = "None",
                "Actor activation started"
            );
            match std::panic::AssertUnwindSafe(activate(actor.as_mut(), activation_context))
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
                            let context = ActorContext {
                                address: &address,
                                state: runtime.state.as_ref(),
                            };
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
                                context,
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
                                let context = ActorContext {
                                    address: &address,
                                    state: runtime.state.as_ref(),
                                };
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
                                        context,
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
        let context = ActorContext {
            address,
            state: runtime.state.as_ref(),
        };
        let CommandOutcome::Panicked(fail_current) = command.execute(actor, context).await else {
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
    }
}
