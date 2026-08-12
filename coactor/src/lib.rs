extern crate self as coactor;

use std::{
    any::Any,
    collections::HashMap,
    convert::Infallible,
    fmt,
    sync::{Arc, Weak},
};

use parking_lot::Mutex;
use thiserror::Error;
use tokio::sync::mpsc;

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
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ActorRefError {
    #[error("Actor Type `{0}` is not registered")]
    ActorTypeNotRegistered(&'static str),
}

#[derive(Clone, Copy, Debug, Default)]
pub struct ActorTypeConfig {
    mailbox_capacity: Option<usize>,
}

impl ActorTypeConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = Some(capacity);
        self
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum SendError<E = Infallible> {
    #[error("handler failed: {0:?}")]
    HandlerError(E),
    #[error("the Active Actor mailbox is full")]
    MailboxFull,
    #[error("the Active Actor failed to activate")]
    ActivationFailed,
    #[error("the Active Actor stopped")]
    ActorStopped,
    #[error("the CoActor runtime stopped")]
    RuntimeStopped,
}

pub struct RuntimeBuilder<S> {
    state: S,
    registrations: Vec<__private::Registration<S>>,
    mailbox_capacity: usize,
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
        }
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
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
        self.registrations.push(registration);
        self
    }

    pub fn build(self) -> Result<Runtime<S>, BuildError> {
        if self.mailbox_capacity == 0 {
            return Err(BuildError::InvalidMailboxCapacity);
        }
        let mut registrations = HashMap::new();
        for mut registration in self.registrations {
            if registration.mailbox_capacity == Some(0) {
                return Err(BuildError::InvalidMailboxCapacity);
            }
            if registration.mailbox_capacity.is_none() {
                registration.mailbox_capacity = Some(self.mailbox_capacity);
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
}

#[doc(hidden)]
pub mod __private {
    use super::*;
    use std::{future::Future, marker::PhantomData, pin::Pin};

    pub use futures_util::FutureExt;
    pub use tokio;

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

    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    pub enum CommandOutcome {
        Completed,
        Panicked,
    }

    #[derive(Clone, Copy)]
    pub enum RuntimeError {
        ActorStopped,
        RuntimeStopped,
        MailboxFull,
        ActivationFailed,
    }

    impl<E> From<RuntimeError> for SendError<E> {
        fn from(value: RuntimeError) -> Self {
            match value {
                RuntimeError::ActorStopped => Self::ActorStopped,
                RuntimeError::RuntimeStopped => Self::RuntimeStopped,
                RuntimeError::MailboxFull => Self::MailboxFull,
                RuntimeError::ActivationFailed => Self::ActivationFailed,
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
        fn make_ref(inner: ActorRef<S>) -> Self::Ref;
    }

    pub struct Registration<S> {
        pub name: &'static str,
        create: fn(ActorId) -> Box<dyn Any + Send>,
        activate: Activate<S>,
        pub mailbox_capacity: Option<usize>,
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
                mailbox_capacity: None,
                marker: PhantomData,
            }
        }
    }

    pub struct RuntimeInner<S> {
        pub state: Arc<S>,
        pub registrations: HashMap<&'static str, Registration<S>>,
        pub actors: Mutex<HashMap<ActorAddress, CommandSender<S>>>,
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
        pub fn send(&self, command: Command<S>) -> Result<(), RuntimeError> {
            let Some(runtime) = self.runtime.upgrade() else {
                command.fail(RuntimeError::RuntimeStopped);
                return Err(RuntimeError::RuntimeStopped);
            };

            let sender = {
                let mut actors = runtime.actors.lock();
                actors
                    .entry(self.address.clone())
                    .or_insert_with(|| spawn_actor(runtime.clone(), self.address.clone()))
                    .clone()
            };

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
    }

    fn spawn_actor<S>(runtime: Arc<RuntimeInner<S>>, address: ActorAddress) -> CommandSender<S>
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
        let mut actor = create(address.actor_id().clone());
        let (sender, mut receiver) = mpsc::channel::<Command<S>>(mailbox_capacity);
        let task_sender = sender.clone();
        tokio::spawn(async move {
            let activation_context = ActorContext {
                address: &address,
                state: runtime.state.as_ref(),
            };
            if let Err(error) = activate(actor.as_mut(), activation_context).await {
                tracing::error!(
                    actor_type = address.actor_type(),
                    actor_id = ?address.actor_id(),
                    lifecycle = "activation",
                    error_category = "ActivationFailed",
                    error = %error,
                    "Actor activation failed"
                );
                receiver.close();
                while let Ok(command) = receiver.try_recv() {
                    command.fail(RuntimeError::ActivationFailed);
                }
                remove_route(&runtime, &address, &task_sender);
                return;
            }
            while let Some(command) = receiver.recv().await {
                let context = ActorContext {
                    address: &address,
                    state: runtime.state.as_ref(),
                };
                let outcome = command.execute(actor.as_mut(), context).await;
                if outcome == CommandOutcome::Panicked {
                    tracing::error!(
                        actor_type = address.actor_type(),
                        actor_id = ?address.actor_id(),
                        lifecycle = "command",
                        error_category = "ActorStopped",
                        "Actor command handler panicked"
                    );
                    receiver.close();
                    while let Ok(command) = receiver.try_recv() {
                        command.fail(RuntimeError::ActorStopped);
                    }
                    remove_route(&runtime, &address, &task_sender);
                    return;
                }
            }
            remove_route(&runtime, &address, &task_sender);
        });
        sender
    }

    fn remove_route<S>(
        runtime: &RuntimeInner<S>,
        address: &ActorAddress,
        sender: &CommandSender<S>,
    ) {
        let mut actors = runtime.actors.lock();
        if actors
            .get(address)
            .is_some_and(|current| current.same_channel(sender))
        {
            actors.remove(address);
        }
    }
}
