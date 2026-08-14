use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::command::{Command, CommandSender, RuntimeError};
use super::core::RuntimeInner;
use crate::ActorAddress;

pub struct Route<S> {
    pub(crate) generation: u64,
    pub(crate) state: RouteState,
    pub(crate) _capacity: OwnedSemaphorePermit,
    pub(crate) task: Spawned<S>,
}

pub enum RouteState {
    Active,
    Deactivating,
}

pub struct Spawned<S> {
    pub(crate) sender: CommandSender<S>,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) abort: tokio::task::AbortHandle,
    pub(crate) completed: watch::Receiver<bool>,
}

pub fn try_send<S: 'static>(
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

pub fn remove_route<S>(runtime: &RuntimeInner<S>, address: &ActorAddress, generation: u64) {
    let mut actors = runtime.actors.lock();
    if actors
        .get(address)
        .is_some_and(|route| route.generation == generation)
    {
        actors.remove(address);
    }
}

pub fn begin_deactivation<S>(
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
