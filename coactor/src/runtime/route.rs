use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};

use super::core::ServerInner;
use super::message::{MailboxMessage, MailboxSender};
use super::session::SessionRegistry;
use crate::ActorAddress;

pub struct Route {
    pub(crate) generation: u64,
    pub(crate) state: RouteState,
    pub(crate) _capacity: OwnedSemaphorePermit,
    pub(crate) task: Spawned,
}

pub enum RouteState {
    Active,
    Deactivating,
}

pub struct Spawned {
    pub(crate) sender: MailboxSender,
    pub(crate) shutdown: watch::Sender<bool>,
    pub(crate) abort: tokio::task::AbortHandle,
    pub(crate) completed: watch::Receiver<bool>,
}

pub fn try_send(sender: &MailboxSender, message: MailboxMessage) -> Result<(), crate::SendError> {
    sender.try_send(message).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => crate::SendError::MailboxFull,
        mpsc::error::TrySendError::Closed(_) => crate::SendError::ActorStopped,
    })
}

pub fn remove_route<S>(runtime: &ServerInner<S>, address: &ActorAddress, generation: u64) {
    let mut actors = runtime.actors.lock();
    if actors
        .get(address)
        .is_some_and(|route| route.generation == generation)
    {
        actors.remove(address);
    }
}

pub fn begin_deactivation(
    runtime: &ServerInner<impl Send + Sync + 'static>,
    address: &ActorAddress,
    generation: u64,
    sender: &MailboxSender,
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

pub fn has_live_sessions(registry: &SessionRegistry, address: &ActorAddress) -> bool {
    registry.session_count(address) > 0
}
