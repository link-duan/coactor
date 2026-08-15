use std::{any::Any, sync::Arc};

use futures_util::FutureExt;
use tokio::sync::{mpsc, watch};

use super::actor::MessageOutcome;
use super::core::ServerInner;
use super::message::{Handle, MailboxMessage, SessionHook};
use super::route::{Spawned, begin_deactivation, has_live_sessions, remove_route};
use crate::{ActorAddress, DeactivationReason, SendError};

pub fn spawn_actor<S>(
    runtime: Arc<ServerInner<S>>,
    address: ActorAddress,
    generation: u64,
) -> Spawned
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
    let idle_timeout = registration
        .idle_timeout
        .expect("idle timeout was not configured");
    let actor_runtime = runtime.make_actor_runtime(&address);
    let mut actor = (registration.create)(actor_runtime);
    let activate = registration.activate;
    let deactivate = registration.deactivate;
    let handle = registration.handle;
    let session_opened = registration.session_opened;
    let session_closed = registration.session_closed;

    let (sender, mut receiver) = mpsc::channel::<MailboxMessage>(mailbox_capacity);
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
        match std::panic::AssertUnwindSafe(activate(actor.as_mut()))
            .catch_unwind()
            .await
        {
            Ok(Ok(())) => {}
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
                fail_pending(&mut receiver, SendError::ActivationFailed);
                runtime
                    .sessions
                    .terminate_all(&address, SendError::ActivationFailed)
                    .await;
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
                fail_pending(&mut receiver, SendError::ActorStopped);
                runtime
                    .sessions
                    .terminate_all(&address, SendError::ActorStopped)
                    .await;
                return;
            }
        }

        loop {
            let message = tokio::select! {
                biased;
                changed = shutdown_receiver.changed() => {
                    if changed.is_ok() && *shutdown_receiver.borrow() {
                        receiver.close();
                        while let Some(message) = receiver.recv().await {
                            tracing::debug!(actor_type = address.actor_type(), "drain message");
                            if !execute_message(&runtime, actor.as_mut(), &address, &handle, &session_opened, &session_closed, message).await {
                                runtime.sessions.terminate_all(&address, SendError::ActorStopped).await;
                                return;
                            }
                        }
                        tracing::debug!(actor_type = address.actor_type(), "drain complete; terminating sessions");
                        runtime.sessions.terminate_all(&address, SendError::RuntimeShuttingDown).await;
                        tracing::debug!(actor_type = address.actor_type(), "sessions terminated");
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
                        Ok(Some(message)) => message,
                        Ok(None) => {
                            remove_route(&runtime, &address, generation);
                            return;
                        }
                        Err(_) => {
                            if has_live_sessions(&runtime.sessions, &address) {
                                continue;
                            }
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
            if !execute_message(
                &runtime,
                actor.as_mut(),
                &address,
                &handle,
                &session_opened,
                &session_closed,
                message,
            )
            .await
            {
                runtime
                    .sessions
                    .terminate_all(&address, SendError::ActorStopped)
                    .await;
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

pub struct ActorTaskGuard<S> {
    runtime: Arc<ServerInner<S>>,
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

async fn execute_message<S>(
    runtime: &ServerInner<S>,
    actor: &mut (dyn Any + Send),
    address: &ActorAddress,
    handle: &Handle,
    session_opened: &SessionHook,
    session_closed: &SessionHook,
    message: MailboxMessage,
) -> bool
where
    S: Send + Sync + 'static,
{
    match message {
        MailboxMessage::Action { session_id, payload } => {
            let ctx = runtime.make_message_context(address, session_id);
            match std::panic::AssertUnwindSafe(handle(actor, &ctx, &payload))
                .catch_unwind()
                .await
            {
                Ok(MessageOutcome::Completed) => true,
                Ok(MessageOutcome::Panicked) => false,
                Err(_) => false,
            }
        }
        MailboxMessage::SessionOpened {
            session_id, complete, ..
        } => {
            let ctx = runtime.make_message_context(address, session_id);
            match std::panic::AssertUnwindSafe(session_opened(actor, &ctx))
                .catch_unwind()
                .await
            {
                Ok(()) => {
                    let _ = complete.send(Ok(()));
                    true
                }
                Err(_) => {
                    let _ = complete.send(Err(SendError::ActorStopped));
                    false
                }
            }
        }
        MailboxMessage::SessionClosed { session_id } => {
            let ctx = runtime.make_message_context(address, session_id);
            match std::panic::AssertUnwindSafe(session_closed(actor, &ctx))
                .catch_unwind()
                .await
            {
                Ok(()) => true,
                Err(_) => false,
            }
        }
    }
}

fn fail_pending(receiver: &mut mpsc::Receiver<MailboxMessage>, error: SendError) {
    while let Ok(message) = receiver.try_recv() {
        if let MailboxMessage::SessionOpened { complete, .. } = message {
            let _ = complete.send(Err(error.clone()));
        }
    }
}
