use std::{any::Any, sync::Arc};

use futures_util::FutureExt;
use tokio::sync::{mpsc, watch};

use super::command::{Command, CommandOutcome, RuntimeError};
use super::core::RuntimeInner;
use super::route::{Spawned, begin_deactivation, remove_route};
use crate::{ActorAddress, CommandContext, DeactivationReason};

pub fn spawn_actor<S>(
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

pub struct ActorTaskGuard<S> {
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
