use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::core::{FENCED, RUNNING, RuntimeInner, SHUTTING_DOWN, STOPPED};
use crate::cluster::{NodeLease, wall_time_millis};

impl<S> RuntimeInner<S>
where
    S: Send + Sync + 'static,
{
    pub(crate) fn has_authority(&self) -> bool {
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
                let _ = route.task.shutdown.send(true);
                completions.push(route.task.completed.clone());
                aborts.push(route.task.abort.clone());
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
                route.task.abort.abort();
                completions.push(route.task.completed.clone());
                aborts.push(route.task.abort.clone());
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
