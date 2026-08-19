use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{oneshot, watch};

use super::confirm_node_lease;
use crate::{
    __macro::ServerInner,
    MutationOutcome, NodeLeaseStore, NodeRecord, Revision,
    transport::{Endpoint, ServerTransport},
};

pub struct NodeAuthority {
    valid: AtomicBool,
    deadline: Mutex<tokio::time::Instant>,
    ttl: Duration,
    fenced: watch::Sender<bool>,
}

impl NodeAuthority {
    pub fn new(
        operation_started: tokio::time::Instant,
        ttl: Duration,
        fenced: watch::Sender<bool>,
    ) -> Self {
        Self {
            valid: AtomicBool::new(true),
            deadline: Mutex::new(operation_started + ttl),
            ttl,
            fenced,
        }
    }
    pub fn is_valid(&self) -> bool {
        self.valid.load(Ordering::Acquire) && tokio::time::Instant::now() < *self.deadline.lock()
    }
    fn renew(&self, operation_started: tokio::time::Instant) {
        *self.deadline.lock() = operation_started + self.ttl;
    }
    fn remaining(&self) -> Option<Duration> {
        self.deadline
            .lock()
            .checked_duration_since(tokio::time::Instant::now())
    }
    fn deadline(&self) -> tokio::time::Instant {
        *self.deadline.lock()
    }
    fn fence(&self) {
        if self.valid.swap(false, Ordering::AcqRel) {
            let _ = self.fenced.send(true);
        }
    }
}

pub struct TransportTask {
    pub shutdown: watch::Sender<bool>,
    pub force: oneshot::Sender<()>,
    pub task: tokio::task::JoinHandle<()>,
    pub stopped: watch::Receiver<bool>,
}

pub struct RenewalTask {
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<RenewalExit>,
    pub stopped: watch::Receiver<bool>,
}
struct RenewalExit {
    leases: Arc<dyn NodeLeaseStore>,
    node_id: String,
    revision: Revision,
    release: bool,
}

pub struct ClusterTasks {
    pub transport: TransportTask,
    pub renewal: RenewalTask,
    pub fenced: watch::Receiver<bool>,
}
impl ClusterTasks {
    pub async fn shutdown(self) {
        let _ = self.renewal.shutdown.send(());
        let _ = self.transport.shutdown.send(true);
        if let Ok(exit) = self.renewal.task.await {
            if exit.release {
                let _ = exit
                    .leases
                    .release_node(&exit.node_id, &exit.revision)
                    .await;
            }
        }
        let _ = self.transport.force.send(());
        let _ = self.transport.task.await;
    }
}

pub fn spawn_transport<S>(
    runtime: Arc<ServerInner<S>>,
    transport: Arc<dyn ServerTransport>,
    advertised: &Endpoint,
    listener: Option<tokio::net::TcpListener>,
) -> TransportTask
where
    S: Send + Sync + 'static,
{
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let (force, force_receiver) = oneshot::channel();
    let (stopped_sender, stopped) = watch::channel(false);
    let mut listener = transport
        .listen(advertised, listener)
        .expect("transport listener bind");
    let task = tokio::spawn(async move {
        let mut shutdown_receiver = shutdown_receiver;
        let mut force_receiver = force_receiver;
        loop {
            tokio::select! {
                changed = shutdown_receiver.changed() => if changed.is_ok() && *shutdown_receiver.borrow() { listener.shutdown(); },
                _ = &mut force_receiver => break,
                stream = listener.accept() => {
                    let Some(stream) = stream else { break };
                    let task_runtime = runtime.clone();
                    let handle = tokio::spawn(async move {
                        let mut stream = stream;
                        let sender = stream.sender();
                        while let Some(envelope) = stream.recv().await { task_runtime.dispatch_inbound(envelope, Some(sender.clone())).await; }
                        task_runtime.close_sessions_for_sender(&sender).await;
                        task_runtime.retain_inbound_tasks();
                    });
                    runtime.register_inbound_task(handle.abort_handle());
                }
            }
        }
        let _ = stopped_sender.send(true);
    });
    TransportTask {
        shutdown,
        force,
        task,
        stopped,
    }
}

pub(crate) struct RenewalTiming {
    pub ttl: Duration,
    pub operation_timeout: Duration,
    pub interval: Duration,
}

pub fn spawn_lease_renewal<S>(
    runtime: Arc<ServerInner<S>>,
    authority: Arc<NodeAuthority>,
    leases: Arc<dyn NodeLeaseStore>,
    mut node: NodeRecord,
    mut revision: Revision,
    timing: RenewalTiming,
) -> RenewalTask
where
    S: Send + Sync + 'static,
{
    let (shutdown, mut shutdown_receiver) = oneshot::channel();
    let (stopped_sender, stopped) = watch::channel(false);
    let task = tokio::spawn(async move {
        let _stopped_sender = stopped_sender;
        loop {
            let jitter = 0.8 + (rand::random::<u16>() % 401) as f64 / 1000.0;
            let renewal_due = tokio::time::Instant::now() + timing.interval.mul_f64(jitter);
            let wake_at = renewal_due.min(authority.deadline());
            tokio::select! {
                _ = tokio::time::sleep_until(wake_at) => {}
                _ = &mut shutdown_receiver => return RenewalExit { leases, node_id: node.node_id, revision, release: true },
            }
            if !authority.is_valid() {
                authority.fence();
                runtime.fence().await;
                return RenewalExit {
                    leases,
                    node_id: node.node_id,
                    revision,
                    release: false,
                };
            }
            let operation_started = tokio::time::Instant::now();
            node.lease_generation = node.lease_generation.saturating_add(1);
            runtime.update_capacity_sample(&mut node);
            let Some(remaining) = authority.remaining() else {
                authority.fence();
                runtime.fence().await;
                return RenewalExit {
                    leases,
                    node_id: node.node_id,
                    revision,
                    release: false,
                };
            };
            let outcome = tokio::time::timeout(
                timing.operation_timeout.min(remaining),
                leases.renew_node(node.clone(), timing.ttl, &revision),
            )
            .await;
            match outcome {
                Ok(Ok(MutationOutcome::Applied(next))) => {
                    revision = next;
                    authority.renew(operation_started);
                }
                Ok(Ok(MutationOutcome::Indeterminate(_))) => {
                    let Some(next) = confirm_node_lease(
                        leases.as_ref(),
                        &node,
                        authority.deadline(),
                        timing.operation_timeout,
                    )
                    .await
                    else {
                        authority.fence();
                        runtime.fence().await;
                        return RenewalExit {
                            leases,
                            node_id: node.node_id,
                            revision,
                            release: false,
                        };
                    };
                    revision = next;
                    authority.renew(operation_started);
                }
                Ok(Ok(MutationOutcome::Conflict)) => {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        leases,
                        node_id: node.node_id,
                        revision,
                        release: false,
                    };
                }
                _ if !authority.is_valid() => {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        leases,
                        node_id: node.node_id,
                        revision,
                        release: false,
                    };
                }
                _ => {}
            }
        }
    });
    RenewalTask {
        shutdown,
        task,
        stopped,
    }
}
