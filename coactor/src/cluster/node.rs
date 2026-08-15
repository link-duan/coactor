use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{oneshot, watch};

use super::{confirm_node_lease, wall_time_millis};
use crate::{
    __macro::ServerInner, transport::grpc::GrpcTransport,
    transport::{Endpoint, ServerTransport}, LeaseMutation, LeaseTiming, NodeLease, NodeSessionId,
    OwnershipBackend, ServerTermination, ServerTerminationReason,
};

pub struct NodeAuthority {
    valid: AtomicBool,
    deadline: Mutex<tokio::time::Instant>,
    ttl: Duration,
    termination: watch::Sender<Option<ServerTermination>>,
}

impl NodeAuthority {
    pub fn new(
        operation_started: tokio::time::Instant,
        ttl: Duration,
        termination: watch::Sender<Option<ServerTermination>>,
    ) -> Self {
        Self {
            valid: AtomicBool::new(true),
            deadline: Mutex::new(operation_started + ttl),
            ttl,
            termination,
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
            let _ = self.termination.send(Some(ServerTermination {
                reason: ServerTerminationReason::Fenced,
            }));
        }
    }
}

pub struct PeerTask {
    /// 优雅停止：通知 serve 停止接受新连接（in-flight 流由 runtime 的 inbound task 中止）。
    pub shutdown: watch::Sender<bool>,
    /// shutdown 触发后立即终止 serve（不等外部连接），runtime 已终止全部 session。
    pub force: oneshot::Sender<()>,
    pub task: tokio::task::JoinHandle<()>,
}

pub struct RenewalTask {
    shutdown: oneshot::Sender<()>,
    task: tokio::task::JoinHandle<RenewalExit>,
}

struct RenewalExit {
    storage: Arc<dyn OwnershipBackend>,
    session_id: NodeSessionId,
    etag: String,
    release: bool,
}

pub struct ClusterTasks {
    pub peer: PeerTask,
    pub renewal: RenewalTask,
    pub termination: watch::Receiver<Option<ServerTermination>>,
}

impl ClusterTasks {
    pub async fn shutdown(self) {
        let _ = self.renewal.shutdown.send(());
        let _ = self.peer.shutdown.send(true);
        if let Ok(exit) = self.renewal.task.await {
            if exit.release {
                let _ = exit
                    .storage
                    .release_node_lease(&exit.session_id, &exit.etag)
                    .await;
            }
        }
        let _ = self.peer.force.send(());
        let _ = self.peer.task.await;
    }
}

pub fn spawn_peer<S>(runtime: Arc<ServerInner<S>>, listener: tokio::net::TcpListener) -> PeerTask
where
    S: Send + Sync + 'static,
{
    let (shutdown, shutdown_receiver) = watch::channel(false);
    let (force, force_receiver) = oneshot::channel();
    let connect_timeout = runtime
        .cluster
        .as_ref()
        .map_or(Duration::from_secs(3), |cluster| cluster.peer_connect_timeout);
    let transport = GrpcTransport::new(connect_timeout);
    let advertised = runtime
        .cluster
        .as_ref()
        .map_or_else(|| Endpoint::new("local"), |cluster| Endpoint::new(cluster.local_node_endpoint()));
    let mut listener = transport
        .listen(&advertised, Some(listener))
        .expect("peer listener bind");
    let task = tokio::spawn(async move {
        let mut shutdown_receiver = shutdown_receiver;
        let mut force_receiver = force_receiver;
        loop {
            tokio::select! {
                changed = shutdown_receiver.changed() => {
                    if changed.is_ok() && *shutdown_receiver.borrow() {
                        listener.shutdown();
                    }
                }
                _ = &mut force_receiver => break,
                stream = listener.accept() => {
                    let Some(stream) = stream else { break };
                    let task_runtime = runtime.clone();
                    let handle = tokio::spawn(async move {
                        let mut stream = stream;
                        while let Some(envelope) = stream.recv().await {
                            task_runtime
                                .dispatch_inbound(envelope, Some(stream.sender()))
                                .await;
                        }
                    });
                    runtime.register_inbound_task(handle.abort_handle());
                }
            }
        }
    });
    PeerTask {
        shutdown,
        force,
        task,
    }
}

pub fn spawn_lease_renewal<S>(
    runtime: Arc<ServerInner<S>>,
    authority: Arc<NodeAuthority>,
    storage: Arc<dyn OwnershipBackend>,
    mut lease: NodeLease,
    mut etag: String,
    timing: LeaseTiming,
) -> RenewalTask
where
    S: Send + Sync + 'static,
{
    let (shutdown, mut shutdown_receiver) = oneshot::channel();
    let task = tokio::spawn(async move {
        loop {
            let renewal_due = tokio::time::Instant::now() + timing.renewal_interval;
            let wake_at = renewal_due.min(authority.deadline());
            tokio::select! {
                _ = tokio::time::sleep_until(wake_at) => {}
                _ = &mut shutdown_receiver => return RenewalExit {
                    storage,
                    session_id: lease.session_id,
                    etag,
                    release: true,
                },
            }
            if !authority.is_valid() {
                authority.fence();
                runtime.fence().await;
                return RenewalExit {
                    storage,
                    session_id: lease.session_id,
                    etag,
                    release: false,
                };
            }
            let operation_started = tokio::time::Instant::now();
            runtime.update_capacity_sample(&mut lease);
            lease.expires_at_unix_ms =
                wall_time_millis().saturating_add(timing.ttl.as_millis() as u64);
            let Some(remaining) = authority.remaining() else {
                authority.fence();
                runtime.fence().await;
                return RenewalExit {
                    storage,
                    session_id: lease.session_id,
                    etag,
                    release: false,
                };
            };
            let outcome = tokio::time::timeout(
                timing.operation_timeout.min(remaining),
                storage.renew_node_lease(lease.clone(), &etag),
            )
            .await;
            match outcome {
                Ok(Ok(LeaseMutation::Applied { etag: next })) => {
                    etag = next;
                    authority.renew(operation_started);
                }
                Ok(Ok(LeaseMutation::Ambiguous(_))) => {
                    let Some(next) = confirm_node_lease(
                        storage.as_ref(),
                        &lease,
                        authority.deadline(),
                        timing.operation_timeout,
                    )
                    .await
                    else {
                        authority.fence();
                        runtime.fence().await;
                        return RenewalExit {
                            storage,
                            session_id: lease.session_id,
                            etag,
                            release: false,
                        };
                    };
                    etag = next;
                    authority.renew(operation_started);
                }
                Ok(Ok(LeaseMutation::ConditionalRejected)) => {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        storage,
                        session_id: lease.session_id,
                        etag,
                        release: false,
                    };
                }
                _ if !authority.is_valid() => {
                    authority.fence();
                    runtime.fence().await;
                    return RenewalExit {
                        storage,
                        session_id: lease.session_id,
                        etag,
                        release: false,
                    };
                }
                _ => {}
            }
        }
    });
    RenewalTask { shutdown, task }
}
