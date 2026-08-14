use std::{
    fmt,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::watch;

use super::{
    ClusterRouter,
    node::{NodeAuthority, spawn_lease_renewal},
};
use crate::{ActorAddress, Runtime, RuntimeBuilder, S3OwnershipConfig, StartError};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct NodeSessionId(Arc<str>);

impl NodeSessionId {
    pub(crate) fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string().into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeSessionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseTiming {
    pub ttl: Duration,
    pub renewal_interval: Duration,
    pub operation_timeout: Duration,
    pub peer_connect_timeout: Duration,
}

impl Default for LeaseTiming {
    fn default() -> Self {
        let ttl = Duration::from_secs(10);
        Self {
            ttl,
            renewal_interval: ttl / 3,
            operation_timeout: Duration::from_secs(15),
            peer_connect_timeout: Duration::from_secs(3),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ClusterRuntimeConfig {
    pub node_id: String,
    pub bind_address: SocketAddr,
    pub advertised_address: SocketAddr,
    pub lease_timing: LeaseTiming,
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    runtime: ClusterRuntimeConfig,
    ownership: S3OwnershipConfig,
}

impl ClusterConfig {
    pub fn new(
        node_id: impl Into<String>,
        bind_address: SocketAddr,
        advertised_address: SocketAddr,
        ownership: S3OwnershipConfig,
    ) -> Self {
        Self {
            runtime: ClusterRuntimeConfig::new(node_id, bind_address, advertised_address),
            ownership,
        }
    }

    pub fn lease_timing(mut self, timing: LeaseTiming) -> Self {
        self.runtime = self.runtime.lease_timing(timing);
        self
    }

    pub(crate) fn into_parts(self) -> (ClusterRuntimeConfig, S3OwnershipConfig) {
        (self.runtime, self.ownership)
    }
}

impl ClusterRuntimeConfig {
    pub(crate) fn new(
        node_id: impl Into<String>,
        bind_address: SocketAddr,
        advertised_address: SocketAddr,
    ) -> Self {
        Self {
            node_id: node_id.into(),
            bind_address,
            advertised_address,
            lease_timing: LeaseTiming::default(),
        }
    }

    pub(crate) fn lease_timing(mut self, timing: LeaseTiming) -> Self {
        self.lease_timing = timing;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), StartError> {
        if self.node_id.trim().is_empty() {
            return Err(StartError::InvalidNodeId);
        }
        if self.advertised_address.port() == 0 {
            return Err(StartError::InvalidAdvertisedAddress);
        }
        let timing = self.lease_timing;
        if timing.ttl.is_zero()
            || timing.renewal_interval.is_zero()
            || timing.renewal_interval >= timing.ttl
            || timing.operation_timeout.is_zero()
            || timing.peer_connect_timeout.is_zero()
        {
            return Err(StartError::InvalidLeaseTiming);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NodeLease {
    pub node_id: String,
    pub session_id: NodeSessionId,
    pub advertised_address: SocketAddr,
    pub protocol_version: u32,
    pub expires_at_unix_ms: u64,
    #[serde(default)]
    pub sampled_at_unix_ms: u64,
    #[serde(default)]
    pub active_actor_count: usize,
    #[serde(default)]
    pub max_actor_count: usize,
    #[serde(default)]
    pub pressured: bool,
    #[serde(default)]
    pub draining: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionedNodeLease {
    pub lease: NodeLease,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ActorOwnerRecord {
    pub owner: Option<ActorOwner>,
    pub ownership_epoch: u64,
}

impl ActorOwnerRecord {
    pub(crate) fn unowned(ownership_epoch: u64) -> Self {
        Self {
            owner: None,
            ownership_epoch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ActorOwner {
    pub node_id: String,
    pub session_id: NodeSessionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VersionedActorOwnerRecord {
    pub record: ActorOwnerRecord,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LeaseMutation {
    Applied { etag: String },
    ConditionalRejected,
    Ambiguous(AmbiguousMutation),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmbiguousMutation {
    Timeout,
    ResponseLost,
    DispatchUnknown,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum OwnershipBackendError {
    #[error("ownership storage is temporarily unavailable")]
    Unavailable,
    #[error("ownership storage operation failed")]
    Failed,
}

#[async_trait]
pub(crate) trait OwnershipBackend: Send + Sync + 'static {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipBackendError>;

    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipBackendError>;

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipBackendError>;

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError>;

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError>;

    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, OwnershipBackendError>;

    async fn claim_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipBackendError>;

    async fn release_actor_owner(
        &self,
        address: &ActorAddress,
        current: &VersionedActorOwnerRecord,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        self.claim_actor_owner(
            address,
            ActorOwnerRecord::unowned(current.record.ownership_epoch),
            Some(&current.etag),
        )
        .await
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RuntimeTerminationReason {
    Fenced,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RuntimeTermination {
    pub reason: RuntimeTerminationReason,
}

#[derive(Clone)]
pub struct RuntimeSupervision {
    pub(crate) receiver: watch::Receiver<Option<RuntimeTermination>>,
}

impl RuntimeSupervision {
    pub async fn terminated(mut self) -> RuntimeTermination {
        loop {
            if let Some(termination) = self.receiver.borrow().clone() {
                return termination;
            }
            if self.receiver.changed().await.is_err() {
                return RuntimeTermination {
                    reason: RuntimeTerminationReason::Shutdown,
                };
            }
        }
    }
}

pub(crate) struct ClusterStarter<S> {
    pub(crate) builder: RuntimeBuilder<S>,
    pub(crate) config: ClusterRuntimeConfig,
    pub(crate) storage: Arc<dyn OwnershipBackend>,
}

impl<S> fmt::Debug for ClusterStarter<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClusterStarter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S> ClusterStarter<S>
where
    S: Send + Sync + 'static,
{
    pub async fn start(self) -> Result<Runtime<S>, StartError> {
        self.builder.validate()?;
        let listener = tokio::net::TcpListener::bind(self.config.bind_address)
            .await
            .map_err(|_| StartError::BindFailed)?;
        let session_id = NodeSessionId::generate();
        let lease = NodeLease {
            node_id: self.config.node_id.clone(),
            session_id: session_id.clone(),
            advertised_address: self.config.advertised_address,
            protocol_version: crate::PEER_PROTOCOL_VERSION,
            expires_at_unix_ms: wall_time_millis()
                .saturating_add(self.config.lease_timing.ttl.as_millis() as u64),
            sampled_at_unix_ms: wall_time_millis(),
            active_actor_count: 0,
            max_actor_count: self.builder.active_actor_limit(),
            pressured: false,
            draining: false,
        };
        let authority_started = tokio::time::Instant::now();
        let acquired = tokio::time::timeout(
            self.config
                .lease_timing
                .operation_timeout
                .min(self.config.lease_timing.ttl),
            self.storage.acquire_node_lease(lease.clone()),
        )
        .await
        .map_err(|_| StartError::LeaseUnconfirmed)?
        .map_err(|_| StartError::OwnershipUnavailable)?;
        let etag = match acquired {
            LeaseMutation::Applied { etag } => etag,
            LeaseMutation::ConditionalRejected => return Err(StartError::LeaseConflict),
            LeaseMutation::Ambiguous(_) => {
                let Some(etag) = confirm_node_lease(
                    self.storage.as_ref(),
                    &lease,
                    authority_started + self.config.lease_timing.ttl,
                    self.config.lease_timing.operation_timeout,
                )
                .await
                else {
                    return Err(StartError::LeaseUnconfirmed);
                };
                etag
            }
        };

        let (termination_sender, termination_receiver) = watch::channel(None);
        let authority = Arc::new(NodeAuthority::new(
            authority_started,
            self.config.lease_timing.ttl,
            termination_sender,
        ));
        if !authority.is_valid() {
            return Err(StartError::LeaseUnconfirmed);
        }
        let cluster = ClusterRouter::new(
            self.storage.clone(),
            self.config.node_id.clone(),
            session_id,
            self.config.lease_timing.operation_timeout,
            self.config.lease_timing.peer_connect_timeout,
        );
        let runtime = self
            .builder
            .build_with_authority(Some(authority.clone()), Some(cluster))?;
        let peer = runtime.spawn_peer(listener);
        let renewal = spawn_lease_renewal(
            runtime.inner.clone(),
            authority,
            self.storage,
            lease,
            etag,
            self.config.lease_timing,
        );
        Ok(runtime.with_cluster_tasks(peer, renewal, termination_receiver))
    }
}

pub(crate) async fn confirm_node_lease(
    storage: &dyn OwnershipBackend,
    expected: &NodeLease,
    deadline: tokio::time::Instant,
    operation_timeout: Duration,
) -> Option<String> {
    for _ in 0..3 {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        let read_back = tokio::time::timeout(
            operation_timeout.min(remaining),
            storage.read_node_lease(&expected.session_id),
        )
        .await;
        if let Ok(Ok(Some(versioned))) = read_back {
            if versioned.lease == *expected {
                return Some(versioned.etag);
            }
        }
    }
    None
}

pub(crate) fn wall_time_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}
