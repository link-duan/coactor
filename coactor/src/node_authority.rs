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

use crate::{__private, ActorAddress, BuildError, Runtime, RuntimeBuilder};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeSessionId(Arc<str>);

impl NodeSessionId {
    pub(crate) fn generate() -> Self {
        Self(uuid::Uuid::new_v4().to_string().into())
    }

    pub fn as_str(&self) -> &str {
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
pub struct DistributedRuntimeConfig {
    pub node_id: String,
    pub bind_address: SocketAddr,
    pub advertised_address: SocketAddr,
    pub lease_timing: LeaseTiming,
}

impl DistributedRuntimeConfig {
    pub fn new(
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

    pub fn lease_timing(mut self, timing: LeaseTiming) -> Self {
        self.lease_timing = timing;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), RuntimeStartError> {
        if self.node_id.trim().is_empty() {
            return Err(RuntimeStartError::InvalidNodeId);
        }
        if self.advertised_address.port() == 0 {
            return Err(RuntimeStartError::InvalidAdvertisedAddress);
        }
        let timing = self.lease_timing;
        if timing.ttl.is_zero()
            || timing.renewal_interval.is_zero()
            || timing.renewal_interval >= timing.ttl
            || timing.operation_timeout.is_zero()
            || timing.peer_connect_timeout.is_zero()
        {
            return Err(RuntimeStartError::InvalidLeaseTiming);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeLease {
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
pub struct VersionedNodeLease {
    pub lease: NodeLease,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorOwnerRecord {
    pub owner: Option<ActorOwner>,
    pub ownership_epoch: u64,
}

impl ActorOwnerRecord {
    pub fn unowned(ownership_epoch: u64) -> Self {
        Self {
            owner: None,
            ownership_epoch,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActorOwner {
    pub node_id: String,
    pub session_id: NodeSessionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedActorOwnerRecord {
    pub record: ActorOwnerRecord,
    pub etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LeaseMutation {
    Applied { etag: String },
    ConditionalRejected,
    Ambiguous(AmbiguousMutation),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AmbiguousMutation {
    Timeout,
    ResponseLost,
    DispatchUnknown,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum OwnershipStorageError {
    #[error("ownership storage is temporarily unavailable")]
    Unavailable,
    #[error("ownership storage operation failed")]
    Failed,
}

#[async_trait]
pub trait NodeLeaseStorage: Send + Sync + 'static {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipStorageError>;

    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipStorageError>;

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipStorageError>;

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError>;

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError>;
}

#[async_trait]
pub trait ActorOwnerStorage: Send + Sync + 'static {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, OwnershipStorageError>;

    async fn claim_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipStorageError>;

    async fn release_actor_owner(
        &self,
        address: &ActorAddress,
        current: &VersionedActorOwnerRecord,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.claim_actor_owner(
            address,
            ActorOwnerRecord::unowned(current.record.ownership_epoch),
            Some(&current.etag),
        )
        .await
    }
}

pub trait OwnershipStorage: NodeLeaseStorage + ActorOwnerStorage {}

impl<T> OwnershipStorage for T where T: NodeLeaseStorage + ActorOwnerStorage {}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum RuntimeStartError {
    #[error(transparent)]
    Build(#[from] BuildError),
    #[error("Node ID must not be empty")]
    InvalidNodeId,
    #[error("advertised address must have a non-zero port")]
    InvalidAdvertisedAddress,
    #[error("lease timing is invalid")]
    InvalidLeaseTiming,
    #[error("the peer listener could not bind")]
    BindFailed,
    #[error("Node Lease is already owned")]
    LeaseConflict,
    #[error("Node Lease acquisition could not be confirmed")]
    LeaseUnconfirmed,
    #[error("ownership storage failed during startup")]
    OwnershipStorage,
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

pub struct DistributedRuntimeBuilder<S> {
    pub(crate) builder: RuntimeBuilder<S>,
    pub(crate) config: DistributedRuntimeConfig,
    pub(crate) storage: Arc<dyn OwnershipStorage>,
}

impl<S> fmt::Debug for DistributedRuntimeBuilder<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DistributedRuntimeBuilder")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S> DistributedRuntimeBuilder<S>
where
    S: Send + Sync + 'static,
{
    pub async fn start(self) -> Result<Runtime<S>, RuntimeStartError> {
        self.builder.validate()?;
        let listener = tokio::net::TcpListener::bind(self.config.bind_address)
            .await
            .map_err(|_| RuntimeStartError::BindFailed)?;
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
        .map_err(|_| RuntimeStartError::LeaseUnconfirmed)?
        .map_err(|_| RuntimeStartError::OwnershipStorage)?;
        let etag = match acquired {
            LeaseMutation::Applied { etag } => etag,
            LeaseMutation::ConditionalRejected => return Err(RuntimeStartError::LeaseConflict),
            LeaseMutation::Ambiguous(_) => {
                let Some(etag) = confirm_node_lease(
                    self.storage.as_ref(),
                    &lease,
                    authority_started + self.config.lease_timing.ttl,
                    self.config.lease_timing.operation_timeout,
                )
                .await
                else {
                    return Err(RuntimeStartError::LeaseUnconfirmed);
                };
                etag
            }
        };

        let (termination_sender, termination_receiver) = watch::channel(None);
        let authority = Arc::new(__private::NodeAuthority::new(
            authority_started,
            self.config.lease_timing.ttl,
            termination_sender,
        ));
        if !authority.is_valid() {
            return Err(RuntimeStartError::LeaseUnconfirmed);
        }
        let distributed = __private::DistributedContext::new(
            self.storage.clone(),
            self.config.node_id.clone(),
            session_id,
            self.config.lease_timing.operation_timeout,
            self.config.lease_timing.peer_connect_timeout,
        );
        let runtime = self
            .builder
            .build_with_authority(Some(authority.clone()), Some(distributed))?;
        let peer = runtime.spawn_peer(listener);
        let renewal = __private::spawn_lease_renewal(
            runtime.inner.clone(),
            authority,
            self.storage,
            lease,
            etag,
            self.config.lease_timing,
        );
        Ok(runtime.with_distributed_tasks(peer, renewal, termination_receiver))
    }
}

pub(crate) async fn confirm_node_lease(
    storage: &dyn OwnershipStorage,
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
