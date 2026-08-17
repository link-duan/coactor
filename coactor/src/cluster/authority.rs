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
use crate::transport::grpc::GrpcTransport;
use crate::transport::{ClientTransport, Endpoint, ServerTransport};
use crate::{ActorAddress, CoordinationConfig, Server, ServerBuilder, StartError};

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

#[derive(Clone)]
pub(crate) struct ServerRuntimeConfig {
    pub node_id: String,
    /// `None` = inmem（无 socket）。
    pub bind_address: Option<SocketAddr>,
    /// 完整 endpoint：gRPC 为 `http://host:port`，inmem 为 registry key。
    pub advertised_address: String,
    pub lease_timing: LeaseTiming,
    pub server_transport: Arc<dyn ServerTransport>,
    pub client_transport: Arc<dyn ClientTransport>,
}

impl std::fmt::Debug for ServerRuntimeConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ServerRuntimeConfig")
            .field("node_id", &self.node_id)
            .field("bind_address", &self.bind_address)
            .field("advertised_address", &self.advertised_address)
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    runtime: ServerRuntimeConfig,
    coordination: CoordinationConfig,
}

impl ServerConfig {
    pub fn new(
        node_id: impl Into<String>,
        bind_address: SocketAddr,
        advertised_address: SocketAddr,
        coordination: impl Into<CoordinationConfig>,
    ) -> Self {
        Self {
            runtime: ServerRuntimeConfig::new(node_id, bind_address, advertised_address),
            coordination: coordination.into(),
        }
    }

    pub fn lease_timing(mut self, timing: LeaseTiming) -> Self {
        self.runtime = self.runtime.lease_timing(timing);
        self
    }

    pub(crate) fn into_parts(self) -> (ServerRuntimeConfig, CoordinationConfig) {
        (self.runtime, self.coordination)
    }
}

impl ServerRuntimeConfig {
    /// gRPC 配置：绑定 socket，advertised 为 `http://host:port`。
    pub(crate) fn new(
        node_id: impl Into<String>,
        bind_address: SocketAddr,
        advertised_address: SocketAddr,
    ) -> Self {
        let transport = GrpcTransport::new(LeaseTiming::default().peer_connect_timeout);
        Self {
            node_id: node_id.into(),
            bind_address: Some(bind_address),
            advertised_address: format!("http://{advertised_address}"),
            lease_timing: LeaseTiming::default(),
            server_transport: Arc::new(transport),
            client_transport: Arc::new(GrpcTransport::new(
                LeaseTiming::default().peer_connect_timeout,
            )),
        }
    }

    /// inmem 配置（测试）：无 socket，advertised 为 registry key。
    #[cfg(test)]
    pub(crate) fn inmem(
        node_id: impl Into<String>,
        endpoint: impl Into<String>,
        registry: Arc<crate::transport::inmem::InmemRegistry>,
    ) -> Self {
        let transport = crate::transport::inmem::InmemTransport::new(registry);
        Self {
            node_id: node_id.into(),
            bind_address: None,
            advertised_address: endpoint.into(),
            lease_timing: LeaseTiming::default(),
            server_transport: Arc::new(transport.clone()),
            client_transport: Arc::new(transport),
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
        if self.advertised_address.trim().is_empty() {
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

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub(crate) struct NodeRecord {
    pub node_id: String,
    pub session_id: NodeSessionId,
    pub advertised_address: String,
    pub protocol_version: u32,
    #[serde(default)]
    pub lease_generation: u64,
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
pub(crate) struct Revision(Arc<str>);

impl Revision {
    pub(crate) fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LeaseToken(Arc<str>);

impl LeaseToken {
    pub(crate) fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }

    pub(crate) fn as_str(&self) -> &str {
        &self.0
    }
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
    pub revision: Revision,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum LeaseMutation {
    Applied { token: LeaseToken },
    Conflict,
    Ambiguous(AmbiguousMutation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Mutation {
    Applied { revision: Revision },
    Conflict,
    Ambiguous(AmbiguousMutation),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AmbiguousMutation {
    Timeout,
    ResponseLost,
    DispatchUnknown,
}

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub(crate) enum CoordinationError {
    #[error("coordination store is temporarily unavailable")]
    Unavailable,
    #[error("coordination store operation failed")]
    Failed,
}

#[async_trait]
pub(crate) trait NodeDirectory: Send + Sync + 'static {
    async fn read_node(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, CoordinationError>;

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError>;
}

#[async_trait]
pub(crate) trait NodeLeaseStore: Send + Sync + 'static {
    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<(NodeRecord, LeaseToken)>, CoordinationError>;

    async fn acquire_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
    ) -> Result<LeaseMutation, CoordinationError>;

    async fn renew_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        token: &LeaseToken,
    ) -> Result<LeaseMutation, CoordinationError>;

    async fn release_node(
        &self,
        session_id: &NodeSessionId,
        token: &LeaseToken,
    ) -> Result<LeaseMutation, CoordinationError>;
}

#[async_trait]
pub(crate) trait ActorOwnerStore: Send + Sync + 'static {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError>;

    async fn compare_exchange_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        revision: Option<&Revision>,
    ) -> Result<Mutation, CoordinationError>;

    async fn release_actor_owner(
        &self,
        address: &ActorAddress,
        current: &VersionedActorOwnerRecord,
    ) -> Result<Mutation, CoordinationError> {
        self.compare_exchange_actor_owner(
            address,
            ActorOwnerRecord::unowned(current.record.ownership_epoch),
            Some(&current.revision),
        )
        .await
    }
}

pub(crate) struct CoordinationStores {
    pub directory: Arc<dyn NodeDirectory>,
    pub node_leases: Arc<dyn NodeLeaseStore>,
    pub actor_owners: Arc<dyn ActorOwnerStore>,
}

impl CoordinationStores {
    pub(crate) fn new<T>(store: Arc<T>) -> Self
    where
        T: NodeDirectory + NodeLeaseStore + ActorOwnerStore,
    {
        Self {
            directory: store.clone(),
            node_leases: store.clone(),
            actor_owners: store,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerTerminationReason {
    Fenced,
    Shutdown,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerTermination {
    pub reason: ServerTerminationReason,
}

#[derive(Clone)]
pub struct ServerSupervision {
    pub(crate) receiver: watch::Receiver<Option<ServerTermination>>,
}

impl ServerSupervision {
    pub async fn terminated(mut self) -> ServerTermination {
        loop {
            if let Some(termination) = self.receiver.borrow().clone() {
                return termination;
            }
            if self.receiver.changed().await.is_err() {
                return ServerTermination {
                    reason: ServerTerminationReason::Shutdown,
                };
            }
        }
    }
}

pub(crate) struct ServerStarter<S> {
    pub(crate) builder: ServerBuilder<S>,
    pub(crate) config: ServerRuntimeConfig,
    pub(crate) stores: CoordinationStores,
}

impl<S> fmt::Debug for ServerStarter<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerStarter")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<S> ServerStarter<S>
where
    S: Send + Sync + 'static,
{
    pub async fn start(self) -> Result<Server<S>, StartError> {
        self.builder.validate()?;
        let listener = match self.config.bind_address {
            Some(address) => Some(
                tokio::net::TcpListener::bind(address)
                    .await
                    .map_err(|_| StartError::BindFailed)?,
            ),
            None => None,
        };
        let session_id = NodeSessionId::generate();
        let node = NodeRecord {
            node_id: self.config.node_id.clone(),
            session_id: session_id.clone(),
            advertised_address: self.config.advertised_address.clone(),
            protocol_version: crate::PEER_PROTOCOL_VERSION,
            lease_generation: 0,
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
            self.stores
                .node_leases
                .acquire_node(node.clone(), self.config.lease_timing.ttl),
        )
        .await
        .map_err(|_| StartError::LeaseUnconfirmed)?
        .map_err(|_| StartError::OwnershipUnavailable)?;
        let token = match acquired {
            LeaseMutation::Applied { token } => token,
            LeaseMutation::Conflict => return Err(StartError::LeaseConflict),
            LeaseMutation::Ambiguous(_) => {
                let Some(token) = confirm_node_lease(
                    self.stores.node_leases.as_ref(),
                    &node,
                    authority_started + self.config.lease_timing.ttl,
                    self.config.lease_timing.operation_timeout,
                )
                .await
                else {
                    return Err(StartError::LeaseUnconfirmed);
                };
                token
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
            self.stores.directory.clone(),
            self.stores.actor_owners.clone(),
            self.config.node_id.clone(),
            session_id,
            self.config.lease_timing.operation_timeout,
            self.config.lease_timing.renewal_interval,
        );
        let builder = self
            .builder
            .with_client_transport(self.config.client_transport.clone());
        let runtime = builder.build_with_authority(Some(authority.clone()), Some(cluster))?;
        let peer = runtime.spawn_peer(
            self.config.server_transport.clone(),
            &Endpoint::new(self.config.advertised_address.clone()),
            listener,
        );
        let renewal = spawn_lease_renewal(
            runtime.inner.clone(),
            authority,
            self.stores.node_leases,
            node,
            token,
            self.config.lease_timing,
        );
        Ok(runtime.with_cluster_tasks(peer, renewal, termination_receiver))
    }
}

pub(crate) async fn confirm_node_lease(
    leases: &dyn NodeLeaseStore,
    expected: &NodeRecord,
    deadline: tokio::time::Instant,
    operation_timeout: Duration,
) -> Option<LeaseToken> {
    for _ in 0..3 {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        let read_back = tokio::time::timeout(
            operation_timeout.min(remaining),
            leases.read_node_lease(&expected.session_id),
        )
        .await;
        if let Ok(Ok(Some((node, token)))) = read_back {
            if node == *expected {
                return Some(token);
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
