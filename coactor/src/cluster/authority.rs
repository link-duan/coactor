use std::{
    error::Error,
    fmt,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use super::{
    ClusterRouter,
    node::{NodeAuthority, RenewalTiming, spawn_lease_renewal},
};
use crate::runtime::ServerBuilderCore;
use crate::transport::grpc::GrpcTransport;
use crate::transport::{ClientTransport, Endpoint, ServerTransport};
use crate::{ActorAddress, Server, ServerError};

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeSessionId(Arc<str>);

impl NodeSessionId {
    pub fn generate() -> Self {
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

#[derive(Clone)]
pub(crate) struct ServerRuntimeConfig {
    pub node_id: String,
    pub bind_address: Option<SocketAddr>,
    pub advertised_endpoint: String,
    pub node_lease_ttl: Duration,
    pub coordination_timeout: Duration,
    pub server_transport: Arc<dyn ServerTransport>,
    pub client_transport: Arc<dyn ClientTransport>,
}

impl fmt::Debug for ServerRuntimeConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServerRuntimeConfig")
            .field("node_id", &self.node_id)
            .field("bind_address", &self.bind_address)
            .field("advertised_endpoint", &self.advertised_endpoint)
            .finish_non_exhaustive()
    }
}

impl ServerRuntimeConfig {
    pub(crate) fn production(
        node_id: String,
        bind_address: SocketAddr,
        advertised_endpoint: String,
        node_lease_ttl: Duration,
        coordination_timeout: Duration,
        peer_connect_timeout: Duration,
    ) -> Self {
        Self {
            node_id,
            bind_address: Some(bind_address),
            advertised_endpoint,
            node_lease_ttl,
            coordination_timeout,
            server_transport: Arc::new(GrpcTransport::new(peer_connect_timeout)),
            client_transport: Arc::new(GrpcTransport::new(peer_connect_timeout)),
        }
    }

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
            advertised_endpoint: endpoint.into(),
            node_lease_ttl: Duration::from_secs(10),
            coordination_timeout: Duration::from_secs(15),
            server_transport: Arc::new(transport.clone()),
            client_transport: Arc::new(transport),
        }
    }

    pub(crate) fn renewal_interval(&self) -> Duration {
        self.node_lease_ttl / 3
    }
}

pub(crate) fn canonical_endpoint(value: &str) -> Option<String> {
    if value.is_empty()
        || value.contains("://")
        || value.contains('/')
        || value.contains('?')
        || value.contains('#')
    {
        return None;
    }
    if let Ok(address) = value.parse::<SocketAddr>() {
        return (address.port() != 0).then(|| address.to_string());
    }
    let (host, port) = value.rsplit_once(':')?;
    if host.is_empty() || host.starts_with('[') || host.ends_with(']') || host.contains(':') {
        return None;
    }
    let port: u16 = port.parse().ok()?;
    let host = host.to_ascii_lowercase();
    if port == 0 || host.len() > 253 || host.split('.').any(|label| !crate::is_dns_label(label)) {
        return None;
    }
    Some(format!("{host}:{port}"))
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeRecord {
    #[serde(skip)]
    pub node_id: String,
    pub session_id: NodeSessionId,
    pub advertised_endpoint: String,
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
pub struct Revision(Arc<str>);
impl Revision {
    pub fn new(value: impl Into<Arc<str>>) -> Self {
        Self(value.into())
    }
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorOwner {
    pub node_id: String,
    pub session_id: NodeSessionId,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionedActorOwnerRecord {
    pub record: ActorOwnerRecord,
    pub revision: Revision,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinationErrorKind {
    Unavailable,
    PermissionDenied,
    InvalidData,
}

pub struct CoordinationError {
    kind: CoordinationErrorKind,
    source: Option<Box<dyn Error + Send + Sync>>,
}
impl CoordinationError {
    pub fn new(kind: CoordinationErrorKind, source: impl Error + Send + Sync + 'static) -> Self {
        Self {
            kind,
            source: Some(Box::new(source)),
        }
    }
    pub fn from_kind(kind: CoordinationErrorKind) -> Self {
        Self { kind, source: None }
    }
    pub fn kind(&self) -> CoordinationErrorKind {
        self.kind
    }
}
impl fmt::Debug for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CoordinationError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}
impl fmt::Display for CoordinationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.kind {
            CoordinationErrorKind::Unavailable => "coordination store is unavailable",
            CoordinationErrorKind::PermissionDenied => "coordination store permission denied",
            CoordinationErrorKind::InvalidData => "coordination store returned invalid data",
        })
    }
}
impl Error for CoordinationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|e| e as _)
    }
}

#[derive(Debug)]
pub enum MutationOutcome<T> {
    Applied(T),
    Conflict,
    Indeterminate(CoordinationError),
}

#[async_trait]
pub trait NodeDirectory: Send + Sync + 'static {
    async fn read_node(&self, node_id: &str) -> Result<Option<NodeRecord>, CoordinationError>;
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError>;
}

#[async_trait]
pub trait NodeLeaseStore: Send + Sync + 'static {
    async fn read_node_lease(
        &self,
        node_id: &str,
    ) -> Result<Option<(NodeRecord, Revision)>, CoordinationError>;
    async fn acquire_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
    ) -> Result<MutationOutcome<Revision>, CoordinationError>;
    async fn renew_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        revision: &Revision,
    ) -> Result<MutationOutcome<Revision>, CoordinationError>;
    async fn release_node(
        &self,
        node_id: &str,
        revision: &Revision,
    ) -> Result<MutationOutcome<()>, CoordinationError>;
}

#[async_trait]
pub trait ActorOwnerStore: Send + Sync + 'static {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError>;
    async fn compare_exchange_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        revision: Option<&Revision>,
    ) -> Result<MutationOutcome<Revision>, CoordinationError>;
    async fn release_actor_owner(
        &self,
        address: &ActorAddress,
        current: &VersionedActorOwnerRecord,
    ) -> Result<MutationOutcome<Revision>, CoordinationError> {
        self.compare_exchange_actor_owner(
            address,
            ActorOwnerRecord::unowned(current.record.ownership_epoch),
            Some(&current.revision),
        )
        .await
    }
}

pub trait CoordinationStore: NodeDirectory + NodeLeaseStore + ActorOwnerStore {}
impl<T> CoordinationStore for T where T: NodeDirectory + NodeLeaseStore + ActorOwnerStore {}

pub(crate) struct CoordinationStores {
    pub directory: Arc<dyn NodeDirectory>,
    pub node_leases: Arc<dyn NodeLeaseStore>,
    pub actor_owners: Arc<dyn ActorOwnerStore>,
}
impl CoordinationStores {
    pub(crate) fn new<T: CoordinationStore>(store: Arc<T>) -> Self {
        Self {
            directory: store.clone(),
            node_leases: store.clone(),
            actor_owners: store,
        }
    }
}

pub(crate) struct ServerStarter<S> {
    pub(crate) builder: ServerBuilderCore<S>,
    pub(crate) config: ServerRuntimeConfig,
    pub(crate) stores: CoordinationStores,
}

impl<S: Send + Sync + 'static> ServerStarter<S> {
    pub(crate) async fn start(self) -> Result<Server<S>, ServerError> {
        self.builder.validate()?;
        let listener = match self.config.bind_address {
            Some(address) => Some(
                tokio::net::TcpListener::bind(address)
                    .await
                    .map_err(ServerError::BindFailed)?,
            ),
            None => None,
        };
        let session_id = NodeSessionId::generate();
        let node = NodeRecord {
            node_id: self.config.node_id.clone(),
            session_id: session_id.clone(),
            advertised_endpoint: self.config.advertised_endpoint.clone(),
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
                .coordination_timeout
                .min(self.config.node_lease_ttl),
            self.stores
                .node_leases
                .acquire_node(node.clone(), self.config.node_lease_ttl),
        )
        .await
        .map_err(|_| ServerError::LeaseUnconfirmed)?
        .map_err(ServerError::Coordination)?;
        let revision = match acquired {
            MutationOutcome::Applied(revision) => revision,
            MutationOutcome::Conflict => return Err(ServerError::LeaseConflict),
            MutationOutcome::Indeterminate(_) => confirm_node_lease(
                self.stores.node_leases.as_ref(),
                &node,
                authority_started + self.config.node_lease_ttl,
                self.config.coordination_timeout,
            )
            .await
            .ok_or(ServerError::LeaseUnconfirmed)?,
        };
        let (termination_sender, termination_receiver) = watch::channel(false);
        let authority = Arc::new(NodeAuthority::new(
            authority_started,
            self.config.node_lease_ttl,
            termination_sender,
        ));
        if !authority.is_valid() {
            return Err(ServerError::LeaseUnconfirmed);
        }
        let cluster = ClusterRouter::new(
            self.stores.directory.clone(),
            self.stores.actor_owners.clone(),
            self.config.node_id.clone(),
            session_id,
            self.config.coordination_timeout,
            self.config.renewal_interval(),
        );
        let builder = self
            .builder
            .with_client_transport(self.config.client_transport.clone());
        let runtime = builder.build_with_authority(Some(authority.clone()), Some(cluster))?;
        let peer = runtime.spawn_peer(
            self.config.server_transport.clone(),
            &Endpoint::new(self.config.advertised_endpoint.clone()),
            listener,
        );
        let renewal = spawn_lease_renewal(
            runtime.inner.clone(),
            authority,
            self.stores.node_leases,
            node,
            revision,
            RenewalTiming {
                ttl: self.config.node_lease_ttl,
                operation_timeout: self.config.coordination_timeout,
                interval: self.config.renewal_interval(),
            },
        );
        Ok(runtime.with_cluster_tasks(peer, renewal, termination_receiver))
    }
}

pub(crate) async fn confirm_node_lease(
    leases: &dyn NodeLeaseStore,
    expected: &NodeRecord,
    deadline: tokio::time::Instant,
    operation_timeout: Duration,
) -> Option<Revision> {
    for _ in 0..3 {
        let remaining = deadline.checked_duration_since(tokio::time::Instant::now())?;
        let read_back = tokio::time::timeout(
            operation_timeout.min(remaining),
            leases.read_node_lease(&expected.node_id),
        )
        .await;
        if let Ok(Ok(Some((node, revision)))) = read_back {
            if node == *expected {
                return Some(revision);
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
