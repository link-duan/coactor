use std::{collections::HashMap, marker::PhantomData, net::SocketAddr, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, watch};

use crate::cluster::{
    ClusterRouter, ClusterTasks, CoordinationStore, CoordinationStores, NodeAuthority, PeerTask,
    RenewalTask, ServerRuntimeConfig, ServerStarter, canonical_endpoint, spawn_peer,
};
use crate::runtime::session::SessionRegistry;
use crate::{
    __macro, IntoActorConfig, PEER_PROTOCOL_VERSION, PlacementStrategy, ServerFailure,
    ServerStartError, transport::ClientTransport,
};

#[doc(hidden)]
pub struct MissingState;
#[doc(hidden)]
pub struct ReadyState;

pub struct ServerBuilder<C, S = (), P = MissingState> {
    store: C,
    core: ServerBuilderCore<S>,
    bind_address: Option<SocketAddr>,
    advertised_endpoint: Option<String>,
    node_id: Option<String>,
    node_lease_ttl: Duration,
    coordination_timeout: Duration,
    peer_connect_timeout: Duration,
    phase: PhantomData<P>,
}

impl<S: Send + Sync + 'static> Server<S> {
    pub fn builder<C>(store: C) -> ServerBuilder<C, S, MissingState>
    where
        C: CoordinationStore,
    {
        ServerBuilder {
            store,
            core: ServerBuilderCore::base(None),
            bind_address: None,
            advertised_endpoint: None,
            node_id: None,
            node_lease_ttl: Duration::from_secs(10),
            coordination_timeout: Duration::from_secs(15),
            peer_connect_timeout: Duration::from_secs(3),
            phase: PhantomData,
        }
    }
}

impl<C, S, P> ServerBuilder<C, S, P>
where
    C: CoordinationStore,
    S: Send + Sync + 'static,
{
    pub fn actor<A>(mut self, config: impl IntoActorConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.core.add_actor::<A>(config.into_actor_config());
        self
    }

    pub fn bind(mut self, address: SocketAddr) -> Self {
        self.bind_address = Some(address);
        self
    }
    pub fn advertised_endpoint(mut self, endpoint: &str) -> Self {
        self.advertised_endpoint = Some(endpoint.to_owned());
        self
    }
    pub fn node_id(mut self, node_id: &str) -> Self {
        self.node_id = Some(node_id.to_owned());
        self
    }
    pub fn default_mailbox_capacity(mut self, capacity: usize) -> Self {
        self.core.mailbox_capacity = capacity;
        self
    }
    pub fn max_active_actors(mut self, maximum: usize) -> Self {
        self.core.max_active_actors = maximum;
        self
    }
    pub fn default_idle_timeout(mut self, timeout: Duration) -> Self {
        self.core.idle_timeout = timeout;
        self
    }
    pub fn deactivation_timeout(mut self, timeout: Duration) -> Self {
        self.core.deactivation_timeout = timeout;
        self
    }
    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.core.shutdown_timeout = timeout;
        self
    }
    pub fn node_lease_ttl(mut self, ttl: Duration) -> Self {
        self.node_lease_ttl = ttl;
        self
    }
    pub fn coordination_timeout(mut self, timeout: Duration) -> Self {
        self.coordination_timeout = timeout;
        self
    }
    pub fn peer_connect_timeout(mut self, timeout: Duration) -> Self {
        self.peer_connect_timeout = timeout;
        self
    }

    fn ready(self, state: S) -> ServerBuilder<C, S, ReadyState> {
        ServerBuilder {
            store: self.store,
            core: self.core.with_state(state),
            bind_address: self.bind_address,
            advertised_endpoint: self.advertised_endpoint,
            node_id: self.node_id,
            node_lease_ttl: self.node_lease_ttl,
            coordination_timeout: self.coordination_timeout,
            peer_connect_timeout: self.peer_connect_timeout,
            phase: PhantomData,
        }
    }

    fn into_starter(mut self, state: Option<S>) -> Result<ServerStarter<S>, ServerStartError> {
        if let Some(state) = state {
            self.core = self.core.with_state(state);
        }
        let bind_address = self
            .bind_address
            .ok_or(ServerStartError::MissingBindAddress)?;
        let raw_endpoint = self
            .advertised_endpoint
            .ok_or(ServerStartError::MissingAdvertisedEndpoint)?;
        let advertised_endpoint =
            canonical_endpoint(&raw_endpoint).ok_or(ServerStartError::InvalidAdvertisedEndpoint)?;
        let node_id = match self.node_id {
            Some(node_id) if crate::is_dns_label(&node_id) => node_id,
            Some(_) => return Err(ServerStartError::InvalidNodeId),
            None => advertised_endpoint.clone(),
        };
        if self.node_lease_ttl.is_zero() {
            return Err(ServerStartError::InvalidNodeLeaseTtl);
        }
        if self.coordination_timeout.is_zero() {
            return Err(ServerStartError::InvalidCoordinationTimeout);
        }
        if self.peer_connect_timeout.is_zero() {
            return Err(ServerStartError::InvalidPeerConnectTimeout);
        }
        let config = ServerRuntimeConfig::production(
            node_id,
            bind_address,
            advertised_endpoint,
            self.node_lease_ttl,
            self.coordination_timeout,
            self.peer_connect_timeout,
        );
        let store = Arc::new(self.store);
        Ok(ServerStarter {
            builder: self.core,
            config,
            stores: CoordinationStores::new(store),
        })
    }
}

impl<C, S> ServerBuilder<C, S, MissingState>
where
    C: CoordinationStore,
    S: Send + Sync + 'static,
{
    pub fn with_state(self, state: S) -> ServerBuilder<C, S, ReadyState> {
        self.ready(state)
    }
}

impl<C> ServerBuilder<C, (), MissingState>
where
    C: CoordinationStore,
{
    pub async fn start(self) -> Result<Server<()>, ServerStartError> {
        self.into_starter(Some(()))?.start().await
    }
}

impl<C, S> ServerBuilder<C, S, ReadyState>
where
    C: CoordinationStore,
    S: Send + Sync + 'static,
{
    pub async fn start(self) -> Result<Server<S>, ServerStartError> {
        self.into_starter(None)?.start().await
    }
}

pub(crate) struct ServerBuilderCore<S> {
    pub(crate) state: Option<S>,
    registrations: Vec<__macro::Registration<S>>,
    client_transport: Option<Arc<dyn ClientTransport>>,
    placement: Arc<dyn PlacementStrategy>,
    pub(crate) mailbox_capacity: usize,
    pub(crate) max_active_actors: usize,
    pub(crate) idle_timeout: Duration,
    pub(crate) deactivation_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    peer_protocol_version: u32,
}

impl<S: Send + Sync + 'static> ServerBuilderCore<S> {
    pub(crate) fn base(state: Option<S>) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            client_transport: None,
            placement: crate::cluster::default_placement(),
            mailbox_capacity: 32,
            max_active_actors: 10_000,
            idle_timeout: Duration::from_secs(60),
            deactivation_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            peer_protocol_version: PEER_PROTOCOL_VERSION,
        }
    }
    pub(crate) fn with_state(mut self, state: S) -> Self {
        self.state = Some(state);
        self
    }
    pub(crate) fn add_actor<A>(&mut self, config: crate::ActorConfig)
    where
        A: __macro::ActorType<S>,
    {
        let mut registration = __macro::Registration::of::<A>(config.name);
        registration.mailbox_capacity = config.mailbox_capacity;
        registration.idle_timeout = config.idle_timeout;
        self.registrations.push(registration);
    }
    pub(crate) fn with_client_transport(mut self, transport: Arc<dyn ClientTransport>) -> Self {
        self.client_transport = Some(transport);
        self
    }
    pub(crate) fn validate(&self) -> Result<(), ServerStartError> {
        if self.mailbox_capacity == 0 {
            return Err(ServerStartError::InvalidMailboxCapacity);
        }
        if self.max_active_actors == 0 {
            return Err(ServerStartError::InvalidMaxActiveActors);
        }
        if self.deactivation_timeout.is_zero() {
            return Err(ServerStartError::InvalidDeactivationTimeout);
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ServerStartError::InvalidShutdownTimeout);
        }
        let mut names = std::collections::HashSet::new();
        for registration in &self.registrations {
            if !crate::is_dns_label(registration.name) {
                return Err(ServerStartError::InvalidActorType(
                    registration.name.to_owned(),
                ));
            }
            if registration.mailbox_capacity == Some(0) {
                return Err(ServerStartError::InvalidMailboxCapacity);
            }
            if !names.insert(registration.name) {
                return Err(ServerStartError::DuplicateActorType(
                    registration.name.to_owned(),
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn active_actor_limit(&self) -> usize {
        self.max_active_actors
    }
    pub(crate) fn build_with_authority(
        self,
        authority: Option<Arc<NodeAuthority>>,
        cluster: Option<Arc<ClusterRouter>>,
    ) -> Result<Server<S>, ServerStartError> {
        self.validate()?;
        let mut registrations = HashMap::new();
        for mut registration in self.registrations {
            if registration.mailbox_capacity.is_none() {
                registration.mailbox_capacity = Some(self.mailbox_capacity);
            }
            if registration.idle_timeout.is_none() {
                registration.idle_timeout = Some(self.idle_timeout);
            }
            registrations.insert(registration.name, registration);
        }
        Ok(Server {
            inner: Arc::new(__macro::ServerInner {
                state: Arc::new(self.state.expect("State is supplied before build")),
                registrations,
                actors: Mutex::new(HashMap::new()),
                sessions: SessionRegistry::new(),
                capacity: Arc::new(Semaphore::new(self.max_active_actors)),
                max_active_actors: self.max_active_actors,
                deactivation_timeout: self.deactivation_timeout,
                next_generation: std::sync::atomic::AtomicU64::new(1),
                status: std::sync::atomic::AtomicU8::new(__macro::RUNNING),
                shutdown_timeout: self.shutdown_timeout,
                peer_protocol_version: self.peer_protocol_version,
                authority,
                cluster,
                channels: Mutex::new(HashMap::new()),
                inbound_tasks: Mutex::new(Vec::new()),
                pending_opens: Mutex::new(HashMap::new()),
                client_transport: self.client_transport,
                placement: self.placement,
                relays: Mutex::new(HashMap::new()),
            }),
            cluster: None,
        })
    }
}

pub struct Server<S = ()> {
    pub(crate) inner: Arc<__macro::ServerInner<S>>,
    cluster: Option<ClusterTasks>,
}
impl<S: Send + Sync + 'static> Server<S> {
    pub(crate) fn spawn_peer(
        &self,
        transport: Arc<dyn crate::transport::ServerTransport>,
        advertised: &crate::transport::Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> PeerTask {
        spawn_peer(self.inner.clone(), transport, advertised, listener)
    }
    pub(crate) fn with_cluster_tasks(
        mut self,
        peer: PeerTask,
        renewal: RenewalTask,
        fenced: watch::Receiver<bool>,
    ) -> Self {
        self.cluster = Some(ClusterTasks {
            peer,
            renewal,
            fenced,
        });
        self
    }
    pub async fn wait(&self) -> Result<(), ServerFailure> {
        let Some(tasks) = &self.cluster else {
            return Ok(());
        };
        let mut fenced = tasks.fenced.clone();
        loop {
            if *fenced.borrow() {
                return Err(ServerFailure::Fenced);
            }
            if fenced.changed().await.is_err() {
                return Ok(());
            }
        }
    }
    pub async fn shutdown(mut self) {
        self.inner.shutdown().await;
        if let Some(tasks) = self.cluster.take() {
            tasks.shutdown().await;
        }
    }
}

pub(crate) mod actor;
pub(crate) mod core;
pub(crate) mod lifecycle;
pub(crate) mod message;
pub(crate) mod route;
pub(crate) mod session;
pub(crate) mod shutdown;
