use std::{
    collections::HashMap, future::Future, marker::PhantomData, net::SocketAddr, pin::Pin,
    sync::Arc, time::Duration,
};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, watch};

use crate::cluster::{
    ClusterRouter, ClusterTasks, CoordinationStore, CoordinationStores, NodeAuthority, RenewalTask,
    ServerRuntimeConfig, ServerStarter, TransportTask, canonical_endpoint, spawn_transport,
};
use crate::runtime::session::SessionRegistry;
use crate::{__macro, IntoActorConfig, ServerError, TRANSPORT_PROTOCOL_VERSION};

#[doc(hidden)]
pub struct MissingState;
#[doc(hidden)]
pub struct ReadyState;

/// Builder for a production [`Server`].
pub struct ServerBuilder<C, S = (), P = MissingState> {
    store: C,
    core: ServerBuilderCore<S>,
    bind_address: Option<SocketAddr>,
    advertised_endpoint: Option<String>,
    node_id: Option<String>,
    node_lease_ttl: Duration,
    coordination_timeout: Duration,
    shutdown_signal: Option<Pin<Box<dyn Future<Output = ()> + Send + 'static>>>,
    phase: PhantomData<P>,
}

impl<S: Send + Sync + 'static> Server<S> {
    /// Creates a production Server builder backed by the supplied Coordination Store.
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
            shutdown_signal: None,
            phase: PhantomData,
        }
    }
}

impl<C, S, P> ServerBuilder<C, S, P>
where
    C: CoordinationStore,
    S: Send + Sync + 'static,
{
    /// Registers an Actor Type with either its name or an [`ActorConfig`](crate::ActorConfig).
    pub fn actor<A>(mut self, config: impl IntoActorConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.core.add_actor::<A>(config.into_actor_config());
        self
    }

    /// Sets the canonical `host:port` endpoint advertised to Clients.
    pub fn advertised_endpoint(mut self, endpoint: &str) -> Self {
        self.advertised_endpoint = Some(endpoint.to_owned());
        self
    }
    /// Sets the stable logical Node ID. It must be a Kubernetes DNS label.
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
    /// Sets the application-owned future that requests graceful Server shutdown.
    pub fn shutdown_signal<F>(mut self, signal: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.shutdown_signal = Some(Box::pin(signal));
        self
    }

    async fn serve_with_state(
        mut self,
        address: &str,
        state: Option<S>,
    ) -> Result<(), ServerError> {
        self.bind_address = Some(parse_listen_address(address)?);
        let shutdown_signal = self.shutdown_signal.take();
        let server = self.into_starter(state)?.start().await?;
        let result = match shutdown_signal {
            Some(signal) => {
                tokio::select! {
                    biased;
                    result = server.wait() => result,
                    () = signal => Ok(()),
                }
            }
            None => server.wait().await,
        };
        let shutdown_result = server.shutdown().await;
        result.and(shutdown_result)
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
            shutdown_signal: self.shutdown_signal,
            phase: PhantomData,
        }
    }

    fn into_starter(mut self, state: Option<S>) -> Result<ServerStarter<S>, ServerError> {
        if let Some(state) = state {
            self.core = self.core.with_state(state);
        }
        let bind_address = self.bind_address.expect("serve supplies the bind address");
        let raw_endpoint = self
            .advertised_endpoint
            .ok_or(ServerError::MissingAdvertisedEndpoint)?;
        let advertised_endpoint =
            canonical_endpoint(&raw_endpoint).ok_or(ServerError::InvalidAdvertisedEndpoint)?;
        let node_id = match self.node_id {
            Some(node_id) if crate::is_dns_label(&node_id) => node_id,
            Some(_) => return Err(ServerError::InvalidNodeId),
            None => advertised_endpoint.clone(),
        };
        if self.node_lease_ttl.is_zero() {
            return Err(ServerError::InvalidNodeLeaseTtl);
        }
        if self.coordination_timeout.is_zero() {
            return Err(ServerError::InvalidCoordinationTimeout);
        }
        let config = ServerRuntimeConfig::production(
            node_id,
            bind_address,
            advertised_endpoint,
            self.node_lease_ttl,
            self.coordination_timeout,
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
    /// Injects the App State shared by every Actor Type in this Server.
    pub fn with_state(self, state: S) -> ServerBuilder<C, S, ReadyState> {
        self.ready(state)
    }
}

impl<C> ServerBuilder<C, (), MissingState>
where
    C: CoordinationStore,
{
    /// Runs the Server until it self-fences, its tasks stop, or the shutdown signal completes.
    ///
    /// Dropping or aborting this future is not cancellation-safe and does not guarantee
    /// graceful cleanup or Node Lease release. Configure [`Self::shutdown_signal`] and
    /// continue awaiting `serve` to shut down gracefully.
    pub async fn serve(self, address: &str) -> Result<(), ServerError> {
        self.serve_with_state(address, Some(())).await
    }
}

impl<C, S> ServerBuilder<C, S, ReadyState>
where
    C: CoordinationStore,
    S: Send + Sync + 'static,
{
    /// Runs the Server until it self-fences, its tasks stop, or the shutdown signal completes.
    ///
    /// Dropping or aborting this future is not cancellation-safe and does not guarantee
    /// graceful cleanup or Node Lease release. Configure [`Self::shutdown_signal`] and
    /// continue awaiting `serve` to shut down gracefully.
    pub async fn serve(self, address: &str) -> Result<(), ServerError> {
        self.serve_with_state(address, None).await
    }
}

fn parse_listen_address(address: &str) -> Result<SocketAddr, ServerError> {
    if let Some(port) = address.strip_prefix(':') {
        if port.is_empty() || port.contains(':') {
            return Err(ServerError::InvalidListenAddress);
        }
        return format!("0.0.0.0:{port}")
            .parse()
            .map_err(|_| ServerError::InvalidListenAddress);
    }
    address
        .parse()
        .map_err(|_| ServerError::InvalidListenAddress)
}

pub(crate) struct ServerBuilderCore<S> {
    pub(crate) state: Option<S>,
    registrations: Vec<__macro::Registration<S>>,
    pub(crate) mailbox_capacity: usize,
    pub(crate) max_active_actors: usize,
    pub(crate) idle_timeout: Duration,
    pub(crate) deactivation_timeout: Duration,
    pub(crate) shutdown_timeout: Duration,
    transport_protocol_version: u32,
}

impl<S: Send + Sync + 'static> ServerBuilderCore<S> {
    pub(crate) fn base(state: Option<S>) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            mailbox_capacity: 32,
            max_active_actors: 10_000,
            idle_timeout: Duration::from_secs(60),
            deactivation_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            transport_protocol_version: TRANSPORT_PROTOCOL_VERSION,
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
    pub(crate) fn validate(&self) -> Result<(), ServerError> {
        if self.mailbox_capacity == 0 {
            return Err(ServerError::InvalidMailboxCapacity);
        }
        if self.max_active_actors == 0 {
            return Err(ServerError::InvalidMaxActiveActors);
        }
        if self.deactivation_timeout.is_zero() {
            return Err(ServerError::InvalidDeactivationTimeout);
        }
        if self.shutdown_timeout.is_zero() {
            return Err(ServerError::InvalidShutdownTimeout);
        }
        let mut names = std::collections::HashSet::new();
        for registration in &self.registrations {
            if !crate::is_dns_label(registration.name) {
                return Err(ServerError::InvalidActorType(registration.name.to_owned()));
            }
            if registration.mailbox_capacity == Some(0) {
                return Err(ServerError::InvalidMailboxCapacity);
            }
            if !names.insert(registration.name) {
                return Err(ServerError::DuplicateActorType(
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
    ) -> Result<Server<S>, ServerError> {
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
                transport_protocol_version: self.transport_protocol_version,
                authority,
                cluster,
                inbound_tasks: Mutex::new(Vec::new()),
            }),
            cluster: None,
        })
    }
}

/// Entry point for configuring and serving a production Actor Server.
pub struct Server<S = ()> {
    pub(crate) inner: Arc<__macro::ServerInner<S>>,
    cluster: Option<ClusterTasks>,
}
impl<S: Send + Sync + 'static> Server<S> {
    pub(crate) fn spawn_transport(
        &self,
        transport: Arc<dyn crate::transport::ServerTransport>,
        advertised: &crate::transport::Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> TransportTask {
        spawn_transport(self.inner.clone(), transport, advertised, listener)
    }
    pub(crate) fn with_cluster_tasks(
        mut self,
        transport: TransportTask,
        renewal: RenewalTask,
        fenced: watch::Receiver<bool>,
    ) -> Self {
        self.cluster = Some(ClusterTasks {
            transport,
            renewal,
            fenced,
        });
        self
    }
    /// Waits until the Server self-fences or its cluster tasks stop.
    pub(crate) async fn wait(&self) -> Result<(), ServerError> {
        let Some(tasks) = &self.cluster else {
            return Ok(());
        };
        let mut fenced = tasks.fenced.clone();
        let mut transport_stopped = tasks.transport.stopped.clone();
        let mut renewal_stopped = tasks.renewal.stopped.clone();
        loop {
            if *fenced.borrow() {
                return Err(ServerError::Fenced);
            }
            if *transport_stopped.borrow() {
                return Err(ServerError::ServiceStopped);
            }
            tokio::select! {
                biased;
                changed = fenced.changed() => {
                    if changed.is_err() {
                        return Err(ServerError::ServiceStopped);
                    }
                }
                changed = transport_stopped.changed() => {
                    if changed.is_err() || *transport_stopped.borrow() {
                        return Err(ServerError::ServiceStopped);
                    }
                }
                _ = renewal_stopped.changed() => {
                    return Err(ServerError::ServiceStopped);
                }
            }
        }
    }
    /// Gracefully stops Actors, transport, and Node Lease renewal.
    pub(crate) async fn shutdown(mut self) -> Result<(), ServerError> {
        let result = self.inner.shutdown().await;
        if let Some(tasks) = self.cluster.take() {
            tasks.shutdown().await;
        }
        result
    }
}

pub(crate) mod actor;
pub(crate) mod core;
pub(crate) mod lifecycle;
pub(crate) mod message;
pub(crate) mod route;
pub(crate) mod session;
pub(crate) mod shutdown;
