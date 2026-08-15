use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, watch};

use crate::cluster::{
    ClusterRouter, ClusterTasks, NodeAuthority, PeerTask, RenewalTask, spawn_peer,
};
use crate::{
    __macro, transport::ClientTransport, ActorTypeConfig, ServerConfig,
    ServerStarter, PEER_PROTOCOL_VERSION, ServerSupervision, ServerTermination, S3OwnershipBackend,
    StartError,
};
#[cfg(test)]
use crate::{ServerRuntimeConfig, OwnershipBackend};
use crate::runtime::session::SessionRegistry;

pub struct ServerBuilder<S> {
    state: S,
    registrations: Vec<__macro::Registration<S>>,
    cluster: Option<ServerConfig>,
    client_transport: Option<Arc<dyn ClientTransport>>,
    mailbox_capacity: usize,
    max_active_actors: usize,
    idle_timeout: Duration,
    deactivation_timeout: Duration,
    shutdown_timeout: Duration,
    peer_protocol_version: u32,
}

impl<S> ServerBuilder<S>
where
    S: Send + Sync + 'static,
{
    fn base(state: S) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            cluster: None,
            client_transport: None,
            mailbox_capacity: 32,
            max_active_actors: 10_000,
            idle_timeout: Duration::from_secs(60),
            deactivation_timeout: Duration::from_secs(5),
            shutdown_timeout: Duration::from_secs(30),
            peer_protocol_version: PEER_PROTOCOL_VERSION,
        }
    }

    pub fn local(state: S) -> Self {
        Self::base(state)
    }

    pub fn cluster(state: S, config: ServerConfig) -> Self {
        let mut builder = Self::base(state);
        builder.cluster = Some(config);
        builder
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
        self
    }

    /// 出站连接的 client transport（网关转发用）；由 ServerStarter 装配。
    pub(crate) fn with_client_transport(mut self, transport: Arc<dyn ClientTransport>) -> Self {
        self.client_transport = Some(transport);
        self
    }

    pub fn max_active_actors(mut self, maximum: usize) -> Self {
        self.max_active_actors = maximum;
        self
    }

    pub fn idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    pub fn deactivation_timeout(mut self, timeout: Duration) -> Self {
        self.deactivation_timeout = timeout;
        self
    }

    pub fn shutdown_timeout(mut self, timeout: Duration) -> Self {
        self.shutdown_timeout = timeout;
        self
    }

    pub fn register<A>(mut self, name: &'static str) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.registrations.push(__macro::Registration::of::<A>(name));
        self
    }

    pub fn register_with<A>(mut self, name: &'static str, config: ActorTypeConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        let mut registration = __macro::Registration::of::<A>(name);
        registration.mailbox_capacity = config.mailbox_capacity;
        registration.idle_timeout = config.idle_timeout;
        self.registrations.push(registration);
        self
    }

    #[cfg(test)]
    pub(crate) fn cluster_with_backend(
        self,
        config: ServerRuntimeConfig,
        storage: Arc<dyn OwnershipBackend>,
    ) -> Result<ServerStarter<S>, StartError> {
        debug_assert!(self.cluster.is_none());
        config.validate()?;
        Ok(ServerStarter {
            builder: self,
            config,
            storage,
        })
    }

    pub(crate) fn build_local(self) -> Result<Server<S>, StartError> {
        debug_assert!(self.cluster.is_none());
        self.build_with_authority(None, None)
    }    pub async fn start(mut self) -> Result<Server<S>, StartError> {
        let Some(cluster) = self.cluster.take() else {
            return self.build_local();
        };
        let (config, ownership) = cluster.into_parts();
        config.validate()?;
        let storage = Arc::new(S3OwnershipBackend::new(ownership));
        ServerStarter {
            builder: self,
            config,
            storage,
        }
        .start()
        .await
    }

    pub(crate) fn validate(&self) -> Result<(), StartError> {
        if self.mailbox_capacity == 0 {
            return Err(StartError::InvalidMailboxCapacity);
        }
        if self.max_active_actors == 0 {
            return Err(StartError::InvalidMaxActiveActors);
        }
        let mut names = std::collections::HashSet::new();
        for registration in &self.registrations {
            if registration.mailbox_capacity == Some(0) {
                return Err(StartError::InvalidMailboxCapacity);
            }
            if !names.insert(registration.name) {
                return Err(StartError::DuplicateActorType(registration.name));
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
    ) -> Result<Server<S>, StartError> {
        self.validate()?;
        let mut registrations = HashMap::new();
        for mut registration in self.registrations {
            if registration.mailbox_capacity.is_none() {
                registration.mailbox_capacity = Some(self.mailbox_capacity);
            }
            if registration.idle_timeout.is_none() {
                registration.idle_timeout = Some(self.idle_timeout);
            }
            let name = registration.name;
            let previous = registrations.insert(name, registration);
            debug_assert!(previous.is_none(), "registrations were validated");
        }
        Ok(Server {
            inner: Arc::new(__macro::ServerInner {
                state: Arc::new(self.state),
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
                relays: Mutex::new(HashMap::new()),
            }),
            cluster: None,
        })
    }
}

pub struct Server<S> {
    pub(crate) inner: Arc<__macro::ServerInner<S>>,
    cluster: Option<ClusterTasks>,
}

impl<S> Server<S>
where
    S: Send + Sync + 'static,
{
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
        termination: watch::Receiver<Option<ServerTermination>>,
    ) -> Self {
        self.cluster = Some(ClusterTasks {
            peer,
            renewal,
            termination,
        });
        self
    }

    pub fn supervision(&self) -> Option<ServerSupervision> {
        self.cluster.as_ref().map(|tasks| ServerSupervision {
            receiver: tasks.termination.clone(),
        })
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

#[cfg(test)]
pub(crate) mod testing {
    use super::ServerBuilder;

    #[allow(dead_code)]
    pub fn with_peer_protocol_version<S>(
        mut builder: ServerBuilder<S>,
        version: u32,
    ) -> ServerBuilder<S> {
        builder.peer_protocol_version = version;
        builder
    }
}
