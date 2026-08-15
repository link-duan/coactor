use std::{collections::HashMap, sync::Arc, time::Duration};

use parking_lot::Mutex;
use tokio::sync::{Semaphore, watch};

use crate::cluster::{
    ClusterRouter, ClusterTasks, NodeAuthority, PeerTask, RenewalTask, spawn_peer,
};
use crate::{
    __macro, ActorAddress, ActorId, ActorRefError, ActorTypeConfig, ClusterConfig, ClusterStarter,
    PEER_PROTOCOL_VERSION, RuntimeSupervision, RuntimeTermination, S3OwnershipBackend, StartError,
};
#[cfg(test)]
use crate::{ClusterRuntimeConfig, OwnershipBackend};
use crate::runtime::core::ActorRef;
use crate::runtime::session::SessionRegistry;

pub struct RuntimeBuilder<S> {
    state: S,
    registrations: Vec<__macro::Registration<S>>,
    cluster: Option<ClusterConfig>,
    mailbox_capacity: usize,
    max_active_actors: usize,
    idle_timeout: Duration,
    deactivation_timeout: Duration,
    shutdown_timeout: Duration,
    peer_protocol_version: u32,
}

impl<S> RuntimeBuilder<S>
where
    S: Send + Sync + 'static,
{
    fn base(state: S) -> Self {
        Self {
            state,
            registrations: Vec::new(),
            cluster: None,
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

    pub fn cluster(state: S, config: ClusterConfig) -> Self {
        let mut builder = Self::base(state);
        builder.cluster = Some(config);
        builder
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.mailbox_capacity = capacity;
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

    pub fn register<A>(mut self) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.registrations.push(__macro::Registration::of::<A>());
        self
    }

    pub fn register_with<A>(mut self, config: ActorTypeConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        let mut registration = __macro::Registration::of::<A>();
        registration.mailbox_capacity = config.mailbox_capacity;
        registration.idle_timeout = config.idle_timeout;
        self.registrations.push(registration);
        self
    }

    #[cfg(test)]
    pub(crate) fn cluster_with_backend(
        self,
        config: ClusterRuntimeConfig,
        storage: Arc<dyn OwnershipBackend>,
    ) -> Result<ClusterStarter<S>, StartError> {
        debug_assert!(self.cluster.is_none());
        config.validate()?;
        Ok(ClusterStarter {
            builder: self,
            config,
            storage,
        })
    }

    fn build_local(self) -> Result<Runtime<S>, StartError> {
        debug_assert!(self.cluster.is_none());
        self.build_with_authority(None, None)
    }

    pub async fn start(mut self) -> Result<Runtime<S>, StartError> {
        let Some(cluster) = self.cluster.take() else {
            return self.build_local();
        };
        let (config, ownership) = cluster.into_parts();
        config.validate()?;
        let storage = Arc::new(S3OwnershipBackend::new(ownership));
        ClusterStarter {
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
    ) -> Result<Runtime<S>, StartError> {
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
        Ok(Runtime {
            inner: Arc::new(__macro::RuntimeInner {
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
            }),
            cluster: None,
        })
    }
}

pub struct Runtime<S> {
    pub(crate) inner: Arc<__macro::RuntimeInner<S>>,
    cluster: Option<ClusterTasks>,
}

impl<S> Runtime<S>
where
    S: Send + Sync + 'static,
{
    /// 按 Actor Type 名字 + Actor ID 获取通用地址句柄；未注册名字立即报错。
    pub fn actor(&self, actor_type: &str, actor_id: ActorId) -> Result<ActorRef<S>, ActorRefError> {
        if !self.inner.registrations.contains_key(actor_type) {
            return Err(ActorRefError::ActorTypeNotRegistered(actor_type.to_owned()));
        }
        Ok(ActorRef {
            runtime: Arc::downgrade(&self.inner),
            address: ActorAddress::new(actor_type, actor_id),
        })
    }

    pub(crate) fn spawn_peer(&self, listener: tokio::net::TcpListener) -> PeerTask {
        spawn_peer(self.inner.clone(), listener)
    }

    pub(crate) fn with_cluster_tasks(
        mut self,
        peer: PeerTask,
        renewal: RenewalTask,
        termination: watch::Receiver<Option<RuntimeTermination>>,
    ) -> Self {
        self.cluster = Some(ClusterTasks {
            peer,
            renewal,
            termination,
        });
        self
    }

    pub fn supervision(&self) -> Option<RuntimeSupervision> {
        self.cluster.as_ref().map(|tasks| RuntimeSupervision {
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
    use super::RuntimeBuilder;

    #[allow(dead_code)]
    pub fn with_peer_protocol_version<S>(
        mut builder: RuntimeBuilder<S>,
        version: u32,
    ) -> RuntimeBuilder<S> {
        builder.peer_protocol_version = version;
        builder
    }
}
