#![allow(clippy::items_after_test_module)]

use std::{
    marker::PhantomData,
    sync::Arc,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use async_trait::async_trait;

use crate::client::Client;
use crate::runtime::ServerBuilderCore;
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::transport::{Endpoint, ServerTransport};
use crate::{
    __macro, ActorOwnerReader, IntoActorConfig, NodeDirectory, NodeRecord, NodeSessionId, Server,
    ServerError,
};

struct SingleNodeDirectory(NodeRecord);

#[async_trait]
impl NodeDirectory for SingleNodeDirectory {
    async fn read_node(
        &self,
        node_id: &str,
    ) -> Result<Option<NodeRecord>, crate::CoordinationError> {
        Ok((self.0.node_id == node_id).then(|| self.0.clone()))
    }
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, crate::CoordinationError> {
        Ok(vec![self.0.clone()])
    }
}

#[async_trait]
impl ActorOwnerReader for SingleNodeDirectory {
    async fn read_actor_owner(
        &self,
        _address: &crate::ActorAddress,
    ) -> Result<Option<crate::VersionedActorOwnerRecord>, crate::CoordinationError> {
        Ok(None)
    }
}

static NEXT_TEST_ENDPOINT: AtomicU64 = AtomicU64::new(0);

/// Builder for the in-memory [`TestServer`] Actor test harness.
pub struct TestServerBuilder<S = (), P = crate::runtime::MissingState> {
    core: ServerBuilderCore<S>,
    phase: PhantomData<P>,
}

/// In-memory Actor test harness with a caller [`Client`].
///
/// This is not a production local mode and does not use the production Coordination Store
/// or gRPC network transport.
pub struct TestServer<S = ()> {
    server: Server<S>,
    client: Client,
    accept_task: tokio::task::JoinHandle<()>,
}

impl<S> TestServer<S> {
    /// Creates a TestServer builder.
    pub fn builder() -> TestServerBuilder<S, crate::runtime::MissingState>
    where
        S: Send + Sync + 'static,
    {
        TestServerBuilder {
            core: ServerBuilderCore::base(None),
            phase: PhantomData,
        }
    }
}

impl<S, P> TestServerBuilder<S, P>
where
    S: Send + Sync + 'static,
{
    pub fn actor<A>(mut self, config: impl IntoActorConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.core.add_actor::<A>(config.into_actor_config());
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

    async fn start_with_state(mut self, state: S) -> Result<TestServer<S>, ServerError> {
        self.core = self.core.with_state(state);
        self.core.validate()?;
        let registry = InmemRegistry::new();
        let key = format!(
            "inmem://test-{}",
            NEXT_TEST_ENDPOINT.fetch_add(1, Ordering::Relaxed)
        );
        let server = self.core.build_with_authority(None, None)?;
        let inner = server.inner.clone();
        let server_transport = Arc::new(InmemTransport::new(registry.clone()));
        let mut listener = server_transport
            .listen(&Endpoint::new(key.clone()), None)
            .expect("in-memory listener");
        let accept_task = tokio::spawn(async move {
            while let Some(stream) = listener.accept().await {
                let inner = inner.clone();
                tokio::spawn(async move {
                    let mut stream = stream;
                    while let Some(envelope) = stream.recv().await {
                        inner
                            .dispatch_inbound(envelope, Some(stream.sender()))
                            .await;
                    }
                });
            }
        });
        let directory = Arc::new(SingleNodeDirectory(NodeRecord {
            node_id: "test".to_owned(),
            session_id: NodeSessionId::generate(),
            advertised_endpoint: key,
            protocol_version: crate::TRANSPORT_PROTOCOL_VERSION,
            lease_generation: 0,
            sampled_at_unix_ms: crate::cluster::wall_time_millis(),
            active_actor_count: 0,
            max_actor_count: usize::MAX,
            pressured: false,
            draining: false,
        }));
        let client = Client::with_transport(Arc::new(InmemTransport::new(registry)), directory);
        Ok(TestServer {
            server,
            client,
            accept_task,
        })
    }
}

impl<S> TestServerBuilder<S, crate::runtime::MissingState>
where
    S: Send + Sync + 'static,
{
    pub fn with_state(mut self, state: S) -> TestServerBuilder<S, crate::runtime::ReadyState> {
        self.core = self.core.with_state(state);
        TestServerBuilder {
            core: self.core,
            phase: PhantomData,
        }
    }
}

impl TestServerBuilder<(), crate::runtime::MissingState> {
    pub async fn start(self) -> Result<TestServer<()>, ServerError> {
        self.start_with_state(()).await
    }
}

impl<S> TestServerBuilder<S, crate::runtime::ReadyState>
where
    S: Send + Sync + 'static,
{
    pub async fn start(mut self) -> Result<TestServer<S>, ServerError> {
        let state = self
            .core
            .state
            .take()
            .expect("ready test builder has State");
        self.start_with_state(state).await
    }
}

impl<S: Send + Sync + 'static> TestServer<S> {
    /// Returns the Client connected to this test harness.
    pub fn client(&self) -> &Client {
        &self.client
    }
    /// Stops the test Server and its in-memory Client.
    pub async fn shutdown(self) {
        self.accept_task.abort();
        let _ = self.server.shutdown().await;
        self.client.shutdown().await;
    }
}

#[cfg(test)]
mod cluster_fakes {
    use crate::{
        ActorAddress, ActorOwnerReader, ActorOwnerRecord, ActorOwnerStore, CoordinationError,
        MutationOutcome, NodeDirectory, NodeLeaseStore, NodeRecord, Revision,
        VersionedActorOwnerRecord,
    };
    use async_trait::async_trait;
    use std::{
        collections::HashMap,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };

    struct TestNodeLease {
        node: NodeRecord,
        revision: Revision,
        expires_at_unix_ms: u64,
    }
    #[derive(Clone, Default)]
    pub(crate) struct TestCoordinationStore {
        leases: Arc<Mutex<HashMap<String, TestNodeLease>>>,
        owners: Arc<Mutex<HashMap<ActorAddress, VersionedActorOwnerRecord>>>,
        next_revision: Arc<Mutex<u64>>,
        panic_on_renewal: Arc<AtomicBool>,
    }
    impl TestCoordinationStore {
        fn next(&self) -> Revision {
            let mut next = self.next_revision.lock().unwrap();
            *next += 1;
            Revision::new(format!("test-revision-{next}"))
        }

        pub(crate) fn expire_node(&self, node_id: &str) {
            if let Some(lease) = self.leases.lock().unwrap().get_mut(node_id) {
                lease.expires_at_unix_ms = 0;
            }
        }

        pub(crate) fn remove_node(&self, node_id: &str) {
            self.leases.lock().unwrap().remove(node_id);
        }

        pub(crate) fn panic_on_renewal(&self) {
            self.panic_on_renewal.store(true, Ordering::Release);
        }
    }
    #[async_trait]
    impl NodeDirectory for TestCoordinationStore {
        async fn read_node(&self, node_id: &str) -> Result<Option<NodeRecord>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            Ok(self
                .leases
                .lock()
                .unwrap()
                .get(node_id)
                .filter(|l| l.expires_at_unix_ms > now)
                .map(|l| l.node.clone()))
        }
        async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            Ok(self
                .leases
                .lock()
                .unwrap()
                .values()
                .filter(|l| l.expires_at_unix_ms > now)
                .map(|l| l.node.clone())
                .collect())
        }
    }
    #[async_trait]
    impl NodeLeaseStore for TestCoordinationStore {
        async fn read_node_lease(
            &self,
            node_id: &str,
        ) -> Result<Option<(NodeRecord, Revision)>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            Ok(self
                .leases
                .lock()
                .unwrap()
                .get(node_id)
                .filter(|l| l.expires_at_unix_ms > now)
                .map(|l| (l.node.clone(), l.revision.clone())))
        }
        async fn acquire_node(
            &self,
            node: NodeRecord,
            ttl: Duration,
        ) -> Result<MutationOutcome<Revision>, CoordinationError> {
            let mut leases = self.leases.lock().unwrap();
            if leases
                .get(&node.node_id)
                .is_some_and(|l| l.expires_at_unix_ms > crate::cluster::wall_time_millis())
            {
                return Ok(MutationOutcome::Conflict);
            }
            let revision = self.next();
            leases.insert(
                node.node_id.clone(),
                TestNodeLease {
                    node,
                    revision: revision.clone(),
                    expires_at_unix_ms: crate::cluster::wall_time_millis() + ttl.as_millis() as u64,
                },
            );
            Ok(MutationOutcome::Applied(revision))
        }
        async fn renew_node(
            &self,
            node: NodeRecord,
            ttl: Duration,
            revision: &Revision,
        ) -> Result<MutationOutcome<Revision>, CoordinationError> {
            assert!(
                !self.panic_on_renewal.load(Ordering::Acquire),
                "injected renewal task failure"
            );
            let mut leases = self.leases.lock().unwrap();
            if !leases
                .get(&node.node_id)
                .is_some_and(|l| l.revision == *revision)
            {
                return Ok(MutationOutcome::Conflict);
            }
            let next = self.next();
            leases.insert(
                node.node_id.clone(),
                TestNodeLease {
                    node,
                    revision: next.clone(),
                    expires_at_unix_ms: crate::cluster::wall_time_millis() + ttl.as_millis() as u64,
                },
            );
            Ok(MutationOutcome::Applied(next))
        }
        async fn release_node(
            &self,
            node_id: &str,
            revision: &Revision,
        ) -> Result<MutationOutcome<()>, CoordinationError> {
            let mut leases = self.leases.lock().unwrap();
            if !leases.get(node_id).is_some_and(|l| l.revision == *revision) {
                return Ok(MutationOutcome::Conflict);
            }
            leases.remove(node_id);
            Ok(MutationOutcome::Applied(()))
        }
    }
    #[async_trait]
    impl ActorOwnerReader for TestCoordinationStore {
        async fn read_actor_owner(
            &self,
            address: &ActorAddress,
        ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
            Ok(self.owners.lock().unwrap().get(address).cloned())
        }
    }
    #[async_trait]
    impl ActorOwnerStore for TestCoordinationStore {
        async fn compare_exchange_actor_owner(
            &self,
            address: &ActorAddress,
            record: ActorOwnerRecord,
            revision: Option<&Revision>,
        ) -> Result<MutationOutcome<Revision>, CoordinationError> {
            let mut owners = self.owners.lock().unwrap();
            let matches = match (owners.get(address), revision) {
                (None, None) => true,
                (Some(current), Some(revision)) => current.revision == *revision,
                _ => false,
            };
            if !matches {
                return Ok(MutationOutcome::Conflict);
            }
            let next = self.next();
            owners.insert(
                address.clone(),
                VersionedActorOwnerRecord {
                    record,
                    revision: next.clone(),
                },
            );
            Ok(MutationOutcome::Applied(next))
        }
    }

    pub(crate) async fn start_cluster_inmem<S, T>(
        core: crate::ServerBuilderCore<S>,
        store: Arc<T>,
        node_id: &str,
        registry: Arc<crate::transport::inmem::InmemRegistry>,
    ) -> crate::Server<S>
    where
        S: Send + Sync + 'static,
        T: crate::coordination::CoordinationStore,
    {
        static NEXT_CLUSTER_ENDPOINT: std::sync::atomic::AtomicU64 =
            std::sync::atomic::AtomicU64::new(0);
        let endpoint = NEXT_CLUSTER_ENDPOINT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let config = crate::ServerRuntimeConfig::inmem(
            node_id,
            format!("node-{node_id}-{endpoint}"),
            registry,
        );
        crate::ServerStarter {
            builder: core,
            config,
            stores: crate::CoordinationStores::new(store),
        }
        .start()
        .await
        .unwrap()
    }
}

#[cfg(test)]
pub(crate) use cluster_fakes::{TestCoordinationStore, start_cluster_inmem};
