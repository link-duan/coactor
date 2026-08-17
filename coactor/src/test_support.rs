#![allow(clippy::items_after_test_module)]

#[cfg(test)]
mod cluster_fakes {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use async_trait::async_trait;

    use crate::{
        ActorAddress, ActorOwnerRecord, ActorOwnerStore, CoordinationError, LeaseMutation,
        LeaseToken, Mutation, NodeDirectory, NodeLeaseStore, NodeRecord, NodeSessionId, Revision,
        Server, ServerBuilder, ServerRuntimeConfig, VersionedActorOwnerRecord,
    };

    struct TestNodeLease {
        node: NodeRecord,
        token: LeaseToken,
        expires_at_unix_ms: u64,
    }

    #[derive(Default)]
    pub(crate) struct TestCoordinationStore {
        leases: Mutex<HashMap<NodeSessionId, TestNodeLease>>,
        owners: Mutex<HashMap<ActorAddress, VersionedActorOwnerRecord>>,
        next_revision: Mutex<u64>,
    }

    impl TestCoordinationStore {
        fn next_value(&self) -> String {
            let mut next = self.next_revision.lock().unwrap();
            *next += 1;
            format!("test-revision-{next}")
        }
    }

    #[async_trait]
    impl NodeDirectory for TestCoordinationStore {
        async fn read_node(
            &self,
            session_id: &NodeSessionId,
        ) -> Result<Option<NodeRecord>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            Ok(self
                .leases
                .lock()
                .unwrap()
                .get(session_id)
                .filter(|lease| lease.expires_at_unix_ms > now)
                .map(|lease| lease.node.clone()))
        }

        async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            let mut nodes: Vec<_> = self
                .leases
                .lock()
                .unwrap()
                .values()
                .filter(|lease| lease.expires_at_unix_ms > now)
                .map(|lease| lease.node.clone())
                .collect();
            nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
            Ok(nodes)
        }
    }

    #[async_trait]
    impl NodeLeaseStore for TestCoordinationStore {
        async fn read_node_lease(
            &self,
            session_id: &NodeSessionId,
        ) -> Result<Option<(NodeRecord, LeaseToken)>, CoordinationError> {
            let now = crate::cluster::wall_time_millis();
            Ok(self
                .leases
                .lock()
                .unwrap()
                .get(session_id)
                .filter(|lease| lease.expires_at_unix_ms > now)
                .map(|lease| (lease.node.clone(), lease.token.clone())))
        }

        async fn acquire_node(
            &self,
            node: NodeRecord,
            ttl: Duration,
        ) -> Result<LeaseMutation, CoordinationError> {
            let mut leases = self.leases.lock().unwrap();
            if leases.contains_key(&node.session_id) {
                return Ok(LeaseMutation::Conflict);
            }
            let token = LeaseToken::new(self.next_value());
            leases.insert(
                node.session_id.clone(),
                TestNodeLease {
                    node,
                    token: token.clone(),
                    expires_at_unix_ms: crate::cluster::wall_time_millis()
                        .saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
                },
            );
            Ok(LeaseMutation::Applied { token })
        }

        async fn renew_node(
            &self,
            node: NodeRecord,
            ttl: Duration,
            token: &LeaseToken,
        ) -> Result<LeaseMutation, CoordinationError> {
            let mut leases = self.leases.lock().unwrap();
            if !leases
                .get(&node.session_id)
                .is_some_and(|entry| entry.token == *token)
            {
                return Ok(LeaseMutation::Conflict);
            }
            let next = LeaseToken::new(self.next_value());
            leases.insert(
                node.session_id.clone(),
                TestNodeLease {
                    node,
                    token: next.clone(),
                    expires_at_unix_ms: crate::cluster::wall_time_millis()
                        .saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
                },
            );
            Ok(LeaseMutation::Applied { token: next })
        }

        async fn release_node(
            &self,
            session_id: &NodeSessionId,
            token: &LeaseToken,
        ) -> Result<LeaseMutation, CoordinationError> {
            let mut leases = self.leases.lock().unwrap();
            if !leases
                .get(session_id)
                .is_some_and(|entry| entry.token == *token)
            {
                return Ok(LeaseMutation::Conflict);
            }
            leases.remove(session_id);
            Ok(LeaseMutation::Applied {
                token: token.clone(),
            })
        }
    }

    #[async_trait]
    impl ActorOwnerStore for TestCoordinationStore {
        async fn read_actor_owner(
            &self,
            address: &ActorAddress,
        ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
            Ok(self.owners.lock().unwrap().get(address).cloned())
        }

        async fn compare_exchange_actor_owner(
            &self,
            address: &ActorAddress,
            record: ActorOwnerRecord,
            revision: Option<&Revision>,
        ) -> Result<Mutation, CoordinationError> {
            let mut owners = self.owners.lock().unwrap();
            let matches = match (owners.get(address), revision) {
                (None, None) => true,
                (Some(current), Some(revision)) => current.revision == *revision,
                _ => false,
            };
            if !matches {
                return Ok(Mutation::Conflict);
            }
            let next = Revision::new(self.next_value());
            owners.insert(
                address.clone(),
                VersionedActorOwnerRecord {
                    record,
                    revision: next.clone(),
                },
            );
            Ok(Mutation::Applied { revision: next })
        }
    }

    pub(crate) async fn free_address() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    pub(crate) async fn start_cluster<S>(
        builder: ServerBuilder<S>,
        store: Arc<TestCoordinationStore>,
        node_id: &str,
    ) -> Server<S>
    where
        S: Send + Sync + 'static,
    {
        let address = free_address().await;
        builder
            .cluster_with_backend(ServerRuntimeConfig::new(node_id, address, address), store)
            .unwrap()
            .start()
            .await
            .unwrap()
    }

    pub(crate) async fn start_cluster_inmem<S, T>(
        builder: ServerBuilder<S>,
        store: Arc<T>,
        node_id: &str,
        registry: Arc<crate::transport::inmem::InmemRegistry>,
    ) -> Server<S>
    where
        S: Send + Sync + 'static,
        T: NodeDirectory + NodeLeaseStore + ActorOwnerStore,
    {
        builder
            .cluster_with_backend(
                ServerRuntimeConfig::inmem(node_id, format!("node-{node_id}"), registry),
                store,
            )
            .unwrap()
            .start()
            .await
            .unwrap()
    }
}

#[cfg(test)]
pub(crate) use cluster_fakes::{TestCoordinationStore, start_cluster, start_cluster_inmem};

// ---------------------------------------------------------------------------
// ---------------------------------------------------------------------------
// TestServer：装配 inmem transport 的本地测试 Server（ADR-0008）。
// 无 authority、无 lease、单节点自动宿主全部地址；`client()` 返回内存直连的 Client。
// ---------------------------------------------------------------------------

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::client::{Client, ClientBuilder};
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::transport::{Endpoint, ServerTransport};
use crate::{__macro, NodeDirectory, NodeRecord, NodeSessionId, Server, ServerBuilder, StartError};
use async_trait::async_trait;

struct SingleNodeDirectory(NodeRecord);

#[async_trait]
impl NodeDirectory for SingleNodeDirectory {
    async fn read_node(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, crate::CoordinationError> {
        Ok((self.0.session_id == *session_id).then(|| self.0.clone()))
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, crate::CoordinationError> {
        Ok(vec![self.0.clone()])
    }
}

static NEXT_TEST_ENDPOINT: AtomicU64 = AtomicU64::new(0);

pub struct TestServerBuilder<S> {
    builder: ServerBuilder<S>,
}

impl<S: Send + Sync + 'static> TestServerBuilder<S> {
    pub fn new(state: S) -> Self {
        Self {
            builder: ServerBuilder::local(state),
        }
    }

    pub fn register<A>(mut self, name: &'static str) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.builder = self.builder.register::<A>(name);
        self
    }

    pub fn register_with<A>(mut self, name: &'static str, config: crate::ActorTypeConfig) -> Self
    where
        A: __macro::ActorType<S>,
    {
        self.builder = self.builder.register_with::<A>(name, config);
        self
    }

    pub fn mailbox_capacity(mut self, capacity: usize) -> Self {
        self.builder = self.builder.mailbox_capacity(capacity);
        self
    }

    pub fn idle_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.builder = self.builder.idle_timeout(timeout);
        self
    }

    pub async fn start(self) -> Result<TestServer<S>, StartError> {
        let registry = InmemRegistry::new();
        let key = format!(
            "inmem://test-{}",
            NEXT_TEST_ENDPOINT.fetch_add(1, Ordering::Relaxed)
        );
        let server = self.builder.build_local()?;
        let inner = server.inner.clone();
        let server_transport = Arc::new(InmemTransport::new(registry.clone()));
        let mut listener = server_transport
            .listen(&Endpoint::new(key.clone()), None)
            .expect("inmem listen");
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
        let client_transport = Arc::new(InmemTransport::new(registry));
        let directory = Arc::new(SingleNodeDirectory(NodeRecord {
            node_id: "test".to_owned(),
            session_id: NodeSessionId::generate(),
            advertised_address: key,
            protocol_version: crate::PEER_PROTOCOL_VERSION,
            lease_generation: 0,
            sampled_at_unix_ms: crate::cluster::wall_time_millis(),
            active_actor_count: 0,
            max_actor_count: usize::MAX,
            pressured: false,
            draining: false,
        }));
        let client = ClientBuilder::with_transport(client_transport, directory).start();
        Ok(TestServer {
            server,
            client,
            accept_task,
        })
    }
}

pub struct TestServer<S> {
    server: Server<S>,
    client: Client,
    accept_task: tokio::task::JoinHandle<()>,
}

impl<S> TestServer<S>
where
    S: Send + Sync + 'static,
{
    /// 直接驱动测试的 Client（经 inmem transport 连接本 Server）。
    pub fn client(&self) -> &Client {
        &self.client
    }

    pub async fn shutdown(self) {
        self.accept_task.abort();
        self.server.shutdown().await;
        self.client.shutdown().await;
    }
}
