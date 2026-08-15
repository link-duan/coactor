#[cfg(test)]
mod cluster_fakes {
    use std::{
        collections::HashMap,
        net::SocketAddr,
        sync::{Arc, Mutex},
    };

    use async_trait::async_trait;

    use crate::{
        ActorAddress, ActorOwnerRecord, ServerRuntimeConfig, LeaseMutation, NodeLease,
        NodeSessionId, OwnershipBackend, OwnershipBackendError, Server, ServerBuilder,
        VersionedActorOwnerRecord, VersionedNodeLease,
    };

    #[derive(Default)]
    pub(crate) struct TestOwnershipBackend {
    leases: Mutex<HashMap<NodeSessionId, VersionedNodeLease>>,
    owners: Mutex<HashMap<ActorAddress, VersionedActorOwnerRecord>>,
    next_etag: Mutex<u64>,
}

impl TestOwnershipBackend {
    fn next_etag(&self) -> String {
        let mut next = self.next_etag.lock().unwrap();
        *next += 1;
        format!("test-etag-{next}")
    }

}

#[async_trait]
impl OwnershipBackend for TestOwnershipBackend {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let mut leases = self.leases.lock().unwrap();
        if leases.contains_key(&lease.session_id) {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let etag = self.next_etag();
        leases.insert(
            lease.session_id.clone(),
            VersionedNodeLease {
                lease,
                etag: etag.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag })
    }

    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipBackendError> {
        Ok(self.leases.lock().unwrap().get(session_id).cloned())
    }

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipBackendError> {
        Ok(self.leases.lock().unwrap().values().cloned().collect())
    }

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let mut leases = self.leases.lock().unwrap();
        if !leases
            .get(&lease.session_id)
            .is_some_and(|entry| entry.etag == etag)
        {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let next = self.next_etag();
        leases.insert(
            lease.session_id.clone(),
            VersionedNodeLease {
                lease,
                etag: next.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag: next })
    }

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let mut leases = self.leases.lock().unwrap();
        if !leases
            .get(session_id)
            .is_some_and(|entry| entry.etag == etag)
        {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        leases.remove(session_id);
        Ok(LeaseMutation::Applied {
            etag: etag.to_owned(),
        })
    }

    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, OwnershipBackendError> {
        Ok(self.owners.lock().unwrap().get(address).cloned())
    }

    async fn claim_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let mut owners = self.owners.lock().unwrap();
        let matches = match (owners.get(address), etag) {
            (None, None) => true,
            (Some(current), Some(etag)) => current.etag == etag,
            _ => false,
        };
        if !matches {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let next = self.next_etag();
        owners.insert(
            address.clone(),
            VersionedActorOwnerRecord {
                record,
                etag: next.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag: next })
    }
}

    pub(crate) async fn free_address() -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        listener.local_addr().unwrap()
    }

    pub(crate) async fn start_cluster<S>(
        builder: ServerBuilder<S>,
        storage: Arc<TestOwnershipBackend>,
        node_id: &str,
    ) -> Server<S>
    where
        S: Send + Sync + 'static,
    {
        let address = free_address().await;
        builder
            .cluster_with_backend(
                ServerRuntimeConfig::new(node_id, address, address),
                storage,
            )
            .unwrap()
            .start()
            .await
            .unwrap()
    }

    /// inmem 集群节点（无 socket）：advertised 为 `node-<id>` registry key。
    pub(crate) async fn start_cluster_inmem<S>(
        builder: ServerBuilder<S>,
        storage: Arc<dyn crate::OwnershipBackend>,
        node_id: &str,
        registry: Arc<crate::transport::inmem::InmemRegistry>,
    ) -> Server<S>
    where
        S: Send + Sync + 'static,
    {
        builder
            .cluster_with_backend(
                ServerRuntimeConfig::inmem(node_id, format!("node-{node_id}"), registry),
                storage,
            )
            .unwrap()
            .start()
            .await
            .unwrap()
    }
}

#[cfg(test)]
pub(crate) use cluster_fakes::{TestOwnershipBackend, start_cluster, start_cluster_inmem};

// ---------------------------------------------------------------------------
// TestServer：装配 inmem transport 的本地测试 Server（ADR-0008）。
// 无 authority、无 lease、单节点自动宿主全部地址；`client()` 返回内存直连的 Client。
// ---------------------------------------------------------------------------

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::client::discovery::StaticListDiscovery;
use crate::client::{Client, ClientBuilder};
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::transport::{Endpoint, ServerTransport};
use crate::{Server, ServerBuilder, StartError, __macro};

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
                        inner.dispatch_inbound(envelope, Some(stream.sender())).await;
                    }
                });
            }
        });
        let client_transport = Arc::new(InmemTransport::new(registry));
        let client = ClientBuilder::with_transport(
            client_transport,
            StaticListDiscovery::new(vec![Endpoint::new(key)]),
        )
        .start();
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
