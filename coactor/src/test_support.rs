use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;

use crate::{
    ActorAddress, ActorOwnerRecord, ClusterRuntimeConfig, LeaseMutation, NodeLease, NodeSessionId,
    OwnershipBackend, OwnershipBackendError, Runtime, RuntimeBuilder, VersionedActorOwnerRecord,
    VersionedNodeLease,
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
    builder: RuntimeBuilder<S>,
    storage: Arc<TestOwnershipBackend>,
    node_id: &str,
) -> Runtime<S>
where
    S: Send + Sync + 'static,
{
    let address = free_address().await;
    builder
        .cluster_with_backend(
            ClusterRuntimeConfig::new(node_id, address, address),
            storage,
        )
        .unwrap()
        .start()
        .await
        .unwrap()
}
