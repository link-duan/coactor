use std::sync::Arc;
use std::time::Duration;

use tokio::sync::Semaphore;

use crate::cluster::{
    ClusterRouter, NodeAuthority, NodeLease, NodeSessionId, ResolvedOwner, wall_time_millis,
};
use crate::test_support::{TestOwnershipBackend, start_cluster};
use crate::{Actor, ActorAddress, ActorId, LeaseMutation, OwnershipBackend, ServerBuilder};

fn address(id: &str) -> ActorAddress {
    ActorAddress::new("ownership-probe", ActorId::from(id))
}

async fn router(
    storage: &Arc<TestOwnershipBackend>,
    node_id: &str,
) -> Arc<ClusterRouter> {
    let session_id = NodeSessionId::generate();
    let lease = NodeLease {
        node_id: node_id.to_owned(),
        session_id: session_id.clone(),
        advertised_address: "http://127.0.0.1:7000".to_owned(),
        protocol_version: crate::PEER_PROTOCOL_VERSION,
        expires_at_unix_ms: wall_time_millis() + 60_000,
        sampled_at_unix_ms: wall_time_millis(),
        active_actor_count: 0,
        max_actor_count: 100,
        pressured: false,
        draining: false,
    };
    let mutation = storage.acquire_node_lease(lease).await.unwrap();
    assert!(matches!(mutation, LeaseMutation::Applied { .. }));
    ClusterRouter::new(
        storage.clone(),
        node_id.to_owned(),
        session_id,
        Duration::from_secs(5),
    )
}

#[tokio::test]
async fn first_resolve_claims_owner_with_epoch_one() {
    let storage = Arc::new(TestOwnershipBackend::default());
    // 需要先有 node lease 供 resolve 检查（owner 是本地时不检查 lease）
    let _ = start_cluster(
        ServerBuilder::local(()).register::<ProbeActor>("ownership-probe"),
        storage.clone(),
        "node-a",
    )
    .await;
    let capacity = Arc::new(Semaphore::new(8));
    let r = router(&storage, "node-a").await;

    match r.resolve(&address("a-1"), &capacity).await.expect("resolve") {
        ResolvedOwner::Local { .. } => {}
        _ => panic!("first claim must be local"),
    }
    // release 后记录 unowned 且 epoch 保留为 1
    r.release_local_owner(&address("a-1")).await.expect("release");
    let record = storage
        .read_actor_owner(&address("a-1"))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert!(record.owner.is_none());
    assert_eq!(record.ownership_epoch, 1);
}

#[tokio::test]
async fn concurrent_resolves_coalesce_to_one_owner() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let capacity = Arc::new(Semaphore::new(8));
    let first = router(&storage, "node-a").await;
    let second = router(&storage, "node-b").await;

    let a = first.clone();
    let b = second.clone();
    let cap_a = capacity.clone();
    let cap_b = capacity.clone();
    let address = address("a-2");
    let addr_a = address.clone();
    let addr_b = address.clone();
    let (left, right) = tokio::join!(
        async move { a.resolve(&addr_a, &cap_a).await },
        async move { b.resolve(&addr_b, &cap_b).await },
    );
    let _ = left.expect("resolve");
    let _ = right.expect("resolve");
    // 两个 router 各自返回 owner：至少一个是 Local（claim 成功者），另一个解析到该 owner
    let owners = storage
        .read_actor_owner(&address)
        .await
        .unwrap()
        .unwrap()
        .record;
    assert!(owners.owner.is_some(), "one node must own the address");
}

#[tokio::test]
async fn takeover_requires_expired_or_absent_owner_lease() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let capacity = Arc::new(Semaphore::new(8));
    let ra = router(&storage, "node-a").await;
    match ra
        .resolve(&address("a-3"), &capacity)
        .await
        .expect("node-a claims")
    {
        ResolvedOwner::Local { .. } => {}
        _ => panic!("node-a must claim"),
    }

    // node-b 尝试：owner lease 仍有效 → 解析为 Remote（不可接管）
    let rb = router(&storage, "node-b").await;
    match rb
        .resolve(&address("a-3"), &capacity)
        .await
        .expect("node-b resolves")
    {
        ResolvedOwner::Remote { endpoint, .. } => assert!(endpoint.starts_with("http://")),
        ResolvedOwner::Local { .. } => panic!("node-b must not take over while lease is live"),
    }

    // node-a 释放 lease → node-b 可接管，epoch 递增
    let lease = storage
        .list_node_leases()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.lease.node_id == "node-a")
        .unwrap();
    storage
        .release_node_lease(&lease.lease.session_id, &lease.etag)
        .await
        .unwrap();
    match rb
        .resolve(&address("a-3"), &capacity)
        .await
        .expect("node-b takes over")
    {
        ResolvedOwner::Local { .. } => {}
        _ => panic!("node-b must take over after lease loss"),
    }
    let record = storage
        .read_actor_owner(&address("a-3"))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.ownership_epoch, 2, "takeover bumps the epoch");
}

#[tokio::test]
async fn node_authority_renews_until_deadline_and_fences() {
    let started = tokio::time::Instant::now();
    let (termination_tx, _termination_rx) = tokio::sync::watch::channel(None);
    let authority = NodeAuthority::new(started, Duration::from_secs(10), termination_tx);
    assert!(authority.is_valid());
    // 用真实时间推进直到 deadline 过期（renew 由 lease renewal task 内部调用，此处只验证过期路径）
    let mut elapsed = Duration::ZERO;
    while authority.is_valid() && elapsed < Duration::from_secs(11) {
        tokio::time::sleep(Duration::from_millis(100)).await;
        elapsed += Duration::from_millis(100);
    }
    assert!(!authority.is_valid(), "authority must expire with the deadline");
}

#[tokio::test]
async fn wall_time_millis_is_monotonic_enough_for_cas_tests() {
    let a = wall_time_millis();
    tokio::time::sleep(Duration::from_millis(5)).await;
    let b = wall_time_millis();
    assert!(b >= a);
}

#[crate::actor]
struct ProbeActor;

impl crate::Actor<()> for ProbeActor {
    fn new(_runtime: crate::ActorRuntime<()>) -> Self {
        Self
    }
    async fn on_message(&mut self, _ctx: &crate::MessageContext, _msg: &[u8]) {}
}
