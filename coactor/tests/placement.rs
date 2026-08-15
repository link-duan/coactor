use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::client::discovery::StaticListDiscovery;
use crate::client::{Client, ClientBuilder};
use crate::test_support::{TestOwnershipBackend, start_cluster_inmem};
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::transport::Endpoint;
use crate::{
    Actor, ActorAddress, ActorId, ActorOwner, ActorOwnerRecord, ActorRuntime, MessageContext,
    OwnershipBackend, PlacementCtx, PlacementStrategy, Server, ServerBuilder, SendError, actor,
};

#[derive(Default, Clone)]
struct AppState;

#[actor]
struct EchoActor {
    runtime: ActorRuntime<AppState>,
}

impl Actor<AppState> for EchoActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let _ = ctx.send(msg.to_vec()).await;
    }

    async fn on_session_opened(&mut self, ctx: &MessageContext) {
        let _ = ctx.send(b"opened".to_vec()).await;
    }
}

/// 测试策略：固定返回候选端点（不含本节点）。
struct FixedPlacement(Vec<Endpoint>);

#[async_trait]
impl PlacementStrategy for FixedPlacement {
    async fn candidates(&self, _address: &ActorAddress, _ctx: &PlacementCtx) -> Vec<Endpoint> {
        self.0.clone()
    }
}

/// 预置节点 lease 的负载快照（active/max/pressured），供默认 p2c 策略决策。
async fn preset_lease(
    storage: &Arc<dyn OwnershipBackend>,
    node_id: &str,
    active: usize,
    max: usize,
    pressured: bool,
) {
    let leases = storage.list_node_leases().await.unwrap();
    let entry = leases
        .iter()
        .find(|entry| entry.lease.node_id == node_id)
        .expect("node lease");
    let mut lease = entry.lease.clone();
    lease.active_actor_count = active;
    lease.max_actor_count = max;
    lease.pressured = pressured;
    let mutation = storage
        .renew_node_lease(lease, &entry.etag)
        .await
        .unwrap();
    assert!(matches!(mutation, crate::LeaseMutation::Applied { .. }));
}

async fn server_with_placement(
    storage: Arc<dyn OwnershipBackend>,
    node_id: &str,
    registry: Arc<InmemRegistry>,
    placement: Arc<dyn PlacementStrategy>,
) -> Server<AppState> {
    start_cluster_inmem(
        ServerBuilder::local(AppState)
            .with_placement_strategy(placement)
            .register::<EchoActor>("placement-echo"),
        storage,
        node_id,
        registry,
    )
    .await
}

fn inmem_client(registry: Arc<InmemRegistry>, endpoint: &str) -> Client {
    ClientBuilder::with_transport(
        Arc::new(InmemTransport::new(registry)),
        StaticListDiscovery::new(vec![Endpoint::new(endpoint)]),
    )
    .start()
}

/// 对目标地址的**首次**读取谎报"未拥有"，之后转交真实 backend——模拟
/// 网关读-认领窗口内被他人抢先的竞态（确定性触发 NotOwner 路径）。
struct FirstReadUnowned {
    inner: Arc<TestOwnershipBackend>,
    target: ActorAddress,
    lied: AtomicBool,
}

#[async_trait]
impl OwnershipBackend for FirstReadUnowned {
    async fn acquire_node_lease(&self, lease: crate::NodeLease) -> Result<crate::LeaseMutation, crate::OwnershipBackendError> {
        self.inner.acquire_node_lease(lease).await
    }
    async fn read_node_lease(&self, session_id: &crate::NodeSessionId) -> Result<Option<crate::VersionedNodeLease>, crate::OwnershipBackendError> {
        self.inner.read_node_lease(session_id).await
    }
    async fn list_node_leases(&self) -> Result<Vec<crate::VersionedNodeLease>, crate::OwnershipBackendError> {
        self.inner.list_node_leases().await
    }
    async fn renew_node_lease(&self, lease: crate::NodeLease, etag: &str) -> Result<crate::LeaseMutation, crate::OwnershipBackendError> {
        self.inner.renew_node_lease(lease, etag).await
    }
    async fn release_node_lease(&self, session_id: &crate::NodeSessionId, etag: &str) -> Result<crate::LeaseMutation, crate::OwnershipBackendError> {
        self.inner.release_node_lease(session_id, etag).await
    }
    async fn read_actor_owner(&self, address: &ActorAddress) -> Result<Option<crate::VersionedActorOwnerRecord>, crate::OwnershipBackendError> {
        if address == &self.target && !self.lied.swap(true, Ordering::SeqCst) {
            return Ok(None);
        }
        self.inner.read_actor_owner(address).await
    }
    async fn claim_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        etag: Option<&str>,
    ) -> Result<crate::LeaseMutation, crate::OwnershipBackendError> {
        self.inner.claim_actor_owner(address, record, etag).await
    }
}

/// 转发目标被抢先认领（NotOwner）→ 网关重解析并转发给真 owner，会话照常建立。
#[tokio::test]
async fn placement_forwards_to_actual_owner_when_claim_raced() {
    let inner = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        inner.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        inner.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let address = ActorAddress::new("placement-echo", ActorId::from("raced-1"));
    // C 先认领目标地址（竞态的另一方）
    let c_lease = inner
        .list_node_leases()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.lease.node_id == "C")
        .unwrap();
    inner
        .claim_actor_owner(
            &address,
            ActorOwnerRecord {
                owner: Some(ActorOwner {
                    node_id: "C".to_owned(),
                    session_id: c_lease.lease.session_id,
                }),
                ownership_epoch: 1,
            },
            None,
        )
        .await
        .unwrap();

    // 网关 A：首次读取谎报 Unowned → 放置到 B → B 发现 owner=C → NotOwner → 转发给 C
    let storage: Arc<dyn OwnershipBackend> = Arc::new(FirstReadUnowned {
        inner: inner.clone(),
        target: address.clone(),
        lied: AtomicBool::new(false),
    });
    let server_a = server_with_placement(
        storage,
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![Endpoint::new("node-B")])),
    )
    .await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("raced-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    session.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    // 最终 owner 仍是 C（真 owner），会话经 A→C 中继可用
    let record = inner
        .read_actor_owner(&address)
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "C");

    client.shutdown().await;
    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 候选不可达 → 剔除后继续试下一个；全部不可达 → 报错。
#[tokio::test]
async fn placement_skips_unreachable_candidates() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_a = server_with_placement(
        storage.clone(),
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![
            Endpoint::new("node-ghost"), // 未注册 → connect 失败
            Endpoint::new("node-B"),
        ])),
    )
    .await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("unreach-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("unreach-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "B");

    client.shutdown().await;
    server_a.shutdown().await;
    server_b.shutdown().await;
}

/// 全部候选不可达 → RuntimeAtCapacity。
#[tokio::test]
async fn placement_reports_capacity_exhausted_when_all_candidates_unreachable() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_a = server_with_placement(
        storage,
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![
            Endpoint::new("node-ghost-1"),
            Endpoint::new("node-ghost-2"),
        ])),
    )
    .await;

    let client = inmem_client(registry.clone(), "node-A");
    let err = match client
        .actor("placement-echo", ActorId::from("unreach-2"))
        .open()
        .await
    {
        Ok(_) => panic!("all-unreachable placement must fail"),
        Err(error) => error,
    };
    assert_eq!(err, SendError::RuntimeAtCapacity);

    client.shutdown().await;
    server_a.shutdown().await;
}

/// Client：池内某网关不可达 → open() 自动驱逐并换下一个（传输层重试）。
#[tokio::test]
async fn client_open_fails_over_to_other_gateway_when_one_is_down() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    // node-A 从未启动（endpoint 未注册）
    let client = ClientBuilder::with_transport(
        Arc::new(InmemTransport::new(registry.clone())),
        StaticListDiscovery::new(vec![Endpoint::new("node-A"), Endpoint::new("node-B")]),
    )
    .start();

    let mut session = client
        .actor("placement-echo", ActorId::from("failover-1"))
        .open()
        .await
        .expect("open succeeds after gateway failover");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    session.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("failover-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "B");

    client.shutdown().await;
    server_b.shutdown().await;
}

/// 默认策略（空候选）→ 网关就地认领；注入策略 → 会话转发到目标节点。
#[tokio::test]
async fn placement_forwards_unowned_actor_to_strategy_target() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_a = server_with_placement(
        storage.clone(),
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![Endpoint::new("node-B")])),
    )
    .await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("placed-1"))
        .open()
        .await
        .expect("open via gateway A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    session.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    // Actor 落在策略目标 B（而非网关 A）
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("placed-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "B");

    client.shutdown().await;
    server_a.shutdown().await;
    server_b.shutdown().await;
}

/// 目标满 → 试下一个候选。
#[tokio::test]
async fn placement_skips_full_candidate() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    // B 容量 1 且已占用；C 正常。B 注入空策略：busy 会话就地认领占用容量
    // （默认 p2c 会把 busy 转发走，无法占满 B）。
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState)
            .max_active_actors(1)
            .with_placement_strategy(Arc::new(FixedPlacement(Vec::new())))
            .register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let server_a = server_with_placement(
        storage.clone(),
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![
            Endpoint::new("node-B"),
            Endpoint::new("node-C"),
        ])),
    )
    .await;

    // 占用 B 的容量（一个会话经 B → B claim，permit 归 Route 持有）
    let busy_client = inmem_client(registry.clone(), "node-B");
    let mut busy = busy_client
        .actor("placement-echo", ActorId::from("busy"))
        .open()
        .await
        .expect("busy session");
    assert_eq!(busy.recv().await, Some(Ok(b"opened".to_vec())));

    // B 满 → 试下一个候选 C
    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("placed-2"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("placed-2"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "C", "skip full B, place on C");

    client.shutdown().await;
    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 全部候选满 → 调用方收到 `RuntimeAtCapacity`（不做自兜底）。
#[tokio::test]
async fn placement_reports_capacity_exhausted_when_all_full() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    // B、C 容量均为 1 且各有一个占用会话（注入空策略：占用会话就地认领）。
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState)
            .max_active_actors(1)
            .with_placement_strategy(Arc::new(FixedPlacement(Vec::new())))
            .register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState)
            .max_active_actors(1)
            .with_placement_strategy(Arc::new(FixedPlacement(Vec::new())))
            .register::<EchoActor>("placement-echo"),
        storage.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let server_a = server_with_placement(
        storage.clone(),
        "A",
        registry.clone(),
        Arc::new(FixedPlacement(vec![
            Endpoint::new("node-B"),
            Endpoint::new("node-C"),
        ])),
    )
    .await;

    let busy_b = inmem_client(registry.clone(), "node-B");
    let mut hold_b = busy_b
        .actor("placement-echo", ActorId::from("hold-b"))
        .open()
        .await
        .expect("hold B");
    assert_eq!(hold_b.recv().await, Some(Ok(b"opened".to_vec())));
    let busy_c = inmem_client(registry.clone(), "node-C");
    let mut hold_c = busy_c
        .actor("placement-echo", ActorId::from("hold-c"))
        .open()
        .await
        .expect("hold C");
    assert_eq!(hold_c.recv().await, Some(Ok(b"opened".to_vec())));

    let client = inmem_client(registry.clone(), "node-A");
    let err = match client.actor("placement-echo", ActorId::from("placed-3")).open().await {
        Ok(_) => panic!("placement exhausted must fail"),
        Err(error) => error,
    };
    assert_eq!(err, SendError::RuntimeAtCapacity);

    client.shutdown().await;
    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 默认策略（p2c）：预置 B 高负载（95/100）、C 空闲（0/10000）→ 新 Actor 放置到 C。
#[tokio::test]
async fn default_placement_picks_less_loaded_node() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let server_a = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "A",
        registry.clone(),
    )
    .await;
    preset_lease(&storage, "B", 95, 100, false).await;
    preset_lease(&storage, "C", 0, 10_000, false).await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("load-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("load-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "C", "低 Load Ratio 节点优先");

    client.shutdown().await;
    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 默认策略：pressured 节点被硬过滤 → 放置到正常节点。
#[tokio::test]
async fn default_placement_skips_pressured_node() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let server_a = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "A",
        registry.clone(),
    )
    .await;
    preset_lease(&storage, "B", 100, 100, true).await;
    preset_lease(&storage, "C", 0, 10_000, false).await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("pressure-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("pressure-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "C", "pressured 节点被过滤");

    client.shutdown().await;
    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 默认策略（p2c + in-flight 记账）：等负载并发放置（Placement Burst）分散到多节点。
#[tokio::test]
async fn default_placement_burst_spreads_across_nodes() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_b = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "B",
        registry.clone(),
    )
    .await;
    let server_c = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "C",
        registry.clone(),
    )
    .await;
    let server_a = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "A",
        registry.clone(),
    )
    .await;

    let mut tasks = Vec::new();
    for i in 0..20 {
        let registry = registry.clone();
        tasks.push(tokio::spawn(async move {
            let client = inmem_client(registry, "node-A");
            let actor_id = ActorId::from(format!("burst-{i}").as_str());
            let mut session = client
                .actor("placement-echo", actor_id)
                .open()
                .await
                .expect("open via A");
            session.recv().await;
        }));
    }
    for task in tasks {
        task.await.expect("burst task");
    }

    let mut owners_b = 0usize;
    let mut owners_c = 0usize;
    for i in 0..20 {
        let record = storage
            .read_actor_owner(&ActorAddress::new(
                "placement-echo",
                ActorId::from(format!("burst-{i}").as_str()),
            ))
            .await
            .unwrap()
            .unwrap()
            .record;
        match record.owner.as_ref().unwrap().node_id.as_str() {
            "B" => owners_b += 1,
            "C" => owners_c += 1,
            other => panic!("unexpected owner {other}"),
        }
    }
    assert!(owners_b >= 1 && owners_c >= 1, "burst 分散到 B({owners_b}) 与 C({owners_c})");

    server_a.shutdown().await;
    server_c.shutdown().await;
    server_b.shutdown().await;
}

/// 单节点集群：无其他可用节点 → 就地认领（默认行为与 LocalPlacement 一致）。
#[tokio::test]
async fn default_placement_claims_locally_when_alone() {
    let storage: Arc<dyn OwnershipBackend> = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_a = start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("placement-echo"),
        storage.clone(),
        "A",
        registry.clone(),
    )
    .await;

    let client = inmem_client(registry.clone(), "node-A");
    let mut session = client
        .actor("placement-echo", ActorId::from("solo-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));
    let record = storage
        .read_actor_owner(&ActorAddress::new(
            "placement-echo",
            ActorId::from("solo-1"),
        ))
        .await
        .unwrap()
        .unwrap()
        .record;
    assert_eq!(record.owner.as_ref().unwrap().node_id, "A", "单节点就地认领");

    client.shutdown().await;
    server_a.shutdown().await;
}
