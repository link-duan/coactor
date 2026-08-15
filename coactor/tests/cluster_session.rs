use std::sync::Arc;
use std::time::Duration;

use crate::client::{Client, ClientBuilder, RouteMode};
use crate::test_support::{TestOwnershipBackend, start_cluster_inmem};
use crate::transport::inmem::{InmemRegistry, InmemTransport};
use crate::transport::Endpoint;
use crate::{
    Actor, ActorId, ActorRuntime, MessageContext, Server, ServerBuilder, actor,
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

async fn inmem_server(
    storage: Arc<TestOwnershipBackend>,
    node_id: &str,
    registry: Arc<InmemRegistry>,
) -> Server<AppState> {
    start_cluster_inmem(
        ServerBuilder::local(AppState).register::<EchoActor>("cluster-echo"),
        storage,
        node_id,
        registry,
    )
    .await
}

fn inmem_client(registry: Arc<InmemRegistry>, endpoint: &str) -> Client {
    ClientBuilder::new(
        Arc::new(InmemTransport::new(registry)),
        RouteMode::Direct(Endpoint::new(endpoint)),
    )
    .start()
}

/// 网关模型：client 直连网关节点，网关就地 claim 或转发到 owner。
#[tokio::test]
async fn sessions_are_location_transparent_across_gateways() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_a = inmem_server(storage.clone(), "A", registry.clone()).await;
    let client_a = inmem_client(registry.clone(), "node-A");

    // 首次 open：A 就地 claim + 宿主
    let mut session_a = client_a
        .actor("cluster-echo", ActorId::from("remote-1"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session_a.recv().await, Some(Ok(b"opened".to_vec())));
    session_a.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session_a.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    // 第二个网关节点 B：会话经 B 转发到 owner A（跨节点中继）
    let server_b = inmem_server(storage.clone(), "B", registry.clone()).await;
    let client_b = inmem_client(registry.clone(), "node-B");
    let mut session_b = client_b
        .actor("cluster-echo", ActorId::from("remote-1"))
        .open()
        .await
        .expect("open via B");
    assert_eq!(session_b.recv().await, Some(Ok(b"opened".to_vec())));
    session_b.send(b"ping".to_vec()).await.expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), session_b.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    client_b.shutdown().await;
    server_b.shutdown().await;
    client_a.shutdown().await;
    server_a.shutdown().await;
}

/// failover：owner 失效后新网关接管；旧会话发送失败、caller 重开。
#[tokio::test]
async fn failover_breaks_sessions_and_requires_reopen() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let registry = InmemRegistry::new();
    let server_a = inmem_server(storage.clone(), "A", registry.clone()).await;
    let client_a = inmem_client(registry.clone(), "node-A");

    let mut session = client_a
        .actor("cluster-echo", ActorId::from("remote-2"))
        .open()
        .await
        .expect("open via A");
    assert_eq!(session.recv().await, Some(Ok(b"opened".to_vec())));

    // owner A 下线（释放 lease + 中止 accept）
    server_a.shutdown().await;
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 新网关 B 接管：claim（epoch 递增）+ 宿主
    let server_b = inmem_server(storage, "B", registry.clone()).await;
    let client_b = inmem_client(registry.clone(), "node-B");
    let mut fresh = client_b
        .actor("cluster-echo", ActorId::from("remote-2"))
        .open()
        .await
        .expect("reopen via B");
    assert_eq!(fresh.recv().await, Some(Ok(b"opened".to_vec())));
    fresh
        .send(b"after-failover".to_vec())
        .await
        .expect("accepted");
    assert_eq!(
        fresh.recv().await,
        Some(Ok(b"after-failover".to_vec()))
    );

    // 旧 session 的后续发送失败（A 已死，连接重建失败）
    let send_result = tokio::time::timeout(
        Duration::from_secs(5),
        session.send(b"stale".to_vec()),
    )
    .await;
    match send_result {
        Ok(Err(_)) => {}
        Ok(Ok(())) => panic!("stale session send unexpectedly succeeded"),
        Err(_) => panic!("stale session send hung"),
    }

    client_b.shutdown().await;
    server_b.shutdown().await;
    client_a.shutdown().await;
}
