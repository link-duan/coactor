use std::{sync::Arc, time::Duration};

use coactor::{Actor, ActorId, ActorRuntime, MessageContext, ServerBuilder, actor};

use crate::test_support::{TestOwnershipBackend, start_cluster};

#[derive(Clone, Default)]
struct AppState {}

#[actor]
struct EchoActor;

impl Actor<AppState> for EchoActor {
    fn new(_runtime: ActorRuntime<AppState>) -> Self {
        Self
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let _ = ctx.send(msg.to_vec()).await;
    }

    async fn on_session_opened(&mut self, ctx: &MessageContext) {
        let _ = ctx.send(b"opened".to_vec()).await;
    }
}

#[tokio::test]
async fn sessions_are_location_transparent_across_nodes() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let server = start_cluster(
        ServerBuilder::local(AppState::default()).register::<EchoActor>("cluster-echo"),
        storage.clone(),
        "server",
    )
    .await;
    // server 先建立 ownership
    let server_actor = server.actor("cluster-echo", ActorId::from("remote-1"));
    let mut server_session = server_actor.open().await.expect("server opens");
    assert_eq!(server_session.recv().await, Some(Ok(b"opened".to_vec())));

    // client 从另一节点 open 同一 address：远程激活 + 双向 echo
    let client = start_cluster(
        ServerBuilder::local(AppState::default()).register::<EchoActor>("cluster-echo"),
        storage,
        "client",
    )
    .await;
    let client_actor = client.actor("cluster-echo", ActorId::from("remote-1"));
    let mut client_session = client_actor.open().await.expect("client opens");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_session.recv()).await,
        Ok(Some(Ok(b"opened".to_vec())))
    );

    client_session
        .send(b"ping".to_vec())
        .await
        .expect("accepted");
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(5), client_session.recv()).await,
        Ok(Some(Ok(b"ping".to_vec())))
    );

    // server 侧本地 session 同样工作（owner 在 server）
    server_session
        .send(b"server-ping".to_vec())
        .await
        .expect("accepted");
    assert_eq!(
        server_session.recv().await,
        Some(Ok(b"server-ping".to_vec()))
    );

    client.shutdown().await;
    server.shutdown().await;
}

#[tokio::test]
async fn failover_breaks_sessions_and_requires_reopen() {
    let storage = Arc::new(TestOwnershipBackend::default());
    let server = start_cluster(
        ServerBuilder::local(AppState::default()).register::<EchoActor>("cluster-echo"),
        storage.clone(),
        "server",
    )
    .await;
    let server_actor = server.actor("cluster-echo", ActorId::from("remote-2"));
    let mut server_session = server_actor.open().await.expect("server opens");
    assert_eq!(server_session.recv().await, Some(Ok(b"opened".to_vec())));

    // client 打开同一 address（owner 在 server）
    let client = start_cluster(
        ServerBuilder::local(AppState::default()).register::<EchoActor>("cluster-echo"),
        storage.clone(),
        "client",
    )
    .await;
    let client_actor = client.actor("cluster-echo", ActorId::from("remote-2"));
    let mut client_session = client_actor.open().await.expect("client opens");
    assert_eq!(client_session.recv().await, Some(Ok(b"opened".to_vec())));

    // server 下线 → owner 失效；新节点接管后必须重新 open
    server.shutdown().await;
    // 等待 client 侧感知连接断开（failover 惰性检测的传播延迟）
    tokio::time::sleep(Duration::from_millis(200)).await;

    let replacement = start_cluster(
        ServerBuilder::local(AppState::default()).register::<EchoActor>("cluster-echo"),
        storage,
        "replacement",
    )
    .await;
    let replacement_actor = replacement.actor("cluster-echo", ActorId::from("remote-2"));
    let mut fresh = replacement_actor.open().await.expect("replacement opens");
    assert_eq!(fresh.recv().await, Some(Ok(b"opened".to_vec())));
    fresh
        .send(b"after-failover".to_vec())
        .await
        .expect("accepted");
    assert_eq!(
        fresh.recv().await,
        Some(Ok(b"after-failover".to_vec()))
    );

    // 旧 client session 的后续发送失败（session 已随旧 owner 消亡）
    let send_result = tokio::time::timeout(
        Duration::from_secs(5),
        client_session.send(b"stale".to_vec()),
    )
    .await;
    match send_result {
        Ok(Err(_)) => {}
        Ok(Ok(())) => panic!("stale session send unexpectedly succeeded"),
        Err(_) => panic!("stale session send hung"),
    }

    replacement.shutdown().await;
    client.shutdown().await;
}
