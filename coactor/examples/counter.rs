use std::time::Duration;

use coactor::{
    Actor, ActorId, ActorRuntime, MessageContext, actor, test_support::TestServerBuilder,
};

#[derive(Clone)]
struct AppState {
    increment_scale: i64,
}

#[actor]
struct CounterActor {
    runtime: ActorRuntime<AppState>,
    value: i64,
}

impl Actor<AppState> for CounterActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime, value: 0 }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        // 最小字节协议：前 8 字节 big-endian i64 为增量。
        let amount = i64::from_be_bytes(msg.try_into().expect("8-byte action"));
        self.value += amount * self.runtime.app_state().increment_scale;
        let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    // 本地测试/示例形态：TestServer 装配 inmem transport，client() 内存直连。
    // 生产形态为分布式 Server + Client（经 Node Directory 与 Gateway 中继，见 ADR-0010）。
    let server = TestServerBuilder::new(AppState { increment_scale: 2 })
        .mailbox_capacity(32)
        .idle_timeout(Duration::from_secs(60))
        .register::<CounterActor>("counter")
        .start()
        .await
        .expect("valid CoActor configuration");

    let counter = server
        .client()
        .actor("counter", ActorId::from("example-counter"));

    let mut session = counter.open().await.expect("Session opens");
    session
        .send(3i64.to_be_bytes().to_vec())
        .await
        .expect("Action accepted");
    let value = session.recv().await.expect("Session alive");
    println!(
        "add(3) = {}",
        i64::from_be_bytes(value.unwrap().try_into().unwrap())
    );

    session
        .send(4i64.to_be_bytes().to_vec())
        .await
        .expect("Action accepted");
    let value = session.recv().await.expect("Session alive");
    println!(
        "add(4) = {}",
        i64::from_be_bytes(value.unwrap().try_into().unwrap())
    );

    server.shutdown().await;
}
