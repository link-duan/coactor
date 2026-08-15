use std::time::Duration;

use coactor::{Actor, ActorId, ActorRuntime, MessageContext, RuntimeBuilder, actor};

#[derive(Clone)]
struct AppState {
    increment_scale: i64,
}

#[actor(name = "counter")]
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

    let runtime = RuntimeBuilder::local(AppState { increment_scale: 2 })
        .mailbox_capacity(32)
        .max_active_actors(1_000)
        .idle_timeout(Duration::from_secs(60))
        .register::<CounterActor>()
        .start()
        .await
        .expect("valid CoActor configuration");

    let counter = runtime
        .actor(CounterActor::ACTOR_NAME, ActorId::from("example-counter"))
        .expect("CounterActor was registered");

    let mut session = counter.open().await.expect("Session opens");
    session
        .send(3i64.to_be_bytes().to_vec())
        .await
        .expect("Action accepted");
    let value = session.recv().await.expect("Session alive");
    println!("add(3) = {}", i64::from_be_bytes(value.unwrap().try_into().unwrap()));

    session
        .send(4i64.to_be_bytes().to_vec())
        .await
        .expect("Action accepted");
    let value = session.recv().await.expect("Session alive");
    println!("add(4) = {}", i64::from_be_bytes(value.unwrap().try_into().unwrap()));

    runtime.shutdown().await;
}
