use std::time::Duration;

use coactor::{ActorContext, ActorId, RuntimeBuilder, actor};

#[derive(Clone)]
struct AppState {
    increment_scale: i64,
}

struct CounterActor {
    value: i64,
}

#[actor(name = "counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId) -> Self {
        Self { value: 0 }
    }

    #[coactor::command]
    pub async fn add(&mut self, context: &ActorContext<'_, AppState>, amount: i64) -> i64 {
        self.value += amount * context.state().increment_scale;
        self.value
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let runtime = RuntimeBuilder::new(AppState { increment_scale: 2 })
        .mailbox_capacity(32)
        .max_active_actors(1_000)
        .idle_timeout(Duration::from_secs(60))
        .register::<CounterActor>()
        .build()
        .expect("valid CoActor configuration");

    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("example-counter"))
        .expect("CounterActor was registered");

    println!(
        "{}",
        counter.add(3).await.expect("Counter command succeeds")
    );
    println!(
        "{}",
        counter.add(4).await.expect("Counter command succeeds")
    );

    runtime.shutdown().await;
}
