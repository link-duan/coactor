use std::{sync::Arc, time::Duration};

use coactor::{ActorId, CommandContext, RuntimeBuilder, actor};

#[derive(Clone)]
struct AppState {
    increment_scale: i64,
}

struct CounterActor {
    state: Arc<AppState>,
    value: i64,
}

#[actor(name = "counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId, state: Arc<AppState>) -> Self {
        Self { state, value: 0 }
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &CommandContext, amount: i64) -> i64 {
        self.value += amount * self.state.increment_scale;
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
