# Quick Start

CoActor is a Rust library embedded in the consumer's Tokio application. This guide walks through defining an Actor, calling its commands, and running a cluster node.

## Prerequisites

- Rust 1.85 or newer.
- CoActor runs on the consumer's Tokio runtime: it spawns Actor tasks but never creates a hidden executor or thread. Use `tokio` with the `rt-multi-thread` (or `rt`) and `macros` features in your application.

## Add the dependency

CoActor is not yet published to crates.io; depend on it by path in a workspace:

```toml
[dependencies]
coactor = { path = "coactor" }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## Define an Actor

An Actor is a plain Rust type with an inherent impl annotated by `#[actor(name = "...")]`. The name is the stable Actor Type — it is never derived from the Rust type name. Mark each caller-visible method with `#[command]`:

```rust
use std::sync::Arc;

use coactor::{ActorId, CommandContext, RuntimeBuilder, actor};

struct CounterActor(i64);

#[actor(name = "counter")]
impl CounterActor {
    pub fn new(_actor_id: ActorId, _app_state: Arc<()>) -> Self {
        Self(0)
    }

    #[coactor::command]
    pub async fn add(&mut self, _context: &CommandContext, amount: i64) -> i64 {
        self.0 += amount;
        self.0
    }
}
```

Requirements on the actor impl:

- a constructor `pub fn new(actor_id: ActorId, app_state: Arc<AppState>) -> Self` — the App State type is inferred from its `Arc` argument;
- every caller-visible method is a `pub async fn` marked `#[command]`, taking `&CommandContext` as its first argument;
- optional lifecycle hooks `on_activate(&mut self)` and `on_deactivate(&mut self, reason)`.

## Start a runtime and call the Actor

```rust
#[tokio::main]
async fn main() {
    let runtime = RuntimeBuilder::local(())
        .register::<CounterActor>()
        .start()
        .await
        .unwrap();

    let counter = runtime
        .actor_ref::<CounterActor>(ActorId::from("room-7"))
        .unwrap();

    assert_eq!(counter.add(2).await.unwrap(), 2);
    runtime.shutdown().await;
}
```

Run the complete example from this repository:

```console
cargo run -p coactor --example counter
```

## The actor model in four concepts

| Concept | Meaning |
|---|---|
| **Actor Type** | The stable business name from `#[actor(name = "...")]`, unique per runtime and registered before startup. |
| **Actor ID** | A caller-provided byte identity, unique within one Actor Type. |
| **Actor Address** | `Actor Type + Actor ID`; the stable identity used for routing. |
| **Actor Ref** | The generated typed handle (e.g. `CounterActorRef`). It weakly references the runtime: cloning or holding it neither activates the Actor nor keeps the runtime alive. Commands are inherent async methods on the Ref. |

The first accepted command lazily activates the addressed Actor. Each Active Actor owns one bounded mailbox and executes one command at a time, including across `.await` points. After idle passivation, a process exit, or a crash, the Actor starts again from empty state.

## Configure the runtime

`RuntimeBuilder` applies runtime-wide defaults; `ActorTypeConfig` overrides them per Actor Type:

| Builder method | Default | Meaning |
|---|---|---|
| `mailbox_capacity` | 32 | per-Actor mailbox size; a full mailbox returns `MailboxFull` immediately |
| `max_active_actors` | 10_000 | simultaneous Active Actor limit; excess activations return `RuntimeAtCapacity` |
| `idle_timeout` | 60s | inactivity before idle passivation |
| `deactivation_timeout` | 5s | deadline for the `on_deactivate` lifecycle hook |
| `shutdown_timeout` | 30s | global deadline for draining and deactivating on shutdown |

```rust
let runtime = RuntimeBuilder::local(state)
    .register_with::<CounterActor>(ActorTypeConfig::new().mailbox_capacity(128))
    .start()
    .await?;
```

## Run a cluster node

Cluster mode adds location-transparent routing: an S3-backed ownership authority decides which node owns each Actor Address, and remote-enabled commands are forwarded over gRPC.

```rust,no_run
use coactor::{
    RuntimeBuilder,
    cluster::{ClusterConfig, S3OwnershipConfig},
};

# async fn start() -> Result<(), coactor::StartError> {
let ownership = S3OwnershipConfig::local(
    "coactor",
    "development",
    "http://127.0.0.1:9000",
);
let cluster = ClusterConfig::new(
    "node-a",
    "0.0.0.0:7000".parse().unwrap(),
    "127.0.0.1:7000".parse().unwrap(),
    ownership,
);
let runtime = RuntimeBuilder::cluster((), cluster).start().await?;
# runtime.shutdown().await;
# Ok(())
# }
```

For production, construct `S3OwnershipConfig` with the deployment's bucket, prefix, region, credentials provider, and request timeout. Each node needs a unique advertised address and a stable operational Node ID; every runtime start creates a distinct Node Session internally.

Commands that may cross nodes must use `#[coactor::command(remote)]`. A remote-enabled command takes exactly one request implementing `prost::Message + Default`; its success value and declared business error must satisfy the same wire decoding requirements. Plain `#[coactor::command]` methods remain local-only even when their Actor Type is registered in cluster mode.

## Semantics you should know

- **Handler Reply is not durable.** A successful return only means the handler completed in-process. It is not an acknowledgment that state was persisted, and passivation, panic, or process termination discards the Actor's in-memory state.
- **Always await `Runtime::shutdown`.** Dropping a runtime without graceful shutdown has no drain or cleanup guarantee. Shutdown stops new admissions, drains accepted commands, runs deactivation, and returns `RuntimeShuttingDown` for new calls.
- **Actor Refs are weak.** Retaining or cloning a Ref does not prevent passivation or keep the runtime alive; after the runtime stops, calls return `RuntimeStopped`.
