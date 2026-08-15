# Quick Start

CoActor is a Rust library embedded in the consumer's Tokio application. This guide walks through defining an Actor, opening a Session, exchanging bidirectional messages, and running a cluster node.

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

`#[actor]` marks the Actor **struct** and generates the type-erased dispatch impl; the stable Actor Type name is passed explicitly at registration (`register::<A>(name)`) and is never derived from the Rust type name. Business logic lives in a hand-written `impl Actor<AppState>` using native `async fn`:

```rust
use coactor::{Actor, ActorId, ActorRuntime, MessageContext, ServerBuilder, actor};

struct AppState;

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
        // Action 入站：字节负载，consumer 自行解析
        let amount = i64::from_be_bytes(msg.try_into().expect("8-byte action"));
        self.value += amount;
        let _ = ctx.send(self.value.to_be_bytes().to_vec()).await; // Event 定向回 caller
    }
}
```

`#[actor]` 直接挂在要注册的 struct 上，生成注册所需的类型擦除 dispatch 实现；Actor Type 名称不在 macro 中声明，由 consumer 在注册时显式传入（`register::<CounterActor>("counter")`），Client/Server 两侧按同一名称约定寻址。`ActorRuntime` 在构造时注入（`app_state()`、`broadcast()` 等能力）。

## Start a runtime, open a Session, exchange messages

```rust
#[tokio::main]
async fn main() {
    let server = coactor::test_support::TestServerBuilder::new(())
        .register::<CounterActor>("counter")
        .start()
        .await
        .unwrap();

    // 按 Actor Type 名字索引（纯字符串 API，无类型参数、无本地注册表校验）
    let counter = server.client().actor("counter", coactor::ActorId::from("room-7"));
    let mut session = counter.open().await.unwrap();

    session.send(2i64.to_be_bytes().to_vec()).await.unwrap(); // Action
    let event = session.recv().await.unwrap().unwrap();        // Event
    assert_eq!(i64::from_be_bytes(event.try_into().unwrap()), 2);

    server.shutdown().await;
}
```

Run the complete example from this repository:

```console
cargo run -p coactor --example counter
```

## The actor model in five concepts

| Concept | Meaning |
|---|---|
| **Actor Type** | The stable business name passed explicitly to `register::<A>(name)`, unique per runtime; never derived from the Rust type name. |
| **Actor ID** | A caller-provided byte identity, unique within one Actor Type. |
| **Actor Address** | `Actor Type + Actor ID`; the stable identity used for routing. |
| **Action** | Inbound message (bytes) from caller to Actor, fire-and-forget. |
| **Event** | Outbound message (bytes) from Actor to caller, pushed at any time. |
| **Session** | A persistent bidirectional channel between caller and Actor, created by `open()`; all messages travel through it. |

`open()` lazily activates the addressed Actor (running `on_activate` and `on_session_opened` first). Each Active Actor owns one bounded mailbox and executes one message at a time, including across `.await` points. After idle passivation, a process exit, or a crash, the Actor starts again from empty state.

## Configure the runtime

`ServerBuilder` 提供 runtime-wide 默认；`ActorTypeConfig` 按 Actor Type 覆盖：

| Builder method | Default | Meaning |
|---|---|---|
| `mailbox_capacity` | 32 | per-Actor mailbox size; a full mailbox returns `MailboxFull` immediately |
| `max_active_actors` | 10_000 | simultaneous Active Actor limit; excess activations return `RuntimeAtCapacity` |
| `idle_timeout` | 60s | inactivity before idle passivation (only when no live Session) |
| `deactivation_timeout` | 5s | deadline for the `on_deactivate` lifecycle hook |
| `shutdown_timeout` | 30s | global deadline for draining and deactivating on shutdown |

```rust
let server = coactor::test_support::TestServerBuilder::new(state)
    .register_with::<CounterActor>(ActorTypeConfig::new().mailbox_capacity(128))
    .start()
    .await?;
```

## Run a cluster node

Cluster mode runs an embedded Server node per process: an S3-backed ownership authority decides which node owns each Actor Address, and the Server acts as a gateway — it claims unowned actors (or forwards sessions to the current Owner, with placement and bounded retry). Callers connect through a `Client`, which discovers gateway nodes, keeps a pool, and opens Sessions through one of them.

```rust,no_run
use coactor::{
    ClientBuilder, ClientConfig, Endpoint, ServerBuilder, StaticListDiscovery,
    cluster::{ServerConfig, S3OwnershipConfig},
};

# async fn start() -> Result<(), coactor::StartError> {
// Server 节点（宿主 + 网关）
let ownership = S3OwnershipConfig::local(
    "coactor",
    "development",
    "http://127.0.0.1:9000",
);
let server = ServerBuilder::cluster(
    (),
    ServerConfig::new(
        "node-a",
        "0.0.0.0:7000".parse().unwrap(),
        "127.0.0.1:7000".parse().unwrap(),
        ownership,
    ),
)
.start()
.await?;

// Client（调用方）：发现网关节点 → 连接池 → 会话经网关中继
let client = ClientBuilder::new(ClientConfig {
    discovery: StaticListDiscovery::new(vec![Endpoint::new(
        "http://127.0.0.1:7000",
    )]),
})
.start();
# Ok(())
# }
```

For production, construct `S3OwnershipConfig` with the deployment's bucket, prefix, region, credentials provider, and request timeout; use `DnsDiscovery` (e.g. a K8s headless service name) instead of `StaticListDiscovery`. Each node needs a unique advertised address and a stable operational Node ID; every runtime start creates a distinct Node Session internally.

## Semantics you should know

- **Events are not durable.** A successful `send` only means the message was accepted in-process. It is not an acknowledgment that state was persisted, and passivation, panic, or process termination discards the Actor's in-memory state.
- **A live Session blocks passivation.** Idle passivation fires only when the Actor has no remaining live Session. A caller that never closes its Session pins the Actor and its capacity — that responsibility belongs to the consumer.
- **Failover breaks Sessions.** When an Owner fails, the new Owner starts from empty state; existing Sessions must be re-established with a fresh `open()`. A stale Session's next send returns a perceivable error.
- **Always await `Server::shutdown` / `Client::shutdown`.** Dropping a runtime without graceful shutdown has no drain or cleanup guarantee. Shutdown stops new admissions, drains accepted messages, terminates Sessions, runs deactivation, and returns `RuntimeShuttingDown` for new calls.
- **Actor Refs are weak.** Retaining or cloning a Ref does not prevent passivation or keep the runtime alive; after the runtime stops, `open()` returns `RuntimeStopped`.
