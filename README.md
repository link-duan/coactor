# CoActor

CoActor 是一个 Rust actor runtime，为稳定业务身份（game room、document 等）提供**串行、非重入**的消息处理，并支持 **caller ↔ Actor 的双向 Session 消息**。

> **Early development:** CoActor is currently an experimental project. Its APIs, runtime semantics, and architecture may change significantly.

## 模型

- **Action**：caller 通过 Session 发送的入站消息（字节负载，fire-and-forget）
- **Event**：Actor 通过 Session 推送的出站消息（字节负载，可随时推送）
- **Session**：caller 与 Actor 之间的一次持久双向通道；`open()` 建立，所有消息都经它
- **Actor**：实现 `Actor<S>` trait 的业务逻辑（`new(ActorRuntime<S>)` + `on_message` + 可选 lifecycle hooks）

状态只在内存中。passivation、进程退出或崩溃后，Actor 从空状态重新启动。CoActor 当前不提供持久化、durable ACK、Recovery 或 Migration。

## Quick start

```rust
use coactor::{Actor, ActorId, ActorRuntime, BoxFuture, MessageContext, RuntimeBuilder, actor};

#[derive(Clone)]
struct AppState { increment_scale: i64 }

#[actor]
struct CounterActor { runtime: ActorRuntime<AppState>, value: i64 }

impl Actor<AppState> for CounterActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime, value: 0 }
    }

    fn on_message<'a>(&'a mut self, ctx: &'a MessageContext, msg: &'a [u8]) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let amount = i64::from_be_bytes(msg.try_into().expect("8-byte action"));
            self.value += amount * self.runtime.app_state().increment_scale;
            let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;  // 定向 Event
            // 或 self.runtime.broadcast(bytes) 广播给全部存活 Session
        })
    }
}

#[tokio::main]
async fn main() {
    let runtime = RuntimeBuilder::local(AppState { increment_scale: 2 })
        .register::<CounterActor>("counter")
        .start()
        .await
        .expect("valid CoActor configuration");

    // 按名字索引（纯字符串 API，无需类型参数），open 建立持久双向 Session
    let counter = runtime.actor("counter", ActorId::from("counter-7"));
    let mut session = counter.open().await.expect("Session opens");

    session.send(3i64.to_be_bytes().to_vec()).await.expect("accepted"); // Action 入站
    if let Some(Ok(event)) = session.recv().await {                     // Event 出站
        println!("value = {}", i64::from_be_bytes(event.try_into().unwrap()));
    }

    runtime.shutdown().await;
}
```

```console
cargo run -p coactor --example counter
```

## Actor trait

```rust
pub trait Actor<S>: Send + 'static {
    fn new(runtime: ActorRuntime<S>) -> Self;
    fn on_message<'a>(&'a mut self, ctx: &'a MessageContext, msg: &'a [u8]) -> BoxFuture<'a, ()>;
    // 可选 lifecycle（默认空实现）
    fn on_activate<'a>(&'a mut self) -> BoxFuture<'a, Result<(), String>>;
    fn on_deactivate<'a>(&'a mut self, reason: DeactivationReason) -> BoxFuture<'a, ()>;
    fn on_session_opened<'a>(&'a mut self, ctx: &'a MessageContext) -> BoxFuture<'a, ()>;
    fn on_session_closed<'a>(&'a mut self, ctx: &'a MessageContext) -> BoxFuture<'a, ()>;
}
```

- `ActorRuntime<S>`：actor 侧句柄（`actor_id()`、`app_state()`、`broadcast()`），构造时注入一次
- `MessageContext`：当前消息上下文（`actor_id()`、`session()`、`send()` 定向当前 Session）
- `SessionHandle`：可 clone 的定向出站句柄，存状态后用于私聊

## 语义要点

- **串行非重入**：同一 Actor 一次处理一条消息（含 `.await` 期间）；不同 Actor 并行
- **懒激活 + open 主动激活**：`open()` 激活 Actor → 注册 Session → `on_session_opened`（可"连接即推送"）
- **passivation 仅当无存活 Session**：有 Session 的 Actor 不会被 idle passivation
- **failover 打断 Session**：Owner 失效后 Session 终止，caller 需重新 `open()`（空状态激活）
- **fire-and-forget**：`send` 同步返回投递状态；进入 mailbox 后不再确认（at-most-once）
- **背压**：mailbox 与出站接收流均 bounded，满载返回 `MailboxFull`

## Cluster 模式

每进程嵌入 runtime（peer node），S3 提供 Node Lease 与 Actor Owner Record 的 ownership 权威；消息经 ownership 解析路由到 Owner（Node 间 bidi gRPC stream 多路复用），对 caller 位置透明。

```rust
let runtime = RuntimeBuilder::cluster((), ClusterConfig::new(
    "node-a", "0.0.0.0:7000".parse().unwrap(),
    "127.0.0.1:7000".parse().unwrap(),
    S3OwnershipConfig::local("coactor", "development", "http://127.0.0.1:9000"),
))
.register::<CounterActor>()
.start()
.await?;
```

详细语义见 `docs/runtime-semantics.md`、`docs/distributed-runtime-semantics.md`。
