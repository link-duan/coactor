# CoActor

CoActor 是一个嵌入式 Rust actor runtime：为稳定业务身份（game room、document 等）提供**串行、非重入**的消息处理，以及 **caller ↔ Actor 的双向 Session 消息**。支持从单机测试到多节点分布的运行形态。

> **Early development:** CoActor is currently an experimental project. Its APIs, runtime semantics, and architecture may change significantly.

## 模型

- **Actor**：`#[coactor::actor]` 标注的 struct + 手写 `impl Actor<S>`（原生 `async fn`），业务逻辑与生命周期 hook 都在 trait 中
- **Action / Event**：Session 上的入站 / 出站字节消息（fire-and-forget）
- **Session**：caller 与 Actor 之间的持久双向通道，由 `ActorRef::open()` 建立
- **Server / Client**：`Server` 宿主 Actor 并充当网关（认领 / 转发 + 放置）；`Client` 是纯调用方（服务发现 → 连接池 → 会话经网关中继），不参与 ownership

状态只在内存中：passivation、进程退出或崩溃后，Actor 从空状态重新启动。当前不提供持久化、durable ACK、Recovery 或 Migration。

## Quick start

```rust
use coactor::{
    Actor, ActorId, ActorRuntime, MessageContext, actor,
    test_support::TestServerBuilder,
};

#[derive(Clone)]
struct AppState { increment_scale: i64 }

#[actor]
struct CounterActor { runtime: ActorRuntime<AppState>, value: i64 }

impl Actor<AppState> for CounterActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self { runtime, value: 0 }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let amount = i64::from_be_bytes(msg.try_into().expect("8-byte action"));
        self.value += amount;
        let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;
    }
}

#[tokio::main]
async fn main() {
    // 本地测试/示例形态：TestServer 装配 inmem transport，client() 内存直连。
    let server = TestServerBuilder::new(AppState { increment_scale: 2 })
        .register::<CounterActor>("counter")
        .start()
        .await
        .expect("valid CoActor configuration");

    let counter = server.client().actor("counter", ActorId::from("counter-7"));
    let mut session = counter.open().await.expect("Session opens");

    session.send(3i64.to_be_bytes().to_vec()).await.expect("accepted");
    if let Some(Ok(event)) = session.recv().await {
        println!("value = {}", i64::from_be_bytes(event.try_into().unwrap()));
    }

    server.shutdown().await;
}
```

```console
cargo run -p coactor --example counter
```

## 了解更多

- [docs/quick-start.md](docs/quick-start.md) — 完整起步（Actor 定义、注册、配置）
- [docs/runtime-semantics.md](docs/runtime-semantics.md) — 本地语义（lifecycle、passivation、错误分类）
- [docs/distributed-runtime-semantics.md](docs/distributed-runtime-semantics.md) — 分布式语义（ownership、fencing、Availability Failover、网关转发）
- [docs/design-brief.md](docs/design-brief.md) — 架构概览与已交付保证
- [docs/adr/](docs/adr/) — 架构决策记录
