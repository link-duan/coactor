# CoActor

CoActor 是一个嵌入式 Rust actor runtime：为稳定业务身份（game room、document 等）提供**串行、非重入**的消息处理，以及 **caller ↔ Actor 的双向 Session 消息**。支持从单机测试到多节点分布的运行形态。

> **Early development:** CoActor is currently an experimental project. Its APIs, runtime semantics, and architecture may change significantly.

## 模型

- **Actor**：`#[coactor::actor]` 标注的 struct + 手写 `impl Actor<S>`（原生 `async fn`），业务逻辑与生命周期 hook 都在 trait 中
- **Action / Event**：Session 上的入站 / 出站字节消息（fire-and-forget）
- **Session**：caller 与 Actor 之间的持久双向通道，由 `ActorRef::open()` 建立
- **Server / Client**：`Server` 宿主 Actor 并充当 Gateway（认领、转发与 Placement）；`Client` 从 Coordination Store 的 Node Directory 建立 Gateway Pool，经 Gateway 中继 Session，不变更 Node Lease 或 Actor Owner Record

状态只在内存中：passivation、进程退出或崩溃后，Actor 从空状态重新启动。当前不提供持久化、durable ACK、Recovery 或 Migration。

## Quick start

生产环境的 Server、Client、S3 Coordination Store 配置与完整运行步骤见：

- [docs/quick-start.md](docs/quick-start.md)
- [coactor/examples/cluster_counter.rs](coactor/examples/cluster_counter.rs)

## 了解更多

- [docs/quick-start.md](docs/quick-start.md) — 完整起步（Actor 定义、注册、配置）
- [docs/runtime-semantics.md](docs/runtime-semantics.md) — 本地语义（lifecycle、passivation、错误分类）
- [docs/distributed-runtime-semantics.md](docs/distributed-runtime-semantics.md) — 分布式语义（ownership、fencing、Availability Failover、网关转发）
- [docs/design-brief.md](docs/design-brief.md) — 架构概览与已交付保证
- [docs/adr/](docs/adr/) — 架构决策记录
