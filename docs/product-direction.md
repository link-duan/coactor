# Product direction

CoActor 的当前目标是提供一个可嵌入 Rust 应用、以稳定业务 key 寻址的分布式 Actor runtime。它优先保证 runtime lifecycle、Session、placement 与 ownership fencing 的语义清晰，再逐步增加持久化能力。

## 已交付方向

- 经过校验的 `ActorAddress` 是唯一公开 Actor 地址值，可跨 Client 复用。
- `Client::open(&ActorAddress)` 直接建立持久双向 Session；Action/Event 使用字节 payload，由 consumer 管理业务 schema。
- `Server::builder(coordination_store)` 通过 capability injection 选择 Coordination backend；`Client` 只依赖 Node Directory。
- App State 默认 `()`，可用 `.with_state` 注入；Actor 通过 `ActorRuntime::state()` 读取共享依赖。
- `TestServer` 提供独立内存测试路径，不把生产 local mode 混入 Server API。
- Node ID 是稳定逻辑 slot，Node Session ID 是进程 incarnation；Ownership Epoch 对 Actor authority 进行 fencing。
- S3 backend 使用 readable object keys、conditional writes、exact read-back 与 conditional delete。
- placement 采用 live Node 过滤、load ratio、p2c 与 in-flight bookkeeping，避免 burst herd。

## API 原则

- 常见路径应链式、具名且无位置歧义。
- backend 通过具体 capability 注入，不通过 crate-owned config enum 选择。
- lifecycle handle 不可 Clone，shutdown 消费 handle。
- 地址校验发生一次；网络阶段不重复同步身份校验。
- Open lifecycle 与 Action/Event delivery 使用不同错误边界。
- public error kind 稳定，backend source 保留在标准 error chain 中。
- 不为尚未交付的 backend、持久化格式或 placement capability 引入 speculative abstraction。

## 下一阶段

未来工作集中在 Actor-scoped KV、Actor Store revision、Restore Cut、Recovery、Migration 与 Durable ACK Gate。它们必须在 Ownership Epoch fencing 之上构建，不能把普通 Event 或内存 mailbox acceptance 重新解释为持久成功。

其他可能方向包括新的 Coordination backend、TLS/transport 配置、per-node Actor Type capability advertisement 与 heterogeneous placement；这些均不是当前 runtime guarantee。
