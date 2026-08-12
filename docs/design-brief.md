# CoActor 自研方案设计简报

## 1. 问题定义

构建一个 Rust stateful Actor runtime，重点服务两类负载：

1. 在线游戏：每个 game room 一个 Actor，承担命令排序、tick、热状态与广播。
2. 协同文档：每个 document 一个 Actor，承担 mutation 串行化、去重、版本分配与热视图。

这不是单纯的 Actor remoting 项目。核心问题是 Actor 的完整生命周期：定位、唯一所有权、启动、持久化、故障转移、fencing、回收和升级。

## 2. 必须明确的运行时语义

- 调用方只使用由 `ActorType + ActorId` 构成的 Actor Address 寻址。
- 同一 Actor 的 mutation 在单 Owner 内串行执行。
- ownership 由 lease/epoch 表示；旧 Owner 的持久数据必须与新 epoch 的状态隔离。
- 不承诺 exactly-once；首版也尚未定义 durable delivery 与 command dedupe。
- ACK 的含义必须区分：已入 mailbox、Handler Reply 与未来可能提供的 Durable ACK。
- 冷 Actor 不对应永久 Tokio task；首次消息触发启动，空闲后安全 passivate。
- 故障恢复使用 epoch-scoped Actor Store 与固定 Restore Cut；不预设 consumer 必须使用 snapshot + event/op log。

## 3. 两类负载的不同策略

### Game room

- 热路径追求稳定低延迟，tick 不应同步等待通用 durable workflow。
- 经济、结算等关键命令 durable；位置等软状态可周期 checkpoint。
- 每房间 mailbox 有界；允许合并过期移动输入，但不可静默丢弃关键命令。
- reconnect token 包含 room epoch，防止客户端继续连接旧 owner。

### Collaborative document

- 如果未来对 operation 提供 Durable ACK，则已 durable ACK 的 operation 必须可恢复；首版普通 Handler Reply 不满足该语义。
- operation 包含 `client_id + op_id + base_version/causal_context + schema_version`。
- CRDT/OT op log 与 snapshot 是长期真源；Actor 是串行器、materialized view 与广播协调器。
- presence/cursor 属于软状态，不进入 durable document log。

## 4. 建议的首个 vertical slice

首个版本只实现单进程 Actor runtime：

1. `ActorAddress`、有界 mailbox、按 Actor ID 懒启动。
2. idle passivation，保证 passivation 与新消息竞争时不丢消息。

状态存储、distributed ownership、fencing、恢复与迁移在后续阶段整体推进，不属于首版实现或保证。

首个 demo 选择最小 `CounterActor`，只验证通用 runtime 语义，不实现 60 Hz 游戏循环、room domain 或完整 CRDT。

## 5. 初始模块草案

```text
coactor-core      ActorType, ActorId, ActorAddress, ActorRef, mailbox, lifecycle
coactor-runtime   Active Actor registry, routing, admission control
examples/room     stateful room vertical slice
```

模块边界只是起点，应由第一个 vertical slice 反推调整。

## 6. 关键验收问题

1. 同一 Actor Address 的并发 command 是否始终由一个 Active Actor 串行处理？
2. passivation 期间到达的消息在哪里排队，是否可能丢失？
3. 一百万冷 Actor 占用什么资源？
4. 热 key 过载时是背压、拒绝、合并还是 OOM？
5. handler panic 或 Active Actor task 异常结束后，已接纳 command 如何失败？

## 7. 调研背景

前置调研结论：Ractor、Kameo、Elfo 更适合节点内 Actor 数据面，缺少完整的跨节点 durable Actor 生命周期；Coerce、Atomr、Murmer 暂不足以直接承载关键状态真源；Restate Virtual Objects 能力接近，但 Server 当前为 BSL 1.1。

原始调研文件位于：

- `/Users/linkduan/Documents/Codex/2026-08-11/tan-s/outputs/rust-distributed-actor-research.md`
- `/Users/linkduan/Documents/Codex/2026-08-11/tan-s/outputs/rust-stateful-entity-actor-for-games-and-docs.md`
