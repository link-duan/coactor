# CoActor 自研方案设计简报

## 1. 问题定义

构建一个 Rust stateful entity runtime，重点服务两类负载：

1. 在线游戏：每个 game room 一个实体，承担命令排序、tick、热状态与广播。
2. 协同文档：每个 document 一个实体，承担 mutation 串行化、去重、版本分配与热视图。

这不是单纯的 Actor remoting 项目。核心问题是 entity 的完整生命周期：定位、唯一所有权、激活、持久化、故障转移、fencing、回收和升级。

## 2. 必须明确的运行时语义

- 调用方只使用 `EntityType + EntityId` 寻址。
- 同一实体的 mutation 在单 owner 内串行执行。
- ownership 由 lease/epoch 表示；旧 owner 的持久写必须被存储层拒绝。
- delivery 采用 at-least-once + command ID 幂等，不承诺神奇的 exactly-once。
- ACK 的含义必须区分：已入 mailbox、已写 journal、已提交业务状态。
- 冷实体不对应永久 Tokio task；首次消息触发激活，空闲后安全 passivate。
- 故障恢复从 snapshot + event/op log 开始，并安全处理重复命令。

## 3. 两类负载的不同策略

### Game room

- 热路径追求稳定低延迟，tick 不应同步等待通用 durable workflow。
- 经济、结算等关键命令 durable；位置等软状态可周期 checkpoint。
- 每房间 mailbox 有界；允许合并过期移动输入，但不可静默丢弃关键命令。
- reconnect token 包含 room epoch，防止客户端继续连接旧 owner。

### Collaborative document

- 已 ACK operation 必须可恢复。
- operation 包含 `client_id + op_id + base_version/causal_context + schema_version`。
- CRDT/OT op log 与 snapshot 是长期真源；entity 是串行器、materialized view 与广播协调器。
- presence/cursor 属于软状态，不进入 durable document log。

## 4. 建议的首个 vertical slice

先实现单机多实体，再引入分布式 ownership：

1. `EntityId`、有界 mailbox、按 key 懒激活。
2. idle passivation，保证 passivation 与新消息竞争时不丢消息。
3. command ID 去重、内存 journal 接口、snapshot/restore。
4. 两节点 owner directory，使用单调 epoch。
5. 模拟旧 owner/zombie，证明带旧 epoch 的写被拒绝。
6. kill -9 恢复测试与重复投递测试。

首个 demo 建议选择回合制 room counter，而不是直接实现 60 Hz 游戏循环或完整 CRDT。

## 5. 初始模块草案

```text
coactor-core      EntityId, EntityRef, mailbox, lifecycle
coactor-storage   journal, snapshot, conditional/fenced writes
coactor-cluster   membership, placement, lease/epoch, handoff
coactor-runtime   activation registry, routing, admission control
examples/room     stateful room vertical slice
tests/chaos       crash, partition, duplicate, stale-owner tests
```

模块边界只是起点，应由第一个 vertical slice 反推调整。

## 6. 关键验收问题

1. 两个节点都认为自己是 owner 时，哪个写被拒绝？
2. 已 ACK 的最后一条命令，节点断电后从哪里恢复？
3. passivation/handoff 期间到达的消息在哪里排队？
4. 一百万冷实体占用什么资源？
5. 热 key 过载时是背压、拒绝、合并还是 OOM？
6. 旧事件和 snapshot 如何跨版本读取和迁移？

## 7. 调研背景

前置调研结论：Ractor、Kameo、Elfo 更适合节点内 Actor 数据面，缺少完整的跨节点 durable entity 生命周期；Coerce、Atomr、Murmer 暂不足以直接承载关键状态真源；Restate Virtual Objects 能力接近，但 Server 当前为 BSL 1.1。

原始调研文件位于：

- `/Users/linkduan/Documents/Codex/2026-08-11/tan-s/outputs/rust-distributed-actor-research.md`
- `/Users/linkduan/Documents/Codex/2026-08-11/tan-s/outputs/rust-stateful-entity-actor-for-games-and-docs.md`
