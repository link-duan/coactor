# CoActor

面向在线游戏与协同文档服务端的 Rust 有状态 Actor 运行时。

> Cooperative stateful actors. Identity survives process and node boundaries.

目标业务模型：

- `room_id -> RoomActor`
- `document_id -> DocumentActor`
- 同一 Actor 在任一时刻只有一个有效 Owner
- Actor 按需启动、空闲 passivate、故障后重新定位并恢复
- 后续版本计划提供 Actor-scoped KV 与对象存储 checkpoint

首个版本不提供状态存储。Actor 状态仅存在于内存中，passivation、进程退出或 crash 后不会恢复；CoActor 不宣称 durable ACK、exactly-once 或任何状态恢复保证。

当前阶段以设计验证和最小原型为主，不预设必须兼容某个现有 Actor 框架。

首个版本是单进程 Rust library，不包含跨节点 ownership、fencing、恢复或迁移。

设计与语义：

- [设计简报](docs/design-brief.md)
- [MVP 运行时语义](docs/runtime-semantics.md)
- [架构决策记录](docs/adr/)
