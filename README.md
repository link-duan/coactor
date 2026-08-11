# CoActor

面向在线游戏与协同文档服务端的 Rust 有状态实体运行时。

> Cooperative stateful actors. Identity survives process and node boundaries.

目标业务模型：

- `room_id -> RoomEntity`
- `document_id -> DocumentEntity`
- 同一实体在任一时刻只有一个有效 writer
- 实体按需激活、空闲回收、故障后重新定位并恢复
- 已确认的关键命令可从持久化日志与快照恢复

当前阶段以设计验证和最小原型为主，不预设必须兼容某个现有 Actor 框架。

设计与语义：

- [设计简报](docs/design-brief.md)
- [MVP 运行时语义](docs/runtime-semantics.md)
- [架构决策记录](docs/adr/)
