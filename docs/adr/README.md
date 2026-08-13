# Architecture Decision Records

本目录是面向维护者的内部决策索引，不作为公开 README 的入口。ADR 只记录难以逆转的架构取舍与原因；具体 API、状态机、测试矩阵和当前实现保证分别由 spec 与 semantics 文档维护。

ADR status 表示决策是否仍然有效，不表示实现已经交付。

- [ADR-0001: Embedded Actor runtime with a typed API](0001-embedded-actor-runtime.md) — 单进程核心、consumer 边界与调用模型
- [ADR-0002: Distributed runtime with stateless Availability Failover](0002-distributed-availability-failover.md) — peer transport、ownership、fencing 与无状态接管
- [ADR-0003: Actor Store and Recovery as a later persistence stage](0003-actor-store-recovery.md) — 后续持久化与恢复边界
