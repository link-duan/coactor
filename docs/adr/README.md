# Architecture Decision Records

本目录是面向维护者的内部决策索引，不作为公开 README 的入口。ADR 只记录难以逆转的架构取舍与原因；具体 API、状态机、测试矩阵和当前实现保证分别由 spec 与 semantics 文档维护。

ADR status 表示决策是否仍然有效，不表示实现已经交付。

- [ADR-0001: Embedded Actor runtime](0001-embedded-actor-runtime.md) — 单进程核心、consumer 边界与调用模型
- [ADR-0002: Distributed runtime with stateless Availability Failover](0002-distributed-availability-failover.md) — peer transport、ownership、fencing 与无状态接管
- [ADR-0003: Actor Store and Recovery as a later persistence stage](0003-actor-store-recovery.md) — 后续持久化与恢复边界
- [ADR-0004: Built-in S3 ownership authority](0004-built-in-s3-ownership-authority.md) — 公开集群配置与内部测试 seam 的边界
- [ADR-0005: Session 双向消息模型](0005-session-message-model.md) — Action/Event 字节消息、passivation 条件、failover 打断
- [ADR-0006: Node 间 bidi gRPC stream](0006-bidi-peer-stream.md) — 多路复用流替代 unary Invoke
- [ADR-0007: Actor trait 形态](0007-actor-trait-shape.md) — struct 挂 macro、原生 async fn、按名字索引
- [ADR-0008: Server/Client 拆分](0008-server-client-split.md) — 网关转发、服务发现与 transport seam
- [ADR-0009: 放置均衡 p2c](0009-p2c-placement-balancing.md) — p2c + Load Ratio + In-flight 记账
