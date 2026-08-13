# Architecture Decision Records

ADR status 表示决策是否仍然有效，不表示实现已经交付。当前可观察保证以 runtime semantics 文档为准。

- [ADR-0001: Runtime core](0001-runtime-core.md) — execution、mailbox、lifecycle、failure、shutdown 与 observability
- [ADR-0002: Actor API](0002-actor-api.md) — typed Ref、request-response、App State、Command Context 与 wire API
- [ADR-0003: Infrastructure boundaries](0003-infrastructure-boundaries.md) — CoActor 与通用基础设施的责任边界
- [ADR-0004: Distributed runtime with stateless Availability Failover](0004-distributed-runtime.md) — 下一 distributed slice
- [ADR-0005: Actor Store and Recovery](0005-actor-store-recovery.md) — 后续 persistence 与 durable Recovery
