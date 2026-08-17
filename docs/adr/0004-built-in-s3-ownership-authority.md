---
status: accepted
---

# 内置 S3 Ownership Authority

> 公开 backend 配置与 `OwnershipBackend` seam 首先被 [ADR-0010](0010-node-directory-coordination-store.md) 取代，其 backend variant 配置随后又被 [ADR-0011](0011-chainable-runtime-api.md) 取代。S3 conditional-write correctness 相关决策仍然有效。

CoActor 早期公开 cluster API 将 ownership 绑定到内置 S3 authority，而不是暴露 consumer-supplied provider interface。这样可以在 Node Lease 与 Actor Owner Record 语义共同演进时，保持支持的配置面和 correctness boundary 足够小。crate-private `OwnershipBackend` seam 当时只用于隔离协议实现与支持 deterministic test；增加另一种 authority 必须是有意的 runtime 修改，而不是 application plugin。

后续 ADR 已将公开边界改为 capability-based Coordination Store。本 ADR 只保留 S3 conditional operation、含糊写入 read-back 与 fencing correctness 方面的历史决策。
