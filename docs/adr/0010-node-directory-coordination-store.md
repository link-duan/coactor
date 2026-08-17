---
status: accepted
---

# Node Directory 与可替换 Coordination Store

Client 的 Gateway 发现与 Server 的 Placement 当前分别依赖 DNS/静态列表和 Node Lease，形成两份可能漂移的节点来源；同时 `OwnershipBackend` 混合节点租约、Actor ownership 与 S3 ETag 语义，不利于引入 etcd。本 ADR 以 Node Lease 支撑的 Node Directory 取代独立 Service Discovery，并把共享存储重塑为可由 S3 或 etcd 提供的 Coordination Store。

本 ADR supersede ADR-0008 中“Client 不访问 authority”与“DNS/static-list Service Discovery”的决策；Server/Client 拆分、Gateway 中继和 transport seam 继续有效。

## Decision

- **Node Directory 是唯一节点来源**：Client 从 Node Directory 建立 Gateway Pool，Server 从同一目录获取 Placement 候选及 Owner Node Session 的 endpoint。删除生产 API 中的 DNS 与静态列表发现；测试使用内存 Coordination Store，而不是另一套节点来源。
- **Node Directory 与 Ownership Authority 保持语义分离**：Node Directory 只描述当前可用 Node Session；Ownership Authority 仍以 Actor Owner Record 与 Ownership Epoch 决定 Actor 服务权。二者由同一个 Coordination Store 承载，但不是同一个领域能力。
- **Client 直接读取 Coordination Store**：cluster mode 的 Client 是受信任内部服务进程，可以持有 S3/etcd 的只读凭证；Client 不能取得 Node Lease 或 Actor Owner 的写能力。
- **现在引入 backend variant 配置**：公开集群配置以 `CoordinationConfig` variant enum 表达后端选择，首个已交付 variant 为 S3；etcd 作为下一实现加入独立 variant，而不改变 Client/Server 的目录与 ownership 模型。
- **按能力拆分内部接口**：Node Directory 读取、Node Lease 生命周期和 Actor Owner CAS 是独立接口；Client 只持有目录读取能力，Server 按职责持有其余能力。具体 S3/etcd adapter 可以由同一实现对象提供这些能力。
- **版本令牌不透明**：通用接口使用 opaque Revision/Version Token 表达“先前读取的版本”，不暴露 ETag。S3 将其映射到 ETag，etcd 将其映射到 revision；上层只能把令牌原样交回条件写。
- **Gateway 与 Placement 使用不同筛选**：Gateway 候选要求 Node Session 当前可用、协议兼容且非 draining，不因 Actor 容量已满而排除；Placement 额外排除 pressured/full 与本 Node Session。容量快照仍只是 hint，目标节点必须执行本地 admission。
- **目录按后端原生存活语义返回节点**：公共 Node Directory 契约是列出/读取当前 live Node Session，不要求所有后端使用相同的物理过期表示。S3 adapter 通过记录中的截止时间和条件写维护租约；etcd adapter 使用 etcd Lease、attached key 与 keep-alive，租约失效后由 etcd 删除节点 key。
- **etcd 不模拟 S3 时间戳租约**：etcd Node Session key 必须绑定原生 etcd Lease；续持使用 keep-alive，释放使用 revoke/delete，节点存活由 attached key 是否存在判断。CoActor 仍维护本地 authority deadline，并在无法及时确认 keep-alive 时 self-fence。
- **Actor ownership 使用 etcd transaction**：etcd 的首次 claim 使用 key 不存在比较，后续 claim/release 使用 `mod_revision` 比较；S3 继续使用 conditional PUT。两者都必须把 timeout/response loss 视为 ambiguous mutation，并通过有界 read-back 协调，不能盲目重放。

## Consequences

- Client 不再需要 Gateway bootstrap endpoint，但必须配置 Coordination Store endpoint、namespace 与只读凭证；这是 cluster mode 的明确部署前提。
- `ServiceDiscovery`、`DnsDiscovery`、`StaticListDiscovery` 及相应公开配置将被移除；Client 的空池、定期刷新和连接失败恢复都通过 Node Directory 完成。
- Client 需要带 jitter 的周期目录刷新，而不能只在池为空时刷新；目录暂时不可用时，可继续使用缓存中仍可接受的节点，缓存耗尽后新 Session 明确失败，已有 Session 不因此主动中断。
- `OwnershipBackend` 将被 Coordination Store 的能力接口取代，`etag` 字段和 S3 命名从通用 routing/authority 类型中移除。
- S3 与 etcd 可以采用不同租约机制，但必须提供相同的可观察语义：只返回当前 live Node Session、支持有条件的 ownership mutation、暴露 ambiguous outcome，并允许 runtime 在本地 deadline 前确认权威或 self-fence。
