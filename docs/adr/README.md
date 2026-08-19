# 架构决策记录

本目录是面向维护者的内部决策索引，不作为公开 README 的入口。ADR 只记录难以逆转的架构取舍与原因；具体 API、状态机、测试矩阵和当前实现保证分别由 spec、源码与对外 semantics 文档维护。

ADR status 表示决策是否仍然有效，不表示实现已经交付。被后续 ADR 部分取代的记录会在正文开头明确说明仍有效与已失效的范围。

- [ADR-0001：嵌入式 Actor runtime](0001-embedded-actor-runtime.md) — 单进程核心、consumer 边界与调用模型
- [ADR-0002：提供无状态 Availability Failover 的分布式 runtime](0002-distributed-availability-failover.md) — peer transport、ownership、fencing 与无状态接管
- [ADR-0004：内置 S3 Ownership Authority](0004-built-in-s3-ownership-authority.md) — S3 conditional-write correctness；公开配置先后由 ADR-0010/0011 取代
- [ADR-0005：Session 双向消息模型](0005-session-message-model.md) — Action/Event 字节消息、Passivation 条件、Failover 打断
- [ADR-0006：Node 间 bidi gRPC stream](0006-bidi-peer-stream.md) — 多路复用流替代 unary Invoke
- [ADR-0007：Actor trait 形态](0007-actor-trait-shape.md) — struct 挂 macro 与原生 async fn；具体寻址 API 由 ADR-0011 取代
- [ADR-0008：Server/Client 拆分](0008-server-client-split.md) — Gateway 与 transport seam；节点发现和公开 API 分别由 ADR-0010/0011 取代
- [ADR-0009：Placement 均衡 p2c](0009-p2c-placement-balancing.md) — p2c + Load Ratio + In-flight Placement 记账
- [ADR-0010：Node Directory 与可替换 Coordination Store](0010-node-directory-coordination-store.md) — capability 拆分与 backend 原生 lease；公开配置和稳定 Node identity 由 ADR-0011/0012 扩展
- [ADR-0011：链式构造 runtime 并直接打开 Session](0011-chainable-runtime-api.md) — capability 注入、可选 State、直接按 Actor Address 打开 Session
- [ADR-0012：稳定 Node identity 与可读 Coordination Store key](0012-stable-node-identity-and-keys.md) — 稳定 Node ID、进程 incarnation 与可读持久 key
- [ADR-0013：Client 直连 Owner 并负责 Placement](0013-client-direct-owner-placement.md) — 移除 Gateway/Relay、Client-side placement 与 endpoint 多连接池
