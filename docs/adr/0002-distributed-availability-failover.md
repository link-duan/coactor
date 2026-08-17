---
status: accepted
---

# 提供无状态 Availability Failover 的分布式 runtime

CoActor 在嵌入式 runtime 之上扩展分布式生产能力，但不引入强制的 control-plane service。生产运行必须显式配置 peer 与 ownership，不能静默退化为 local-only。Session 消息保持 location-transparent；Node 间 bidi gRPC stream（见 ADR-0006）提供 peer transport，Ownership Authority 协调 Placement 与 Fencing。CoActor 负责 Actor 层规则，只在其实际保证的底层能力上复用成熟基础设施。

当前公开 cluster API 使用 AWS S3 conditional operation 实现共享 Coordination Store。可续持的 Node Lease 证明一个 runtime session 是否可以服务请求；per-Actor ownership record 决定路由并通过单调 epoch 对 takeover 进行 fencing。Node Lease 丢失会阻止受影响的 runtime session 继续服务 Actor，并通过宿主 supervision boundary 报告终止；library 不终止宿主进程。两层模型避免续租工作随 Active Actor 数量线性增长。

设计有意假设普通时钟同步，并使用本地 monotonic time 执行 self-fencing；不宣称可以容忍任意 clock skew。含糊的 storage mutation 必须执行有界 read-back reconciliation。Capacity information 只是 Placement hint，不是 reservation。

本阶段提供 Availability Failover，而不是 Recovery：确认旧 Owner 失效后，新 Owner 从空 CoActor-managed state 启动，caller 必须重新建立 Session（见 ADR-0005）。开发与 CI 通过 deterministic fake、本地 HTTP contract test 和 S3-compatible emulator 验证 adapter；真实 AWS qualification 是发布门禁，而不是日常测试前提。详细 wire、lease 与 takeover 规则由 distributed runtime semantics 文档维护。
