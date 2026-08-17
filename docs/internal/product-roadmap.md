# 产品路线图

本文件记录内部方向，不构成对外承诺，也不表示相关能力已经交付。

CoActor 当前已完成内存 Actor lifecycle、Session、Node Lease、Ownership Fencing、Availability Failover、Placement 与 S3 Coordination Store。

后续持久化方向包括：

- Actor-scoped KV 与 Actor Store Revision；
- Restore Cut 与 Recovery；
- Migration；
- Durable ACK Gate。

这些能力必须建立在 Ownership Epoch fencing 之上，不能把普通 Event 或 mailbox acceptance 重新解释为持久成功。

其他候选方向包括新的 Coordination backend、TLS/transport 配置，以及 heterogeneous Actor Type Placement。是否实施由独立 issue、spec 和必要的 ADR 决定。
