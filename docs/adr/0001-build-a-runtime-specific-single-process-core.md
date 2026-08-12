# Build a runtime-specific single-process core

CoActor 的第一个切片采用直接构建于 Tokio bounded `mpsc` 之上的 keyed Active Actor registry，而不基于现有 Actor remoting 框架。前置调研显示 Ractor、Kameo 与 Elfo 可提供良好的节点内数据面，但它们都不能替代 durable Actor ownership、fencing 与恢复；先引入其抽象会让本项目最关键的 mailbox、passivation 和 ACK 语义受第三方生命周期约束。这个决定只覆盖单进程核心，不声称已经选择未来网络或集群实现。
