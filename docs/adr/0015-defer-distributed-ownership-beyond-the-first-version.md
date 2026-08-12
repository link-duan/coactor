# Defer distributed ownership beyond the first version

CoActor 首个版本不实现 distributed ownership、Ownership Epoch、fencing、故障转移或 migration，只验证单进程内 Actor Address 到 Active Actor 的唯一映射。没有状态恢复时单独提供跨节点 ownership 会形成“Actor 可以切换节点但会以空状态启动”的误导能力，也无法验证完整的 stale-owner 安全闭环。ownership、fencing 与恢复将在后续阶段整体推进；首版不得把进程内 registry 描述为全局 ownership 证明。

该唯一映射进一步限定在单个 CoActor runtime 实例内。同一进程可以创建多个 runtime，但它们不共享 registry 或协调相同 Actor Address；library 不通过进程级全局单例制造隐式 ownership。
