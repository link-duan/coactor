# Own Actor semantics, not infrastructure

CoActor 自己定义并验证 Actor identity、注册、寻址、生命周期、路由、ownership 与 fencing 语义，但不以自研共识算法、数据库或通用网络传输为目标。成熟基础设施通过明确边界接入，其实际保证必须由一手资料和 CoActor 的集成测试确认；仅仅采用某个组件不能自动证明 CoActor 的分布式正确性。这样既保留运行时的核心产品价值，也避免把基础设施重造成本混入 Actor 模型。
