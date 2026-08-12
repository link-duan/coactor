# Hide fencing behind the Persistence API

consumer 不感知 ownership epoch，也不负责为持久写附加 fencing token。runtime 将当前 Actor Address 与 Ownership Epoch 自动绑定到每次 Persistence API 写操作；storage adapter 必须保证旧 epoch 数据不能覆盖或进入当前逻辑提交状态，但对象存储可以留下不可见 orphan。无法提供这一隔离能力的 adapter 不能被声明为支持分布式 ownership。consumer 直接绕过 CoActor Persistence API 的外部写不在 CoActor 的 fencing 保证范围内。
