# CoActor Runtime

CoActor 为业务 key 提供按需存在的有状态实体执行边界。当前上下文只定义实体生命周期与寻址语言，不把进程内状态等同于持久化事实。

## Language

**Entity**:
由 Entity Type 与 Entity Key 唯一标识、按命令顺序改变热状态的逻辑对象。
_Avoid_: Actor instance, Tokio task

**Entity Type**:
稳定命名的一类实体，例如 `room` 或 `document`；它与 Entity Key 共同构成身份。
_Avoid_: Rust type name

**Entity Key**:
由业务提供的稳定字节串，例如 room ID 或 document ID；其含义不依赖进程内 hash seed。
_Avoid_: Actor ID, task ID

**Entity ID**:
Entity Type 与 Entity Key 的稳定组合，可跨进程和节点重新构造同一逻辑身份。
_Avoid_: Memory address, local handle

**Activation**:
一个 Entity 在某个运行时进程中的临时执行实例；同一 Entity 可在 passivation 后产生新的 Activation。
_Avoid_: Entity

**Passivation**:
Activation 在无待处理命令且空闲达到阈值后退出；它不删除 Entity 的逻辑身份或持久状态。
_Avoid_: Deletion, shutdown

**Ownership Epoch**:
未来由控制面授予 owner 的单调 fencing token；所有持久写必须携带它，以便存储层拒绝旧 owner。
_Avoid_: Process generation, activation number
