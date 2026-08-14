# CoActor Runtime

CoActor 为业务 key 提供按需运行的 Actor 执行边界。当前上下文只定义 Actor 生命周期与寻址语言，不把进程内状态等同于持久化事实。

## Language

**Actor**:
由 Actor Address 唯一标识的逻辑 Actor；无论它当前是否启动、位于哪个进程，其身份保持不变。
_Avoid_: Entity, Actor instance, Tokio task

**Actor Type**:
consumer 通过 Actor macro 显式声明的稳定业务类型名称，例如 `room` 或 `document`；名称不从 Rust 类型名推导，并在一个 runtime 内唯一。
_Avoid_: Rust type name

**Actor ID**:
由业务提供、在同一 Actor Type 内唯一的稳定字节包装，例如 room ID 或 document ID；Actor Type 的 `new` 接收该统一类型，业务解释由 consumer 负责。
_Avoid_: Actor Key, Task ID, memory address

**Actor Address**:
Actor Type 与 Actor ID 的稳定组合，是 runtime 用于寻址 Actor 的完整身份，可跨进程和节点重新构造。
_Avoid_: Actor ID, Entity ID, local handle

**Actor Ref**:
macro 为每个 Actor Type 生成的专用类型安全稳定地址句柄，例如 `RoomActorRef`；每个 command 是其 inherent async method。它只弱引用 runtime，持有或 clone 不会保持 runtime/Actor 存活，也不会阻止 passivation。
_Avoid_: Actor Address, untyped byte channel

**Active Actor**:
一个 Actor 启动后在某个 runtime 进程中的临时执行实例，持有 mailbox 与热状态；同一 Actor 可先后产生多个 Active Actor。
_Avoid_: Actor, Entity, Tokio task

**Actor Stopped**:
Active Actor 因 handler panic 或异常 task 终止而无法继续处理已接纳 command 的失败结果；后续 command 可以创建新的空状态实例。
_Avoid_: Passivation, deactivation

**Command**:
caller 对 Actor method 的一次 request-response 调用；macro 将方法调用转换为内部消息，首版每个 command 必须产生业务结果或可观察的 runtime error。
_Avoid_: Event, fire-and-forget message

**Send Error**:
Actor method 调用的统一失败类型，既包含 mailbox、lifecycle 与 Actor 停止等 runtime failure，也以 `HandlerError(E)` 承载方法声明的业务错误。
_Avoid_: Nested runtime and handler Results

**Passivation**:
Active Actor 在满足安全条件后退出；它不删除 Actor 的逻辑身份或持久状态。
_Avoid_: Deletion, shutdown

**Deactivating**:
Active Actor 已开始执行 deactivation lifecycle、不可再恢复服务的临时状态；路由保留到 lifecycle 结束，新 command 返回可重试错误。
_Avoid_: Idle, passivation candidate

**Ownership Epoch**:
ownership CAS 产生的单调世代；runtime 将其绑定到持久数据命名空间和恢复边界，consumer 不感知该值。
_Avoid_: Process generation, activation number

**Owner**:
当前被授权为某个 Actor Address 处理命令并执行持久写的 runtime 节点；授权必须与 Ownership Epoch 一起判断。
_Avoid_: Active Actor location, registry entry

**Fencing**:
阻止 stale Owner 继续服务，或阻止其数据成为当前逻辑提交状态的安全机制组合。
_Avoid_: Heartbeat alone, process shutdown alone

**Self-Fence**:
节点或 Active Actor 无法在时限内证明自己仍有权威时，主动停止 mutation、持久化和成功响应的状态转换。
_Avoid_: Graceful shutdown, lease renewal

**Durable ACK Gate**:
在持久化证明覆盖本次 mutation 之前扣留 durable success response 的边界。CoActor 首版不提供 durable ACK，因此普通 Handler Reply 不经过该边界。
_Avoid_: Handler Reply, enqueue ACK

**Handler Reply**:
Actor 完成本地 command handling 后返回的结果；首版中它不证明对应 mutation 已持久化，crash 后可能丢失。
_Avoid_: Durable ACK, commit receipt

**Restore Cut**:
新 Owner 获得 Ownership Epoch 后，从小于新 epoch 的最大现存 state epoch 第一次成功读取时固定的确定版本，由 source epoch、Actor Store revision 与 object ETag 标识。
_Avoid_: Re-reading until newer, best-effort snapshot

**Recovery**:
Owner 意外失效后，在有效 Owner 上重建 Actor 可继续服务状态的过程。
_Avoid_: In-memory restart, migration

**Migration**:
在原 Owner 可参与时，有计划地转移 Actor ownership 与服务位置的过程。
_Avoid_: Recovery, restart

**Actor Lifecycle Method**:
由 runtime 在确定生命周期阶段调用并等待、由 consumer 异步实现的 Actor 方法；consumer 可在其中执行 restore 或持久化。
_Avoid_: Command handler, arbitrary callback

**Command Context**:
runtime 为一次 Command invocation 提供的最小只读运行环境；它只标识当前 Actor，不是 Actor 的业务状态，也不提供跨 Actor 调用。
_Avoid_: Actor Context, Actor state, global variable

**App State**:
一个 CoActor runtime 内由所有 Actor Type 共享的 consumer 强类型依赖容器，例如数据库 client、HTTP client 和配置。
_Avoid_: TypeId service locator, Actor state

**Persistence API**:
CoActor 向 Actor 提供的持久存储访问边界；它不规定 consumer 保存的数据模型，后台 checkpoint 时机由 runtime policy 决定。
_Avoid_: Automatic persistence, durable ACK

**Actor-scoped KV**:
以当前 Actor Address 隔离命名空间的 KV 能力；Active Actor 通过本地嵌入式 KV 引擎操作单文件 Actor Store，consumer 不感知物理文件或对象地址。
_Avoid_: Global KV, raw object-store client

**Actor Store**:
承载一个 Actor-scoped KV 的本地单文件存储；Active Actor 操作本地文件，runtime 将其连同 `format_version`、`revision` 和 `checksum` 作为单个 durable state object 持久化到对象存储。
_Avoid_: In-memory state, one S3 object per KV key

**Actor Store Revision**:
Actor Store 每次成功提交本地 KV 写事务后由 runtime/KV 层递增的状态版本；它不对应 command 或单个 key。
_Avoid_: Command sequence, ownership epoch

**Persistence Fault**:
Actor Store 在有界重试后仍无法持久化的单 Actor 停止服务状态；Active Actor 保留本地文件并拒绝所有新 command，等待恢复或运维处置。
_Avoid_: Ownership loss, node-wide fence

**Restore Fault**:
选定的历史 state object 存在但无法通过完整性、格式或 KV 文件校验时的 activation 失败状态；runtime 不自动回退到更早状态。
_Avoid_: Empty state, first activation

**Ownership Authority**:
由 runtime 掌控、负责竞争、续持和释放 ownership，并产生单调 Ownership Epoch 的共享权威。它是集群正确性边界，不是 consumer 可替换的插件 API。
_Avoid_: Ownership Provider, Actor registry, service discovery

**Node**:
嵌入 CoActor runtime 并通过网络接收或转发 Command 的 consumer 进程运行位置；一个 Node 的长期运维身份与单次运行会话必须区分。
_Avoid_: Actor Owner, Runtime handle, Pod

**Node ID**:
用于日志、运维和部署定位的 Node 稳定标签；它本身不证明当前进程拥有服务权。
_Avoid_: Node Session ID, Owner

**Node Session ID**:
一次 runtime 启动的唯一身份，是 Node Lease 与 Actor Owner Record 中判断实际 Owner 进程的依据。
_Avoid_: Node ID, hostname, Pod name

**Node Lease**:
共享 ownership authority 中证明某个 Node Session 在有界时间内仍具备运行资格的记录；失去该资格会触发整个 runtime self-fence。
_Avoid_: Actor Owner Record, heartbeat alone

**Actor Owner Record**:
共享 ownership authority 中把一个 Actor Address 绑定到 Node Session 与 Ownership Epoch 的记录；它决定路由和 takeover，不表示 Active Actor 当前一定驻留内存。
_Avoid_: Node Lease, local route, service discovery entry

**Availability Failover**:
旧 Owner 失效后，由新 Owner 以更高 Ownership Epoch 从空 CoActor 状态重新提供服务；consumer 可自行重建外部状态，但 CoActor 不保证状态恢复。
_Avoid_: Recovery, Migration, restart

**Ownership Fault**:
runtime 无法取得、确认或继续持有服务权时产生的分布式 lifecycle failure；它不得被当作普通业务错误或持久化故障。
_Avoid_: Handler Error, Persistence Fault, mailbox overload

**Consumer**:
通过 CoActor Rust library 注册 Actor Type、接收命令并提供 Actor 业务逻辑的宿主应用。
_Avoid_: Actor, runtime node
