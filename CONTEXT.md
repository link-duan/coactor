# CoActor Runtime

CoActor 为业务 key 提供按需运行的 Actor 执行边界。本词汇表定义已交付的执行与 ownership 语言。

## Language

**Actor**:
由 Actor Address 唯一标识的逻辑 Actor；无论它当前是否启动、位于哪个进程，其身份保持不变。
_Avoid_: Entity, Actor instance, Tokio task

**Actor Type**:
consumer 约定并在注册时显式提供的稳定业务类型名称，在一个 runtime 内唯一；采用 Kubernetes DNS label 形式（1–63 个字符、小写字母或数字起止、中间可含 `-`），例如 `room` 或 `collab-document`。Client 与 Server 通过该名称约定相互寻址，名称不从 Rust 类型名推导、不由 macro 生成。
_Avoid_: Rust type name, macro 属性, arbitrary string, path

**Actor ID**:
由业务提供、在同一 Actor Type 内唯一的稳定标识符；采用 Kubernetes DNS label 形式（1–63 个字符、小写字母或数字起止、中间可含 `-`），例如 `room-123`。
_Avoid_: Actor Key, arbitrary bytes, path, memory address

**Actor Address**:
Actor Type 与 Actor ID 的稳定组合，是 runtime 用于寻址 Actor 的完整身份，可跨进程和节点重新构造。
_Avoid_: Actor ID, Entity ID, local handle

**Active Actor**:
一个 Actor 启动后在某个 runtime 进程中的临时执行实例，持有 mailbox 与热状态；同一 Actor 可先后产生多个 Active Actor。
_Avoid_: Actor, Entity, Tokio task

**Actor Stopped**:
Active Actor 因 handler panic 或异常 task 终止而无法继续处理已接纳 Action 的失败结果；后续 Action 可以创建新的空状态实例。
_Avoid_: Passivation, deactivation

**Action**:
caller 通过 Session 向 Actor 发送的入站消息，字节负载，无 reply 语义；Actor 处理 Action 不产生同步结果，任何输出都以 Event 形式推送。
_Avoid_: Command, request, method call

**Event**:
Actor 通过 Session Handle 推送给 caller 的出站消息，字节负载；Actor 可随时推送，不要求对应一次 Action。
_Avoid_: Reply, response

**Session**:
caller 与 Actor 之间的一次双向通道；所有 Action 都经 Session 入站，Event 经 Session 出站。Client 以 Actor Address 打开 Session；Owner 或绑定的 Transport Connection 失效都会显式打断它，由 caller 重新打开。
_Avoid_: Connection handle, command channel

**Session Handle**:
Actor 侧持有的、可 clone 的 Session 出站句柄；Actor 通过它向对应 caller 推送 Event，可存入 Actor 状态用于广播。
_Avoid_: Reply channel, caller reference

**Session ID**:
Session 的唯一标识，由 caller 建立 Session 时生成；Action 与 Event 消息用它寻址，也是运行时 Session 记录与路由的标签。
_Avoid_: Connection ID, message ID

**Send Error**:
Action 投递与 Event 推送的统一失败类型，包含 mailbox、lifecycle 与 Actor 停止等 runtime failure；业务错误不属于 Send Error，由 Event 表达。
_Avoid_: Nested runtime and handler Results

**Passivation**:
Active Actor 在满足安全条件后退出；它不删除 Actor 的逻辑身份或 ownership 历史。
_Avoid_: Deletion, shutdown

**Deactivating**:
Active Actor 已开始执行 deactivation lifecycle、不可再恢复服务的临时状态；路由保留到 lifecycle 结束，新 Action 返回可重试错误。
_Avoid_: Idle, passivation candidate

**Ownership Epoch**:
ownership CAS 产生的单调 Owner 世代；它区分同一 Actor Address 的先后授权，consumer 不感知该值。
_Avoid_: Process generation, activation number

**Owner**:
当前被授权为某个 Actor Address 处理 Action 的 runtime 节点；授权必须与 Ownership Epoch 一起判断。
_Avoid_: Active Actor location, registry entry

**Fencing**:
阻止 stale Owner 继续服务的安全机制组合。
_Avoid_: Heartbeat alone, process shutdown alone

**Self-Fence**:
节点无法在时限内证明自己仍有权威时，主动停止 mutation 和成功响应的状态转换。
_Avoid_: Graceful shutdown, lease renewal

**Actor Lifecycle Method**:
由 runtime 在确定生命周期阶段调用并等待、由 consumer 异步实现的 Actor 方法。
_Avoid_: Action handler, arbitrary callback

**Message Context**:
runtime 为一次 Action 处理提供的最小只读运行环境；它标识当前 Actor，并携带当前 Session 的出站句柄，供 Actor 推送 Event。它不是 Actor 的业务状态，也不提供跨 Actor 调用。
_Avoid_: Actor Context, Actor state, global variable

**App State**:
一个 CoActor runtime 内由所有 Actor Type 共享的 consumer 强类型依赖容器，例如数据库 client、HTTP client 和配置。
_Avoid_: TypeId service locator, Actor state

**Ownership Authority**:
由 runtime 掌控、负责竞争、续持和释放 ownership，并产生单调 Ownership Epoch 的共享权威。它是集群正确性边界，通过 Coordination Store 实现，但不等同于 Node Directory。
_Avoid_: Ownership Provider, Actor registry, Node Directory

**Coordination Store**:
承载 Node Lease 与 Actor Owner Record 的共享存储边界；它提供节点租约、live Node Directory 与 ownership 条件写语义，具体后端可采用 S3 或 etcd 的原生机制。
_Avoid_: Ownership Authority, Service Discovery

**Node**:
嵌入 CoActor Server 并通过网络直接服务 Session 的 consumer 进程运行位置；一个 Node 的长期运维身份与单次运行会话必须区分。
_Avoid_: Actor Owner, Runtime handle, Pod

**Node ID**:
Server 跨进程重启保持的稳定逻辑节点身份；同一时刻最多由一个有效 Node Session 占用。Actor placement 可保留该逻辑归属，但实际服务权仍须结合 Node Session ID 与 Ownership Epoch 判断。
_Avoid_: Node Session ID, display label, process identity

**Node Session ID**:
一次 runtime 启动的唯一身份，是 Node Lease 与 Actor Owner Record 中判断实际 Owner 进程的依据。
_Avoid_: Node ID, hostname, Pod name

**Node Lease**:
授予某个 Node Session 在有界时间内占用某个 Node ID 的运行资格；失去续持资格会触发整个 runtime self-fence。
_Avoid_: Actor Owner Record, heartbeat alone

**Load Ratio**:
Node 当前负载的排序指标，由当前活跃 Actor 数与上限之比（active/max）表示，用于 placement 决策的相对比较；它区分节点容量差异，避免以绝对数量或剩余容量误导决策。
_Avoid_: active actor count alone, remaining capacity

**Actor Owner Record**:
把一个 Actor Address 绑定到 Node ID、Node Session ID 与 Ownership Epoch 的权威记录；Node ID 保留逻辑 placement，Node Session ID 区分具体进程 incarnation。它不表示 Active Actor 当前一定驻留内存。
_Avoid_: Node Lease, local route, Node Directory entry

**Availability Failover**:
旧 Owner 失效后，由新 Owner 以更高 Ownership Epoch 从空 CoActor 状态重新提供服务。
_Avoid_: restart

**Ownership Fault**:
runtime 无法取得、确认或继续持有服务权时产生的分布式 lifecycle failure；它不得被当作普通业务错误或 mailbox overload。
_Avoid_: Actor Stopped, mailbox overload

**Consumer**:
通过 CoActor Rust library 注册 Actor Type、发送 Action、接收 Event 并提供 Actor 业务逻辑的宿主应用。
_Avoid_: Actor, runtime node

**Server**:
embed CoActor runtime 并宿主 Actor 的一侧；它注册 Actor Type、维护 ownership 与生命周期、accept Client 直连 Session，并且是唯一可以变更 Node Lease 或 Actor Owner Record 的一方。Server 不提供 caller 能力，也不转发其他 Server 的 Session 消息。
_Avoid_: Runtime, Node, Host

**Client**:
embed CoActor runtime 但只发起 Session 的调用方；它通过 Coordination Store 的只读 Node Directory 与 Actor Owner 查询直接连接 Owner，并在 Actor unowned/stale 时执行 Placement。Client 不宿主 Actor，也不能变更 Node Lease 或 Actor Owner Record。
_Avoid_: Runtime, caller handle

**Transport Connection**:
Client 与一个 Server endpoint 之间承载 Envelope 的双向连接；一条连接可复用多个 Session，但一个 Session 从打开到关闭固定使用同一条连接。
_Avoid_: Session, Node peer, Actor connection

**Transport Connection Pool**:
Client 为一个 Server endpoint 维护的有界 Transport Connection 集合，用于分散 Session 并隔离单连接故障。
_Avoid_: Gateway Pool, Session pool

**Node Directory**:
由当前 live Node Session 构成的只读节点目录，是 Client 解析 Owner endpoint 与构造 Placement 候选的共同节点来源；它不决定 Actor ownership，也不保证 endpoint 此刻一定可连接。
_Avoid_: Service Discovery, Actor registry, Actor Owner Record

**Placement**:
Client 在 Actor unowned/stale 时根据 Node Directory 负载快照选择候选 Server 的一次性决策；目标 Server 的 capacity reservation 与 ownership claim 才是最终 admission。Placement 不持续再平衡。
_Avoid_: Load balancing, routing

**Placement Burst**:
短时间内大量未拥有 Actor 同时需要放置决策（例如直播开播时上千新 room 同时 open）；它是放置倾斜的主要触发场景，由 p2c 随机化与 In-flight Placement 记账共同缓解。
_Avoid_: Traffic spike, hot start

**In-flight Placement**:
Client 已选择候选 Server、但该 Server 的 Lease 尚未反映新 Active Actor 的本地记账计数；它参与预测 Load Ratio，防止同一 Client 内并发放置的 herd。
_Avoid_: Pending claim, reservation
