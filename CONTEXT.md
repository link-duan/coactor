# CoActor Runtime

CoActor 为业务 key 提供按需运行的 Actor 执行边界。本词汇表同时定义已交付的执行与 ownership 语言，以及为后续持久化保留的术语；术语存在不表示对应能力已经交付。

## Language

**Actor**:
由 Actor Address 唯一标识的逻辑 Actor；无论它当前是否启动、位于哪个进程，其身份保持不变。
_Avoid_: Entity, Actor instance, Tokio task

**Actor Type**:
consumer 约定并在注册时显式传入 runtime 的稳定业务类型名称，例如 `room` 或 `document`；名称不从 Rust 类型名推导、不由 macro 生成，并在一个 runtime 内唯一。Client 与 Server 两侧 consumer 通过该名称约定相互寻址。
_Avoid_: Rust type name, macro 属性

**Actor ID**:
由业务提供、在同一 Actor Type 内唯一的稳定字节包装，例如 room ID 或 document ID；Actor Type 的 `new` 接收该统一类型，业务解释由 consumer 负责。
_Avoid_: Actor Key, Task ID, memory address

**Actor Address**:
Actor Type 与 Actor ID 的稳定组合，是 runtime 用于寻址 Actor 的完整身份，可跨进程和节点重新构造。
_Avoid_: Actor ID, Entity ID, local handle

**Actor Ref**:
Client 按 Actor Type 名称 + Actor ID 返回的通用稳定地址句柄（非泛型，不携带 AppState 类型参数）；它通过 `open()` 建立与 Actor 的 Session。它只弱引用 runtime，持有或 clone 不会保持 runtime/Actor 存活，也不会阻止 passivation。
_Avoid_: Actor Address, untyped byte channel

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
Actor 通过 Session Handle 推送给 caller 的出站消息，字节负载；Actor 可随时推送，不要求对应一次 Action。它不证明对应 mutation 已持久化，crash 后可能丢失。
_Avoid_: Reply, response, Durable ACK

**Session**:
caller 与 Actor 之间的一次持久双向通道；所有 Action 都经 Session 入站，Event 经 Session 出站。它由 Actor Ref 的 `open()` 建立；物理路径可经网关节点中继到 owner，owner 或网关失效都会显式打断它，由 caller 重新 `open()` 恢复。
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
ownership CAS 产生的单调 Owner 世代；它区分同一 Actor Address 的先后授权，consumer 不感知该值。未来持久化可将其作为 fencing 与恢复边界。
_Avoid_: Process generation, activation number

**Owner**:
当前被授权为某个 Actor Address 处理 Action 的 runtime 节点；授权必须与 Ownership Epoch 一起判断。
_Avoid_: Active Actor location, registry entry

**Fencing**:
阻止 stale Owner 继续服务，或阻止其数据成为当前逻辑提交状态的安全机制组合。
_Avoid_: Heartbeat alone, process shutdown alone

**Self-Fence**:
节点无法在时限内证明自己仍有权威时，主动停止 mutation 和成功响应的状态转换。
_Avoid_: Graceful shutdown, lease renewal

**Durable ACK Gate**:
在持久化证明覆盖本次 mutation 之前扣留 durable success response 的边界。CoActor 当前不提供 durable ACK，因此普通 Event 不经过该边界。
_Avoid_: Event, enqueue ACK



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
_Avoid_: Action handler, arbitrary callback

**Message Context**:
runtime 为一次 Action 处理提供的最小只读运行环境；它标识当前 Actor，并携带当前 Session 的出站句柄，供 Actor 推送 Event。它不是 Actor 的业务状态，也不提供跨 Actor 调用。
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
Actor Store 每次成功提交本地 KV 写事务后由 runtime/KV 层递增的状态版本；它不对应 Action 或单个 key。
_Avoid_: Action sequence, ownership epoch

**Persistence Fault**:
Actor Store 在有界重试后仍无法持久化的单 Actor 停止服务状态；Active Actor 保留本地文件并拒绝所有新 Action，等待恢复或运维处置。
_Avoid_: Ownership loss, node-wide fence

**Restore Fault**:
选定的历史 state object 存在但无法通过完整性、格式或 KV 文件校验时的 activation 失败状态；runtime 不自动回退到更早状态。
_Avoid_: Empty state, first activation

**Ownership Authority**:
由 runtime 掌控、负责竞争、续持和释放 ownership，并产生单调 Ownership Epoch 的共享权威。它是集群正确性边界，不是 consumer 可替换的插件 API。
_Avoid_: Ownership Provider, Actor registry, service discovery

**Node**:
嵌入 CoActor runtime 并通过网络接收或转发 Action 的 consumer 进程运行位置；一个 Node 的长期运维身份与单次运行会话必须区分。
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

**Load Ratio**:
Node 当前负载的排序指标，由当前活跃 Actor 数与上限之比（active/max）表示，用于 placement 决策的相对比较；它区分节点容量差异，避免以绝对数量或剩余容量误导决策。
_Avoid_: active actor count alone, remaining capacity

**Actor Owner Record**:
共享 ownership authority 中把一个 Actor Address 绑定到 Node Session 与 Ownership Epoch 的记录；它决定路由和 takeover，不表示 Active Actor 当前一定驻留内存。
_Avoid_: Node Lease, local route, service discovery entry

**Availability Failover**:
旧 Owner 失效后，由新 Owner 以更高 Ownership Epoch 从空 CoActor 状态重新提供服务；consumer 可自行重建外部状态，但 CoActor 不保证状态恢复。
_Avoid_: Recovery, Migration, restart

**Ownership Fault**:
runtime 无法取得、确认或继续持有服务权时产生的分布式 lifecycle failure；它不得被当作普通业务错误或持久化故障。
_Avoid_: Actor Stopped, Persistence Fault, mailbox overload

**Consumer**:
通过 CoActor Rust library 注册 Actor Type、发送 Action、接收 Event 并提供 Actor 业务逻辑的宿主应用。
_Avoid_: Actor, runtime node

**Server**:
embed CoActor runtime 并宿主 Actor 的一侧；它注册 Actor Type、维护 ownership 与生命周期、accept 入站传输并负责网关转发与放置，是唯一访问 ownership authority 写入的一方。Server 不提供 caller 能力。
_Avoid_: Runtime, Node, Host

**Client**:
embed CoActor runtime 但只发起 Session 的调用方；它通过 Service Discovery 发现 Server 节点、维护连接池，经网关节点中继 Action/Event。Client 不宿主 Actor，不持有 authority 访问。
_Avoid_: Runtime, caller handle

**Service Discovery**:
Client 获取候选 Server 节点列表的公开机制，返回 `Vec<Endpoint>` 供连接池建池；内置 `dns`（DNS 名多 A 记录，覆盖 K8s headless service）与 `static-list` 两个实现。它不是正确性边界，发现错误可重发现恢复。
_Avoid_: Actor Owner Record, registry entry

**Placement**:
网关对未拥有或 stale 的 Actor 决定 Owner 节点的决策过程；基于候选节点的 Load Ratio 快照与随机化（p2c），一次性决策经 ownership claim 固定，直到 failover。它不持续再平衡。
_Avoid_: Load balancing, routing

**Placement Burst**:
短时间内大量未拥有 Actor 同时需要放置决策（例如直播开播时上千新 room 同时 open）；它是放置倾斜的主要触发场景，由 p2c 随机化与 In-flight Placement 记账共同缓解。
_Avoid_: Traffic spike, hot start

**In-flight Placement**:
网关已决定放置到某节点、但该节点 Lease 尚未反映的本地记账计数；它参与预测 Load Ratio 计算，防止同一网关内并发放置的 herd。
_Avoid_: Pending claim, reservation
