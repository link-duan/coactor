# Local runtime semantics

本文定义当前 local mode 的可观察语义，也是 cluster mode 复用的 Actor 执行与生命周期基础。未写在"当前保证"中的能力均不能从实现中推导出来。

## Identity and addressing

- 调用方以稳定的 `ActorType + ActorId` 构造 Actor Address 并寻址。
- `ActorType` 是显式稳定名称，不使用 Rust 类型名；`#[coactor::actor]` 只挂在 Actor struct 上，生成类型擦除 dispatch 实现，**不声明名称**。名称由 consumer 注册时显式传入（`register::<A>(name)`），Rust 类型重命名不会自动改变 Actor Address。
- Actor Type 只在 runtime 启动前注册，名称必须唯一；重复注册使 runtime 构建失败，启动后 registry 冻结。
- consumer 通过 runtime builder 显式 `register::<A>(name)`；macro 只生成注册所需类型描述，不执行自动发现。
- caller 通过 `Client::actor(name, actor_id)` 按名字索引获取通用 `ActorRef`（纯字符串 API，无类型参数、无本地注册表 eager 校验）；未注册名字的错误推迟到 `open()`。成功获取 Ref 仍不会启动 Active Actor。Server 不提供 caller 能力（ADR-0008）。
- `ActorId` 是调用方提供、在同一 Actor Type 内唯一的原始字节。
- `ActorAddress` 的稳定编码为 `u32 big-endian type length + type bytes + actor ID bytes`。
- `ActorRef` 只保存稳定寻址信息，不保持 Active Actor 存活。获取或 clone Ref 不会启动 Actor；`open()` 才执行 registry lookup 或 lazy activation。passivation 后已有 Ref 仍可复用。
- `ActorRef` 对 runtime 使用弱引用；它不会保持 runtime 资源存活。runtime 已停止或释放后，`open()` 返回 `RuntimeStopped`。
- `Session` 与 `SessionHandle` 是非泛型类型：消息为字节，类型参数不再携带信息。

## Session and messages

- Session 是 caller 与 Actor 之间的一次持久双向通道，由 `ActorRef::open()` 建立；所有入站消息（Action）都经过 Session，不提供无 Session 的单向 tell。
- `open()` 主动激活：`on_activate` 完成 → 注册 Session → `on_session_opened` 完成 → 返回。失败时同步返回对应错误（如 `ActivationFailed`、`MailboxFull`）并清理注册。
- `on_session_opened` 中可通过 ctx 立即推送（"连接即推送"）。
- Action 是入站消息，字节负载，fire-and-forget（at-most-once）。网关模型下 `Session::send` 同步返回**传输投递**状态（进入节点间通道后不再确认）；server 侧拒绝（如 `MailboxFull`、`ActorStopped`）经 `SessionError` 异步通知到接收流。
- Event 是出站消息，字节负载，Actor 可随时推送：`MessageContext::send` 定向当前 Session；`ActorRuntime::broadcast` 广播给全部存活 Session（尽力而为，单点失败跳过并记录日志）。
- 业务错误不属于 `SendError`，由 Event 表达；`SendError` 只表达 runtime 投递失败。
- 出站接收流 bounded；caller 不消费时投递被丢弃（尽力而为）。
- 同一 Session 的出站 Event 保序（Actor 串行执行 + FIFO 投递）。

## Active Actor and execution

- runtime 启动时没有每 Actor 常驻的 task。首次 `open()` 为对应 Actor 创建 Active Actor。
- Actor 通过约定的同步 `new(runtime: ActorRuntime<S>)` 构造实例；`ActorRuntime` 提供 `actor_id()`、`actor_address()`、`app_state()` 与 `broadcast()`，是未来扩展（session 查询、定向、timer）的挂载点。
- `new` 接收 runtime 句柄；Actor 自行保存完整 `Arc<AppState>` 或提取所需依赖。
- 每次 Action 处理由 runtime 创建一个只读 `MessageContext`，提供 `actor_id()`、`actor_address()`、`session()`（可 clone 的 `SessionHandle`）与 `send()`（定向当前 Session）。
- shared state 是每个 runtime 唯一的 consumer 强类型 `AppState`，所有已注册 Actor Type 使用同一实例；不提供按 `TypeId` 动态查找。
- `AppState` 必须为 `Send + Sync + 'static`。
- builder 接收 App State 所有权，runtime 内部使用 `Arc` 共享，并在每次 lazy activation 时 clone 给 Actor 构造器（经 `ActorRuntime::app_state`）。
- 每个 Active Actor 拥有一个 mailbox 和一份热状态。
- runtime await 异步 activation lifecycle，成功完成后才开始处理 mailbox；当前 lifecycle 不执行 runtime state restore。
- lifecycle hooks 为可选；缺少 activation hook 等同于立即成功，缺少 deactivation hook 等同于立即完成。
- hook 名称固定为 `on_activate(&mut self)`、`on_deactivate(&mut self, reason)`、`on_session_opened(&mut self, ctx)` 与 `on_session_closed(&mut self, ctx)`；前两者不接收 context。
- `on_deactivate` 的当前 reason 只有 `Idle` 与 `Shutdown`。
- 同一 Active Actor 一次只执行一个消息（Action 或 Session 控制消息）；不同 Actor 可以并发。
- 消息 future 的整个生命周期都占有 Actor 执行权，包括 `.await` 期间；下一条消息必须等待当前 future 完成，当前不支持 reentrancy。
- 多个并发发送者之间不承诺业务顺序。实际处理顺序是 mailbox 成功接纳的顺序。
- 发送的 Event 只表示本次进程内处理完成，不表示 journal、snapshot 或业务状态已经 durable commit。
- Action 一旦成功进入 mailbox，caller 取消等待不会取消 Action；Active Actor 仍按 mailbox 顺序执行。
- `Session::send` 不提供内置 timeout。consumer 可在外层使用 Tokio timeout。
- runtime 通过 `max_active_actors` 限制同时存在的 Active Actor 数量。达到上限时，新 activation 返回 `RuntimeAtCapacity`；已存在实例不受影响。实例 passivate 或停止后释放容量。

## Session lifecycle and termination

- `on_session_opened`/`on_session_closed` 作为 mailbox 控制消息与 Action 串行执行。
- 终止矩阵（caller 侧 `recv()` 的可观察结果）：
  - actor panic / fail-stop → `Some(Err(ActorStopped))`；
  - runtime shutdown → `Some(Err(RuntimeShuttingDown))`；
  - caller drop Session → 本地接收流正常结束，并异步通知 owner（`SessionClose`）；
  - failover / 无法通知的终止 → 接收流结束（`None`）或下一次 `send` 返回可感知的错误。
- passivation 或 actor 停止时，actor 状态中持有的 `SessionHandle` 随状态失效；对已终止 Session 的 `send` 返回错误。
- 对已终止 Session 的 `Action` 投递返回 `ActorStopped`（session 记录不存在）。

## Bounded mailbox

- 每个 Active Actor 的 mailbox 容量固定且大于零。
- capacity 使用 runtime 默认值，Actor Type 可以覆盖；同一 Actor Type 的全部 Actor ID 使用相同配置。
- 容量只计算排队中的消息，不包含正在执行的消息。
- 当前过载策略是立即拒绝：mailbox 满时经 `SessionError` 通知 caller（不终止 Session），不等待、不合并、不静默丢弃。
- 调用方只有在 enqueue 成功后才可认为 Action 已被进程内 runtime 接纳；进程崩溃仍可能丢失已接纳但未持久化的 Action。

## Idle passivation

- idle passivation 默认开启，idle timeout 使用 runtime 默认值并可由 Actor Type 覆盖；同一 Actor Type 的全部 Actor ID 使用相同值，当前不提供逐 Actor 配置或单独的 opt-in 开关。
- **passivation 仅在无存活 Session 时触发**：idle timeout 到期时若 Session 计数大于零，则不 passivate 并重置 idle 计时；计数归零后才按 idle 语义 passivate。
- Active Actor 仅在 handler 已结束、mailbox 为空、无存活 Session 且持续一个 idle timeout 没有新消息时尝试 passivate。
- passivation 与路由使用同一个 registry 临界区。退出前，Active Actor 必须确认 registry 仍指向自己的 generation 且 mailbox 仍为空，然后原子移除该路由。
- 如果消息先被旧 mailbox 接纳，passivation 取消并继续处理；如果旧 Active Actor 先移除路由，消息会创建新的 Active Actor。两种竞争结果都不得静默丢失消息。
- passivation 后再次 `open()` 同一 Actor Address 会产生新的 Active Actor。当前没有 restore，因此内存状态从 `new` 的初始值重新开始。
- idle passivation 与 graceful shutdown 会调用并 await 异步 deactivation lifecycle；panic、异常 task 退出或进程 crash 不保证调用。
- deactivation lifecycle 一旦开始便不再因新消息取消。Active Actor 进入 `Deactivating`，本地路由保留到 lifecycle 结束，期间新 Action 立即返回可重试的 `ActorDeactivating`；结束后移除路由。
- deactivation lifecycle 不返回可供 runtime 处理的错误；consumer 在方法内部消化清理错误。
- deactivation lifecycle 有可配置 timeout。超时后 runtime 记录 `DeactivationTimedOut`、取消等待、结束实例并移除路由。consumer 不能依赖清理逻辑一定完成。

## Failure semantics

- local mode 不提供状态存储、durable mailbox、journal、snapshot、checkpoint、restore、消息去重、监督恢复或跨进程投递。
- Active Actor 异常退出后，下一次 `open()` 可以替换已关闭的本地路由并重新启动；先前内存状态和已终止的 Session 没有恢复保证。
- 消息 handler panic 会立即终止整个 Active Actor；该 Actor 的全部存活 Session 终止（`ActorStopped`），不调用 deactivation lifecycle，并移除本地路由。后续 `open()` 从空状态创建新实例；当前不重放或原地重启。
- `on_session_opened`/`on_session_closed` panic 同样按 fail-stop 处理（终止 Actor 及其 Session）。
- idle passivation 后再次 `open()` 同一 Actor Address 也通过 `new(runtime)` 从空状态重新启动。
- 本文描述的 local mode 不提供 exactly-once、at-least-once、跨节点 ownership、故障转移或迁移保证；cluster mode 的附加保证见 `distributed-runtime-semantics.md`。

## Graceful shutdown

- shutdown 开始后，runtime 不再接纳新 Action 或 Session，并返回 `RuntimeShuttingDown`。
- 正在执行以及已成功进入 mailbox 的消息继续处理，直到各 mailbox drain 完成。
- drain 完成后，runtime 终止该 Actor 的全部存活 Session（`RuntimeShuttingDown`），再调用并等待 deactivation lifecycle，reason 为 shutdown。
- 整个 shutdown 受全局 timeout 约束；超时后终止剩余 Active Actor，未完成消息对应的 Session 终止。
- consumer 必须显式 await graceful shutdown。未调用 shutdown 而直接释放最后一个 runtime handle 属于 API 误用；runtime 不定义 drain、deactivation 或 pending 消息的结果，可以取消 tasks、记录错误或 panic，但不涉及 Rust memory-safety 意义上的 Undefined Behavior。

## Local-mode boundary

- 每个 CoActor runtime 实例独立维护 Actor Address 到 Active Actor 的唯一映射；同一 runtime 内同一地址最多一个 Active Actor。
- 同一进程可以创建多个 runtime，但它们不共享全局 registry，也不协调相同 Actor Address。
- local mode 没有 Node identity、placement、Ownership Authority、Ownership Epoch 或 self-fence；这些只由显式配置的 cluster mode 提供。
- 进程内唯一 Active Actor 不能被解释为跨节点 ownership 保证。
- 当前没有 Actor-scoped timer/tick；Actor 只由外部 Action 或 Session 驱动，consumer 可自行使用 Tokio task 定时调用 `Session::send` 或 `ActorRuntime::broadcast`。
- CoActor 必须运行在 consumer 提供的 Tokio runtime 中；library 不创建隐藏线程或嵌套 runtime，只 spawn Actor tasks。graceful shutdown 由 consumer 显式调用并 await。
- Actor、Action/Event 负载与 handler future 必须可跨 Tokio worker 发送并满足 `'static`；Actor 不要求 `Sync`，当前不支持 `!Send`/`LocalSet` 模式。`Actor` trait 方法使用原生 `async fn`，future 的 `Send` 由 `trait Actor<S>: Send` 的 supertrait 自动继承（见 ADR-0007）。

## Observability

- runtime 通过 `tracing` 产生结构化 spans/events，不安装全局 subscriber。
- event 至少包含 Actor Type、Actor ID、lifecycle 阶段与错误类别。
- runtime 不记录 Action/Event 负载。
- 当前不提供 metrics registry、管理端口或自定义 observer trait。

## Vertical slice

- 使用最小 `CounterActor` 验证按名字索引、`open()` 建立 Session、双向 Action/Event、不同 Actor ID 隔离、bounded mailbox、Session 存活阻止 passivation、lifecycle、panic 与 graceful shutdown。
- 示例不引入游戏 tick、room 业务规则、协同文档 operation、状态存储或分布式语义。

## Reserved boundaries

以下是后续版本的设计边界，不属于当前 API 或保证：

- Active Actor factory：未来在 Actor 开始处理 mailbox 前执行 snapshot + journal restore；
- 类型化消息：未来在 Action/Event 上引入 prost 类型化与序列化约束（ADR-0005 已记录当前字节边界）；
- durable commit：未来由 storage trait 接收 Actor Address 与 ownership epoch，并在成功提交后产生 durable ACK；
- planned handoff：未来必须定义旧 Owner 停止接纳、新 Owner 获取更高 epoch、排队位置与失败回退，并用双节点测试证明。
