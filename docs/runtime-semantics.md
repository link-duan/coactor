# Local runtime semantics

本文定义当前 local mode 的可观察语义，也是 cluster mode 复用的 Actor 执行与生命周期基础。未写在“当前保证”中的能力均不能从实现中推导出来。

## Identity and addressing

- 调用方以稳定的 `ActorType + ActorId` 构造 Actor Address 并寻址。
- `ActorType` 是显式稳定名称，不使用 Rust 类型名。
- Actor macro 强制 consumer 显式声明 Actor Type 名称；名称缺失时编译失败，Rust 类型重命名不会自动改变 Actor Address。
- Actor Type 只在 runtime 启动前注册，名称必须唯一；重复注册使 runtime 构建失败，启动后 registry 冻结。
- consumer 通过 runtime builder 显式 `register::<ActorType>()`；macro 只生成注册所需类型描述，不执行自动发现。
- consumer 获取 Actor Ref 时立即检查 Actor Type 注册状态；未注册返回 `ActorTypeNotRegistered`。成功获取 Ref 仍不会启动 Active Actor。
- `ActorId` 是调用方提供、在同一 Actor Type 内唯一的原始字节。
- `ActorAddress` 的稳定编码为 `u32 big-endian type length + type bytes + actor ID bytes`。
- 进程内 `HashMap` 可以使用随机 hash seed，但它只影响本地查找，不定义 Actor Address、分片或持久化 key。
- consumer 通过 Actor Ref 上由 macro 生成的类型安全异步方法发送 command；每个方法独立定义参数和返回类型。内部消息封装、分发和 reply channel 对 consumer 隐藏。local-only command 不做序列化，Actor Address 仍可作为底层稳定身份单独表示。
- 每个 Actor Type 有专用 generated Ref 类型，例如 `RoomActorRef`；command 是该类型的 inherent method，而非通用 ActorRef extension trait。
- 普通 `#[command]` method 只是进程内 Rust API；跨节点 command 的额外约束见 `distributed-runtime-semantics.md`。
- 只有 Actor impl 中显式标记 `#[command]` 的方法会生成 Actor Ref API；未标记的方法不参与消息分发。
- command 必须声明为 `pub async fn`；macro 保持其公共 API 意图，不允许私有方法通过 attribute 被隐式公开。
- Actor Ref 只保存稳定寻址信息，不保持 Active Actor 存活。获取或 clone Ref 不会启动 Actor；调用 method 才执行 registry lookup 或 lazy activation。passivation 后已有 Ref 仍可复用。
- Actor Ref 对 runtime 使用弱引用；它不会保持 runtime 资源存活。runtime 已停止或释放后，method 返回 `RuntimeStopped`。
- method 返回使用 Kameo 风格的统一 `SendError`：handler 的 `Result<T, E>` 被展平为 caller 可见的 `Result<T, SendError<E>>`，其中业务错误为 `HandlerError(E)`；无业务错误的方法使用 `Infallible`。
- `SendError` 不携带原始方法参数；被 mailbox 或 lifecycle 拒绝的调用如需重试，由 caller 自行保存参数。

## Active Actor and execution

- runtime 启动时没有每 Actor 常驻的 task。首条被接纳的命令为对应 Actor 创建 Active Actor。
- Actor Type 通过约定的同步 `new(actor_id, Arc<AppState>)` 构造实例，不提供独立 factory registration；异步初始化由 activation lifecycle 承担。
- `new` 接收 runtime `ActorId` 和共享 App State；Actor 自行保存完整 `Arc` 或提取所需依赖。
- 每个 command invocation 由 runtime 创建一个只读 `CommandContext`，当前仅提供 `actor_id()` 与 `actor_address()`。
- `CommandContext` 无生命周期参数、无 App State 泛型且不实现 `Clone`；它不包含 runtime handle、自身 Actor Ref 或跨 Actor 调用能力。
- shared state 是每个 runtime 唯一的 consumer 强类型 `AppState`，所有已注册 Actor Type 使用同一实例；不提供按 `TypeId` 动态查找。
- `AppState` 必须为 `Send + Sync + 'static`。
- builder 接收 App State 所有权，runtime 内部使用 `Arc` 共享，并在每次 lazy activation 时 clone 给 Actor 构造器。
- Actor command 显式接收 `&CommandContext`，generated Actor Ref API 不向 caller 暴露该参数；context 只在当前 command invocation 中有效。
- `#[coactor::actor]` 不包含单独的 state type 参数；App State 类型由构造器的 `Arc<AppState>` 参数决定。
- 每个 Active Actor 拥有一个 mailbox 和一份热状态。
- runtime await 异步 activation lifecycle，成功完成后才开始处理 mailbox；当前 lifecycle 不执行 runtime state restore。
- lifecycle hooks 为可选；缺少 activation hook 等同于立即成功，缺少 deactivation hook 等同于立即完成。
- hook 名称固定为 `on_activate(&mut self)` 与 `on_deactivate(&mut self, reason)`，由 Actor macro 识别并校验，不接收 context。
- `on_deactivate` 的当前 reason 只有 `Idle` 与 `Shutdown`。
- activation 期间到达的 command 继续进入同一个 bounded mailbox并计入容量；不创建额外队列。成功后按接纳顺序处理，满载返回 `MailboxFull`。
- activation lifecycle 返回错误时，当前 Active Actor 不进入服务态，mailbox 中全部 command 返回 `ActivationFailed`，对应本地路由被移除；后续 command 可以创建新的 Active Actor 并重试 activation。失败实例不调用 deactivation lifecycle。
- activation 的具体 consumer error 由 runtime 记录，但不向 caller 暴露；caller 统一收到无 payload 的 `SendError::ActivationFailed`。
- 当前不为 activation lifecycle 设置 timeout。永久阻塞的 activation 会持续占用该 Actor Address 的本地路由，已接纳 command 保持等待，直到 runtime shutdown、task cancellation 或进程退出。
- 同一 Active Actor 一次只执行一个 command handler；不同 Actor 可以并发。
- command handler 的整个 future 生命周期都占有 Actor 执行权，包括 `.await` 期间；下一条 command 必须等待当前 future 完成，当前不支持 reentrancy。
- 多个并发发送者之间不承诺业务顺序。实际处理顺序是 mailbox 成功接纳的顺序，业务仍需在后续 durable command 中携带 command ID 与业务序号。
- handler 完成后的 reply 只表示本次进程内处理完成，不表示 journal、snapshot 或业务状态已经 durable commit。
- 当前只支持 request-response command；每个 command 返回业务结果或 runtime error，不提供 fire-and-forget API。
- command 一旦成功进入 mailbox，caller 取消等待不会取消 command；Active Actor 仍按 mailbox 顺序执行，若 reply receiver 已消失则丢弃 reply。
- generated Actor method 不提供内置 reply timeout。consumer 可在外层使用 Tokio timeout，其语义等同于 caller 取消等待，不撤回 command。
- runtime 通过 `max_active_actors` 限制同时存在的 Active Actor 数量。达到上限时，新 Actor activation 返回 `RuntimeAtCapacity`；已存在实例不受影响，也不会为腾出容量被主动驱逐。实例 passivate 或停止后释放容量。

## Bounded mailbox

- 每个 Active Actor 的 mailbox 容量固定且大于零。
- capacity 使用 runtime 默认值，Actor Type 可以覆盖；同一 Actor Type 的全部 Actor ID 使用相同配置。
- 容量只计算排队中的命令，不包含正在执行的命令。
- 当前过载策略是立即拒绝：mailbox 满时返回 `MailboxFull`，不等待、不合并、不静默丢弃。
- 调用方只有在 enqueue 成功后才可认为命令已被进程内 runtime 接纳；进程崩溃仍可能丢失已接纳但未持久化的命令。

## Idle passivation

- idle passivation 默认开启，idle timeout 使用 runtime 默认值并可由 Actor Type 覆盖；同一 Actor Type 的全部 Actor ID 使用相同值，当前不提供逐 Actor 配置或单独的 opt-in 开关。
- Active Actor 仅在 handler 已结束、mailbox 为空且持续一个 idle timeout 没有新命令时尝试 passivate。
- passivation 与路由使用同一个 registry 临界区。退出前，Active Actor 必须确认 registry 仍指向自己的 generation 且 mailbox 仍为空，然后原子移除该路由。
- 如果命令先被旧 mailbox 接纳，passivation 取消并继续处理；如果旧 Active Actor 先移除路由，命令会创建新的 Active Actor。两种竞争结果都不得静默丢失命令。
- passivation 后再次访问同一 Actor Address 会产生新的 Active Actor。当前没有 restore，因此内存状态从 factory 的初始值重新开始。
- idle passivation 与 graceful shutdown 会调用并 await 异步 deactivation lifecycle；panic、异常 task 退出或进程 crash 不保证调用。
- deactivation lifecycle 一旦开始便不再因新 command 取消。Active Actor 进入 `Deactivating`，本地路由保留到 lifecycle 结束，期间新 command 立即返回可重试的 `ActorDeactivating`；结束后移除路由，下一条 command 才能创建新实例。
- deactivation lifecycle 不返回可供 runtime 处理的错误；consumer 在方法内部消化清理错误，runtime 在方法结束后正常移除路由。
- deactivation lifecycle 有可配置 timeout。超时后 runtime 记录 `DeactivationTimedOut`、取消等待、结束实例并移除路由；后续 command 可以创建新的 Active Actor。consumer 不能依赖清理逻辑一定完成。

## Failure semantics

- local mode 不提供状态存储、durable mailbox、journal、snapshot、checkpoint、restore、command 去重、监督恢复或跨进程投递。
- Active Actor 异常退出后，下一次发送可以替换已关闭的本地路由并重新启动；先前内存状态和未回复命令没有恢复保证。
- command handler panic 会立即终止整个 Active Actor；当前及 mailbox 中已接纳但未处理的 command 返回 `ActorStopped`，不调用 deactivation lifecycle，并移除本地路由。后续 command 从空状态创建新实例；当前不重放或原地重启。
- command handler 正常返回业务错误只结束当前 command；Active Actor 保持运行并继续消费 mailbox。runtime 不解释业务错误，也不据此重启或 passivate Actor。
- idle passivation 后再次访问同一 Actor Address 也通过 `new(actor_id, Arc<AppState>)` 从空状态重新启动。
- 本文描述的 local mode 不提供 exactly-once、at-least-once、跨节点 ownership、故障转移或迁移保证；cluster mode 的附加保证见 `distributed-runtime-semantics.md`。

## Graceful shutdown

- shutdown 开始后，runtime 不再接纳新 command，并返回 `RuntimeShuttingDown`。
- 正在执行以及已成功进入 mailbox 的 command 继续处理，直到各 mailbox drain 完成。
- drain 完成后，runtime 调用并等待 deactivation lifecycle，reason 为 shutdown。
- 整个 shutdown 受全局 timeout 约束；超时后终止剩余 Active Actor，尚未完成的 command 返回 `ActorStopped`。
- consumer 必须显式 await graceful shutdown。未调用 shutdown 而直接释放最后一个 runtime handle 属于 API 误用；runtime 不定义 drain、deactivation 或 pending reply 的结果，可以取消 tasks、记录错误或 panic，但不涉及 Rust memory-safety 意义上的 Undefined Behavior。

## Local-mode boundary

- 每个 CoActor runtime 实例独立维护 Actor Address 到 Active Actor 的唯一映射；同一 runtime 内同一地址最多一个 Active Actor。
- 同一进程可以创建多个 runtime，但它们不共享全局 registry，也不协调相同 Actor Address。
- local mode 没有 Node identity、placement、Ownership Authority、Ownership Epoch 或 self-fence；这些只由显式配置的 cluster mode 提供。
- 进程内唯一 Active Actor 不能被解释为跨节点 ownership 保证。
- 当前没有 Actor-scoped timer/tick；Actor 只由外部 method call 驱动，consumer 可自行使用 Tokio task 定时调用 Actor Ref。
- CoActor 必须运行在 consumer 提供的 Tokio runtime 中；library 不创建隐藏线程或嵌套 runtime，只 spawn Actor tasks。graceful shutdown 由 consumer 显式调用并 await。
- Actor、command 参数、reply 与 handler future 必须可跨 Tokio worker 发送并满足 `'static`；handler 业务错误还必须实现 `Debug`。Actor 不要求 `Sync`，当前不支持 `!Send`/`LocalSet` 模式。

## Observability

- runtime 通过 `tracing` 产生结构化 spans/events，不安装全局 subscriber。
- event 至少包含 Actor Type、Actor ID、lifecycle 阶段与错误类别。
- runtime 不记录 command 参数或返回值。
- 当前不提供 metrics registry、管理端口或自定义 observer trait。

## Vertical slice

- 使用最小 `CounterActor` 验证 generated method-shaped API、Actor Ref、按 ID lazy activation、单 Actor 串行、不同 Actor ID 隔离、bounded mailbox、idle passivation 后空状态重启、lifecycle、panic 与 graceful shutdown。
- 示例不引入游戏 tick、room 业务规则、协同文档 operation、状态存储或分布式语义。

## Reserved boundaries

以下是后续版本的设计边界，不属于当前 API 或保证：

- Active Actor factory：未来在 Actor 开始处理 mailbox 前执行 snapshot + journal restore；
- command envelope：未来加入 command ID、schema version 与 Ownership Epoch；
- durable commit：未来由 storage trait 接收 Actor Address 与 ownership epoch，并在成功提交后产生 durable ACK；
- planned handoff：未来必须定义旧 Owner 停止接纳、新 Owner 获取更高 epoch、排队位置与失败回退，并用双节点测试证明。
