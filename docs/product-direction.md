# Product direction

本文记录方案访谈中已经确认的目标，以及仍需继续决策的方向。未确认事项不得被实现默认值替代。

## Confirmed

- CoActor 必须提供 Rust library。
- consumer 通过该 library 注册 Actor Type、接收命令并提供 Actor 业务逻辑。
- 首期方案不能依赖一个尚未决定的独立 platform、service 或 binary 才能使用。
- 核心 library 不假定 consumer 的负载类型；room 与 document 是验证场景，不是硬编码的运行模式。
- 只有确实跨负载成立的 identity、注册、寻址、串行执行和生命周期概念才能进入共同核心。
- CoActor 自己定义并掌控 Actor 语义、生命周期、路由以及 ownership/fencing 正确性边界。
- 共识算法、数据库和通用网络传输优先复用成熟基础设施，通过明确接口接入。
- 外部基础设施的能力不能未经验证就表述为 CoActor 提供的保证。
- 跨节点 ownership、故障恢复和迁移是 CoActor 的长期核心能力。
- 这些能力按单机生命周期、durable recovery、双节点 ownership/fencing、迁移逐步验证，不要求首个切片同时完成。
- 单机 Active Actor registry、进程存活和 handler reply 不得表述为全局 ownership、durable delivery 或 durable ACK。

## Post-MVP candidate design

以下内容是已经讨论并接受的后续方向，但因首版已明确排除状态存储和分布式能力，仍允许在真正实施前根据验证结果调整。它们不是首版 API、实现要求或保证。

- CoActor 提供 Actor 可使用的 Actor-scoped KV API，不规定 consumer 在 KV 中保存 journal、snapshot 或其他业务数据模型。
- consumer 定义业务状态、持久记录 schema 与序列化；后续首个 persistence slice 暂不提供 durable ACK 或 `ensure_durable()`。
- persistence API 对 consumer 隐藏 fencing 细节；runtime 自动把当前 Actor Address 与 Ownership Epoch 绑定到每次持久写。
- storage adapter 必须保证旧 Ownership Epoch 的数据不能覆盖或进入当前逻辑提交状态；对象存储可以留下不可见的 orphan object。
- consumer 绕过 CoActor Persistence API 的外部写不属于 CoActor 的 fencing 正确性保证范围。
- Persistence API 的底层持久化模型面向对象存储。
- consumer 只看到 Actor-scoped KV，不传入 Actor Address，也不接触物理 bucket、object key 或 storage address；runtime 负责命名空间映射。
- ownership provider 必须可替换；项目提供 S3 ownership provider 并将其作为默认实现方向。
- 默认 S3 ownership provider 以 S3 conditional write/CAS 作为 ownership 与 epoch 分配的权威协调点，不依赖额外 coordinator。
- S3 默认方案采用组合安全模型：ownership CAS、node/owner self-fence、epoch-isolated Actor Store、异步持久化与明确定义的 Restore Cut。Durable ACK Gate 保留为更后续能力，普通 Handler Reply 不经过它。
- 后续首个 distributed slice 按 Actor Address 独立管理 ownership；每个 Actor 在 S3 上对应一个 ownership object。
- 选择 Per-Actor ownership 是为了简化首个协议并支持单 Actor 独立迁移，暂时接受 ownership 对象和续租请求随 Active Actor 数量增长的成本。
- ownership object 只保存 owner、epoch、lease 等控制信息；Actor Store 使用独立 state object。
- 每个 Active Actor 使用本地嵌入式 KV 引擎操作一个本地单文件 Actor Store。
- runtime 将 Actor Store 以单个 durable state object 持久化到对象存储，允许覆盖该 state object；不为每个逻辑 KV key 创建独立 S3 object。
- state object 按 Ownership Epoch 隔离，概念布局为 `actors/<address>/epochs/<epoch>/state`；旧 Owner 的迟到 PUT 只能影响其旧 epoch namespace。
- state object 的最小 runtime metadata 只有 `format_version`、`revision` 与 `checksum`。Actor Address 和 Ownership Epoch 已由 object key 表达，不在文件中重复保存；KV 引擎兼容信息合并进 `format_version`。
- Actor Store revision 在每次成功提交的本地 KV 写事务后递增一次；一个事务可以包含多个 put/delete，失败、回滚和只读事务不递增。一个 command 可以产生零个、一个或多个 revision，consumer 不直接修改 revision。
- 同一 Active Actor 同时最多有一个 state `PutObject` 在途；后台上传严格串行。
- 后续首个 persistence slice 只提供可配置的 `max_dirty_age` 策略。计时从 Actor Store 第一次由 clean 变 dirty 开始，后续 mutation 不重置计时。
- `max_dirty_age` 具有 runtime 全局默认值，Actor Type 注册时可以覆盖；不支持按 Actor ID 单独配置。
- 每次 checkpoint 固定一个 Actor Store revision。上传成功只证明该 revision 已持久化；如果上传期间当前 revision 已前进，Store 仍为 dirty，下一轮 `max_dirty_age` 从 checkpoint 固定后发生的第一次新 mutation 开始计算。
- Handler Reply 在本地 KV mutation 完成后即可返回，不等待 S3；因此 reply 不是 durable ACK，crash 可能丢失最近已回复的修改，该 persistence slice 接受非零 RPO。
- 计划内 passivation、migration 或 shutdown 必须在退出前成功持久化 dirty Actor Store；失败时取消或延迟退出。
- state PUT 使用普通重试，不使用 ETag CAS。同一 epoch 的迟到重试可能覆盖更新 object，因此远端 revision 不保证单调，`max_dirty_age` 也不是严格 RPO 上限。
- state PUT 在有界重试后仍失败时，Active Actor 进入 Persistence Fault：不再执行 handler，拒绝所有新 command，保留 mailbox 中已有 command 与本地 Actor Store，但不影响同节点其他 Actor。该 slice 不推断 command 是否只读；未来如需故障期间查询，再设计显式 read-only API。
- stale Owner 的迟到上传不得覆盖新 epoch 的 Actor Store，也不得改变已经固定的 Restore Cut。
- 节点或 Active Actor 无法及时证明权威时必须 self-fence，停止 mutation、持久化与尚未证明 durable 的成功响应。
- 普通 Handler Reply 不表示 durable；首个 persistence slice 不提供可由 consumer 请求的 Durable ACK。
- 新 Owner 获得 epoch 后，从选定来源 epoch 第一次成功 GET 返回的 `{source_epoch, embedded_revision, ETag}` 被固定为 Restore Cut；恢复期间不重新读取该旧 epoch object。
- Restore 来源不要求 epoch 连续：runtime 通过对象存储 LIST，选择严格小于新 epoch且存在 state object 的最大 epoch。
- 如果完全不存在历史 state object，runtime 直接恢复为空 Actor Store，不要求 consumer 处理 `NoPreviousState`；该语义无法区分首次创建、首次 checkpoint 前崩溃和历史对象被删除。
- 如果选定的最大历史 state object 存在但未通过 checksum、格式或 KV 文件校验，activation 进入 Restore Fault；runtime 不自动回退到更早 epoch，以免把损坏静默转换成状态丢失。
- 新 Owner 恢复后不需要先把完整 Actor Store 上传为新 epoch baseline，即可开始处理消息。
- 本地 KV 引擎负责 KV 索引、事务与 batch 原子性；对象存储层不再把每个 KV mutation 表示为独立 commit object。
- CoActor 不把 Active Actor 内存当作唯一状态真源，也不强制所有负载采用同一种业务状态模型。

## First version scope

- 首个版本不引入状态存储、Actor-scoped KV、journal、snapshot、checkpoint 或 restore。
- 首个版本不实现 distributed ownership、Ownership Epoch、fencing、故障转移或 migration；这些能力与状态恢复在后续阶段整体推进。
- Active Actor 的业务状态只存在于当前进程内；passivation、异常退出或进程重启后，同一 Actor 通过 `new(actor_id)` 从空状态重新启动。
- Handler Reply 只表示内存中的 command handling 完成，不具有 durable 含义。
- 已讨论的 Actor Store、S3 state object、Restore Cut、Persistence Fault 与 `max_dirty_age` 均保留为后续版本候选设计，不属于首版实现或保证。
- 首版 API 不应提前暴露虚假的 persistence abstraction，也不要求 consumer 实现尚未使用的 restore lifecycle。
- 首版保留异步 lifecycle methods：`on_activate` 完成后才处理 command；`on_deactivate(reason)` 用于 idle passivation 与 graceful shutdown。它们可供 consumer 初始化和清理外部资源，但不承担 runtime 状态存储或 restore；panic、异常退出和进程 crash 不保证调用 `on_deactivate`。
- activation lifecycle 返回错误时，Active Actor 不进入服务态；当前 mailbox 中所有 command 返回 `ActivationFailed`，runtime 移除本地路由，后续新 command 可以重新触发 activation。由于 activation 从未成功，不调用 deactivation lifecycle。
- 首版不提供 activation lifecycle timeout。若 activation 永久阻塞，该 Actor Address 会持续处于 activating 状态，已接纳 command 继续等待，只能由 runtime shutdown、task cancellation 或进程退出解除。
- deactivation lifecycle 一旦开始便不可撤销；Active Actor 进入 `Deactivating`，本地路由保留到 lifecycle 结束以阻止第二个实例启动，期间新 command 立即返回可重试的 `ActorDeactivating`。结束后移除路由，后续 command 可以重新启动 Actor。
- deactivation lifecycle 不向 runtime 返回业务错误；consumer 必须在方法内部处理、记录或重试清理失败。runtime 只等待 lifecycle 结束，随后移除路由。
- deactivation lifecycle 受可配置 timeout 约束。超时前 Actor 保持 `Deactivating`；超时后 runtime 取消等待、记录 `DeactivationTimedOut`、强制结束实例并移除路由。consumer 的清理逻辑必须可取消，且不能依赖它一定完成。
- command handler panic 采用 fail-stop：当前 Active Actor 立即终止，当前及 mailbox 中已接纳但未处理的 command 返回 `ActorStopped`，不调用 deactivation lifecycle，runtime 移除路由。后续 command 创建新的空状态实例；首版不自动重放或原地重启。
- command handler 正常返回业务错误时，仅当前 command 返回错误；Active Actor 保持运行并继续处理 mailbox。consumer 负责保证返回错误后的内存状态仍然有效。
- graceful shutdown 开始后，runtime 停止接纳新 command 并返回 `RuntimeShuttingDown`；正在执行及已进入 mailbox 的 command 继续处理。mailbox drain 完成后调用 deactivation lifecycle，reason 为 shutdown。整个过程受全局 shutdown timeout 约束，超时后终止剩余 Active Actor，未完成 command 返回 `ActorStopped`。
- consumer 必须显式调用并 await graceful shutdown；这是 runtime 的使用契约。直接 drop 最后一个 runtime handle 属于误用，CoActor 不承诺 command drain、deactivation 或错误返回语义，首版实现可以直接取消 task、记录错误或 panic。这里的“不承诺”不是 Rust `unsafe` 意义上的 Undefined Behavior。
- 首版 idle passivation 默认开启，不因缺少状态存储增加单独的启用开关；idle timeout 仍可配置。consumer 必须接受 passivation 后同一 Actor 从空状态重新启动。
- idle timeout 具有 runtime 默认值，Actor Type 注册时可以覆盖；同一 Actor Type 的所有 Actor ID 使用相同值，首版不支持逐 Actor ID 动态配置。
- 首版只提供 request-response command，不提供 fire-and-forget。每个 command 都必须返回业务结果或可观察的 runtime error；未来若增加 fire-and-forget，需要另行定义丢弃与错误观测语义。
- command 成功进入 mailbox 后，caller 超时、取消或 drop request future 不会撤回 command；Active Actor 仍按顺序执行，只丢弃无法投递的 reply。首版不提供 mailbox command cancellation。
- 所有 Actor Type 必须在 runtime 启动前注册；名称在一个 runtime 内唯一，重复注册导致构建失败。runtime 启动后 registry 冻结，首版不支持动态注册或热替换 factory、handler 与配置。
- 首版对外提供 method-shaped typed API：每种 command 表现为 Actor Ref 上的独立异步方法，并拥有独立参数与返回类型。Rust macro 生成内部消息类型、类型擦除、安全分发与 reply plumbing，consumer 不需要维护统一 command/reply enum。
- Actor method 的返回采用 Kameo 风格错误展平：handler 声明 `Result<T, E>` 时，caller 获得 `Result<T, SendError<E>>`，业务错误映射为 `SendError::HandlerError(E)`；不会形成 `Result<Result<T, E>, RuntimeError>`。不返回业务错误的方法使用 `Infallible` 作为 handler error 类型。
- 首版 `SendError` 不返还尚未执行 command 的原始方法参数；macro 生成的内部 request 类型保持隐藏。caller 如需重试，自行保留、克隆或重建参数。
- Actor method future 从开始到结束独占 Active Actor；即使在 `.await` 外部 IO 时也不处理下一条 command。首版不支持 Actor reentrancy，consumer 需要自行避免无限等待和循环 Actor 调用。
- Actor Ref 是稳定地址句柄，不是 Active Actor 的强引用。获取、持有或 clone Actor Ref 不启动 Actor，也不阻止 idle passivation；method 调用时才按 Actor Address 查找或懒启动实例。passivation 后旧 Actor Ref 仍有效，下一次调用会创建新的空状态 Active Actor。
- Actor Ref 只持有 runtime 的弱引用，不延长 runtime 生命周期。runtime 已关闭或最后一个强 handle 已释放后，已有 Ref 的 method 调用返回 `RuntimeStopped`。
- 首版使用 Rust `tracing` 产生结构化 spans/events，但 library 不安装全局 subscriber。至少记录 Actor Type、Actor ID、lifecycle 阶段与错误类别；不记录 command 参数或返回值。首版不提供 metrics registry、管理端口或自定义 observer trait。
- Actor Type 不注册自定义 factory；约定提供同步 `new(actor_id)` 构造初始内存状态，runtime 在 lazy activation 时直接调用。需要异步执行的初始化放入 activation lifecycle。
- `new` 统一接收 runtime 的 `ActorId` 稳定字节包装类型；consumer 自行解析为业务 ID。首版不设计泛型 typed ID、序列化 trait 或 macro 自动转换。
- runtime 持有 consumer shared state，并向 activation lifecycle、deactivation lifecycle 与 `#[command]` 方法提供 `ActorContext`。context 只访问当前 `ActorId`/`ActorAddress` 与共享应用依赖；它不替代 Actor 自身的可变业务状态。
- 首版 `ActorContext` 不提供 runtime handle、获取其他 Actor Ref 或跨 Actor 调用能力。Actor 间协调由 consumer 在 Actor runtime 外部完成，后续再单独设计调用环与失败语义。
- `ActorContext<'a, S>` 首版只借用 `&'a ActorAddress` 与 `&'a S`，公开 `actor_id()`、`actor_address()`、`state()`。不暴露 mailbox、generation、task handle、shutdown token、tracing span 或动态扩展 map；runtime 自行处理观测上下文。
- 首版不向 Actor 暴露自身 Actor Ref。非重入 method 若 await 自己的下一条 command 会死锁；Actor 内复用通过普通私有方法完成，延迟调用由外部 consumer 调度。
- 每个 CoActor runtime 绑定一个 consumer 定义的强类型 `AppState`，由全部 Actor Type 共用。consumer 自行把数据库 client、HTTP client 与配置组合进该类型；首版不使用 `TypeId -> Any` service locator，也不允许缺失依赖在运行时才暴露。
- `AppState` 必须满足 `Send + Sync + 'static`，因为同一实例会被多个 Active Actor task 并发借用。
- runtime builder 接收 `AppState` 值，runtime 内部以 `Arc<AppState>` 持有；`ActorContext::state()` 默认只暴露 `&AppState` 借用，不要求 consumer 自行管理外层 Arc，也避免每次 command clone 容器。
- consumer 的 activation、deactivation 与 `#[command]` 实现显式接收 `&ActorContext<AppState>`；macro 在生成的 Actor Ref method 中隐藏该参数，因此 caller 只传业务参数。context 不永久存入 Actor 实例。
- Actor macro attribute 不重复声明 AppState 类型。macro 从 command/lifecycle 方法中显式写出的 `ActorContext<AppState>` 参数提取类型，并校验同一 Actor Type 的所有方法使用同一种 AppState；注册到不同 state 类型的 runtime 时编译失败。
- Actor Type 稳定名称必须在 Actor macro attribute 中显式声明，例如 `#[coactor::actor(name = "room")]`；不从 Rust struct 名或 `type_name` 推导，缺失时编译失败。Rust 类型重命名不改变 Actor Address。
- mailbox capacity 具有 runtime 默认值，Actor Type 注册时可以覆盖；同一 Actor Type 的所有 Actor ID 使用同一容量，首版不支持逐 Actor ID 动态配置。
- Actor macro 只把显式标记 `#[command]` 的方法加入公共 Actor API；其他同步或异步方法保持 consumer 内部实现。macro 据此区分 constructor、lifecycle、command 与辅助方法。
- `#[command]` 只能标记显式 `pub async fn`，generated Actor Ref method 同为 public；macro 不隐式扩大私有方法的可见性。
- macro 为每个 Actor Type 生成专用 Ref 类型，例如 `RoomActorRef`，并把 command 生成为 inherent async methods；不要求 consumer 导入 extension trait。内部可以包装通用 erased ref。
- 首版 Active Actor 唯一性只在单个 CoActor runtime 实例内成立：同一 runtime 中同一 Actor Address 最多一个 Active Actor。多个 runtime 不共享进程级 registry，也不协调相同 Actor Address；consumer 不应把同一逻辑 Actor 分散到多个 runtime。
- 首版暂不决定 command 的稳定协议名称、版本号、wire schema 或序列化格式；generated 内部消息只服务进程内 Rust API，未来 remoting 必须另行设计，不能默认复用首版内部格式。
- 项目仍处于早期 API 探索阶段，暂不决定 `SendError` 是否标记 `#[non_exhaustive]`，也不为尚未稳定的公开 API 承诺向后兼容；在进入稳定发布前统一评估 enum 扩展策略。
- 首个 vertical slice 使用最小 `CounterActor` 示例，验证 macro method API、Actor Ref、按 ID lazy activation、单 Actor 串行、不同 ID 隔离、bounded mailbox、idle passivation 后空状态重启、lifecycle、panic 与 graceful shutdown；不引入 room tick 或 document operation 模型。
- runtime 提供可配置的 `max_active_actors`。达到上限后不再启动新 Actor，并返回 `RuntimeAtCapacity`；已有 Active Actor 继续接收 command，runtime 不主动驱逐实例。passivation 或停止释放容量，首版不提供 Actor Type quota。
- 首版不提供 Actor-scoped timer 或 tick。需要周期驱动时，由 consumer 的 Tokio task 定时调用 Actor Ref；runtime 不定义 timer 是否保持 Actor active、积压合并或 shutdown 语义。
- CoActor library 使用 consumer 当前的 Tokio runtime，只 spawn 自身 Actor tasks；不创建隐藏线程、专用 executor 或嵌套 Tokio runtime。consumer 显式调用并 await CoActor graceful shutdown。
- 首版沿用 Kameo 的线程安全边界：Actor 为 `Send + 'static`；command 参数、成功返回值及 handler future 为 `Send + 'static`；业务错误为 `Debug + Send + 'static`。不要求 Actor `Sync`，也不支持 `LocalSet` 或 `!Send` Actor。
- activation lifecycle 的具体 consumer error 只记录到日志/tracing；caller 收到无 payload 的 `SendError::ActivationFailed`。method error 类型不额外携带 lifecycle error 泛型。
- Actor macro 生成类型描述，但 consumer 必须通过 runtime builder 显式 `register::<ActorType>()`；首版不使用自动扫描或 link-time inventory。builder 在启动前验证重复 Actor Type 名称。
- consumer 侧调用 `runtime.actor_ref::<ActorType>(actor_id)` 时立即验证类型已注册；未注册返回 `ActorTypeNotRegistered`，不会创建延迟失败的 Ref，也不会启动 Actor。
- lazy activation 期间复用 Active Actor 的同一个 bounded mailbox；触发启动的首条 command 和后续 command 都计入容量，不建立额外等待队列。满载返回 `MailboxFull`；activation 成功后按接纳顺序处理，失败则全部返回 `ActivationFailed`。
- 首版不提供内置 call/reply timeout 或 per-call options。consumer 使用 `tokio::time::timeout` 控制等待；timeout 只放弃 reply，不取消已经进入 mailbox 的 command。
- activation 与 deactivation lifecycle 都是可选 hook；未实现时分别视为立即成功和立即完成。简单 Actor 只需提供 `new(actor_id)` 与 `#[command]` methods，runtime 的 lifecycle 顺序语义不变。
- lifecycle hook 使用固定可选方法名 `on_activate` 与 `on_deactivate`，不增加 `#[activate]`/`#[deactivate]` attribute。macro 校验 context、reason 与返回类型签名。
- 首版 `DeactivationReason` 只包含 `Idle` 与 `Shutdown`；不提前加入 migration、ownership loss 或 persistence fault 等尚未实现的原因。
- 首版不引入通用序列化字节协议。Actor Address 仍是底层稳定、可擦除的运行时身份，为后续网络路由保留边界。

## Open

- 后续是否提供独立部署的 platform、service 或 binary。
- 如果提供独立进程，哪些职责留在 consumer，哪些职责由独立进程承载。
- library 与未来独立组件之间采用进程内调用、网络协议还是两种模式并存。
- 不同负载如何提供 mailbox 满载、durability 和 ACK 策略，而不污染共同核心。
- 本地 KV 引擎需要满足的单文件、事务、checkpoint 与 crash-consistency 能力。
- checkpoint 期间是否允许继续 mutation，以及该能力如何受本地 KV 引擎约束。
- `max_dirty_age` 的默认值。
- node lease/self-fence 的 TTL、续租时序和故障状态机。
- 未来是否提供 Durable ACK、dedupe 与外部副作用协调能力。
