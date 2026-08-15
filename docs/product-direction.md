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
- 跨节点 ownership/fencing 与 Availability Failover 已交付；durable Recovery 和 Migration 是后续核心能力。
- 这些能力按单机生命周期、双节点 ownership/fencing、durable Recovery、Migration 逐步验证。
- 单机 Active Actor registry、进程存活和 handler reply 不得表述为全局 ownership、durable delivery 或 durable ACK。
- distributed runtime 采用纯嵌入式 Node，不要求独立 CoActor control-plane、service 或 binary。
- cluster mode 以 typed Actor Ref 为入口；`#[coactor::command(remote)]` 对 Node location 透明，Node 间使用 gRPC 与 Protobuf payload，consumer 负责业务 schema 兼容。
- distributed ownership 采用 Node Lease 与 Actor Owner Record 两层模型：Node Lease 证明 Node Session 资格，Actor Owner Record 通过 CAS 绑定 Actor Address、Node Session 与 Ownership Epoch。
- cluster mode 的公开 API 绑定内置 S3 Ownership Authority，不向 consumer 暴露可替换 provider；crate-private backend seam 只服务协议隔离和 deterministic tests。
- 当前 distributed runtime 不提供 CoActor 状态持久化或 Recovery；Owner 失效后由新 Owner 从空状态启动，该能力称为 Availability Failover。
- Node self-fence 当前停止本节点的 Active Actor execution 与成功响应，但 CoActor 作为 library 不退出宿主进程；consumer 通过 runtime supervision 结果停止使用该 Node 并决定进程处置。foreign Owner 的 ingress 转发不属于当前 fail-stop 保证。
- 外部数据库写、HTTP 调用等 consumer side effect 不在 CoActor fencing 保证内。
- consumer 必须显式选择 local 或 cluster mode；cluster mode 必须完整配置 distributed dependencies，不能在配置缺失时静默退化为 local mode。

## Post-MVP candidate design

以下内容是已经讨论并接受的持久化后续方向，仍允许在真正实施前根据验证结果调整。它们不是当前 API、实现要求或保证。

- CoActor 提供 Actor 可使用的 Actor-scoped KV API，不规定 consumer 在 KV 中保存 journal、snapshot 或其他业务数据模型。
- consumer 定义业务状态、持久记录 schema 与序列化；后续首个 persistence slice 暂不提供 durable ACK 或 `ensure_durable()`。
- persistence API 对 consumer 隐藏 fencing 细节；runtime 自动把当前 Actor Address 与 Ownership Epoch 绑定到每次持久写。
- storage adapter 必须保证旧 Ownership Epoch 的数据不能覆盖或进入当前逻辑提交状态；对象存储可以留下不可见的 orphan object。
- consumer 绕过 CoActor Persistence API 的外部写不属于 CoActor 的 fencing 正确性保证范围。
- Persistence API 的底层持久化模型面向对象存储。
- consumer 只看到 Actor-scoped KV，不传入 Actor Address，也不接触物理 bucket、object key 或 storage address；runtime 负责命名空间映射。
- 未来持久化方案在当前 ownership CAS 与 node/owner self-fence 上增加 epoch-isolated Actor Store、异步持久化与明确定义的 Restore Cut。Durable ACK Gate 保留为更后续能力，普通 Handler Reply 不经过它。
- 选择 Per-Actor ownership 是为了简化协议并为单 Actor 独立迁移保留边界，暂时接受 ownership 对象以及 resolution/CAS 操作随 Actor 数量增长的成本；Node Lease 续租仍按 Node 计费。
- Actor Owner Record 只保存 owner 与 epoch；Node Lease 使用独立 Node object，未来 Actor Store 再使用独立 state object。
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

## Delivered distributed slice

- AWS S3 conditional write/CAS 是 Node Lease 与 Actor Owner Record 的内置 authority；协议与 adapter 通过 deterministic fake、AWS SDK HTTP contract tests 与 S3-compatible local endpoint 验证。
- 当前 cluster mode 按 Actor Address 独立管理 ownership；每个 Actor 在 S3 上对应一个 ownership object。
- 在真实 AWS qualification suite 通过前，项目只表述为“面向 AWS S3 语义设计”，不宣称已通过真实 AWS S3 验证；qualification 是首次生产发布条件，不是日常 CI 阻塞项。
- lease 时序采用 celld 的务实基线：默认 TTL 10 秒、约每 TTL/3 续租、ownership 单操作 15 秒、peer connect 3 秒；部署要求正常 NTP，不宣称容忍任意时钟偏差。
- Node ID 是运维标签；每次 runtime 启动必须产生唯一 Node Session ID，ownership authority 以 session 身份区分重叠运行的新旧进程。
- bind address 与 advertised address 分离；Kubernetes 可通过 Downward API 注入，裸机可显式配置，library 不自动猜测网卡、NAT 或 DNS。
- 同一 Actor Address 的并发冷调用合并为一次 owner resolution。CAS rejected 重新读取；ambiguous mutation 必须 read-back 精确 session/epoch，并有界协调。
- 只有能证明请求未被 handler 接纳的远程失败才自动重试；请求可能已上 wire 后的失败返回 outcome unknown，不自动重放。
- capacity sample 随 Node Lease 发布，只作 placement hint；目标 Node 必须重新做本地 admission，拒绝后最多改选一次。
- idle passivation 完成 Actor 停止后将 Actor Owner Record CAS 为 unowned 并保留 epoch；graceful shutdown drain 后释放 Node Lease，保留 Actor Owner Record 供更高 epoch takeover。
- Handler Reply 只证明 handler 本地完成且 Node 仍通过本地 authority check；它不是 Durable ACK，也不经过每次 reply 的 S3 owner reread。

## Current runtime scope

- 当前版本不引入状态存储、Actor-scoped KV、journal、snapshot、checkpoint 或 restore。
- local mode 不实现 distributed ownership、Ownership Epoch、fencing 或故障转移；cluster mode 已提供这些能力，但仍不提供 durable Recovery 或 Migration。
- Active Actor 的业务状态只存在于当前进程内；passivation、异常退出或进程重启后，同一 Actor 通过 `new(actor_id, Arc<AppState>)` 从空状态重新启动。
- Handler Reply 只表示内存中的 command handling 完成，不具有 durable 含义。
- 已讨论的 Actor Store、S3 state object、Restore Cut、Persistence Fault 与 `max_dirty_age` 均保留为后续版本候选设计，不属于当前实现或保证。
- 当前 API 不提前暴露虚假的 persistence abstraction，也不要求 consumer 实现尚未使用的 restore lifecycle。
- 当前保留异步 lifecycle methods：`on_activate` 完成后才处理 command；`on_deactivate(reason)` 用于 idle passivation 与 graceful shutdown。它们可供 consumer 初始化和清理外部资源，但不承担 runtime 状态存储或 restore；panic、异常退出和进程 crash 不保证调用 `on_deactivate`。
- activation lifecycle 返回错误时，Active Actor 不进入服务态；当前 mailbox 中所有 command 返回 `ActivationFailed`，runtime 移除本地路由，后续新 command 可以重新触发 activation。由于 activation 从未成功，不调用 deactivation lifecycle。
- 当前不提供 activation lifecycle timeout。若 activation 永久阻塞，该 Actor Address 会持续处于 activating 状态，已接纳 command 继续等待，只能由 runtime shutdown、task cancellation 或进程退出解除。
- deactivation lifecycle 一旦开始便不可撤销；Active Actor 进入 `Deactivating`，本地路由保留到 lifecycle 结束以阻止第二个实例启动，期间新 command 立即返回可重试的 `ActorDeactivating`。结束后移除路由，后续 command 可以重新启动 Actor。
- deactivation lifecycle 不向 runtime 返回业务错误；consumer 必须在方法内部处理、记录或重试清理失败。runtime 只等待 lifecycle 结束，随后移除路由。
- deactivation lifecycle 受可配置 timeout 约束。超时前 Actor 保持 `Deactivating`；超时后 runtime 取消等待、记录 `DeactivationTimedOut`、强制结束实例并移除路由。consumer 的清理逻辑必须可取消，且不能依赖它一定完成。
- command handler panic 采用 fail-stop：当前 Active Actor 立即终止，当前及 mailbox 中已接纳但未处理的 command 返回 `ActorStopped`，不调用 deactivation lifecycle，runtime 移除路由。后续 command 创建新的空状态实例；当前不自动重放或原地重启。
- command handler 正常返回业务错误时，仅当前 command 返回错误；Active Actor 保持运行并继续处理 mailbox。consumer 负责保证返回错误后的内存状态仍然有效。
- graceful shutdown 开始后，runtime 停止接纳新 command 并返回 `RuntimeShuttingDown`；正在执行及已进入 mailbox 的 command 继续处理。mailbox drain 完成后调用 deactivation lifecycle，reason 为 shutdown。整个过程受全局 shutdown timeout 约束，超时后终止剩余 Active Actor，未完成 command 返回 `ActorStopped`。
- consumer 必须显式调用并 await graceful shutdown；这是 runtime 的使用契约。直接 drop 最后一个 runtime handle 属于误用，CoActor 不承诺 command drain、deactivation 或错误返回语义，可以直接取消 task、记录错误或 panic。这里的“不承诺”不是 Rust `unsafe` 意义上的 Undefined Behavior。
- 当前 idle passivation 默认开启，不因缺少状态存储增加单独的启用开关；idle timeout 仍可配置。consumer 必须接受 passivation 后同一 Actor 从空状态重新启动。
- idle timeout 具有 runtime 默认值，Actor Type 注册时可以覆盖；同一 Actor Type 的所有 Actor ID 使用相同值，当前不支持逐 Actor ID 动态配置。
- 当前提供 Session 双向消息：入站 Action 与出站 Event 均为字节负载；Action 是 fire-and-forget（send 同步返回投递状态，进入 mailbox 后不确认，at-most-once），业务错误由 Event 表达。所有入站都经过 Session，不提供无 Session 的单向 tell。
- Action 成功进入 mailbox 后，caller 取消或 drop future 不会撤回消息；Active Actor 仍按顺序执行。当前不提供 mailbox message cancellation。
- 所有 Actor Type 必须在 runtime 启动前注册；名称在一个 runtime 内唯一，重复注册导致构建失败。runtime 启动后 registry 冻结，当前不支持动态注册或热替换 factory、handler 与配置。
- `#[coactor::actor]` macro 只挂在 Actor struct 上，生成类型擦除 dispatch 实现（不声明名称）；consumer 手写 `impl Actor<S>`（`new(ActorRuntime<S>)`、`on_message`、可选 lifecycle hooks）。Actor Type 名称不随 Rust 类型名推导。
- client 侧按名字索引：`runtime.actor("room", actor_id)` 返回通用 `ActorRef`（无类型参数），`open()` 建立持久双向 `Session`。
- Actor message future 从开始到结束独占 Active Actor；即使在 `.await` 外部 IO 时也不处理下一条消息。当前不支持 Actor reentrancy，consumer 需要自行避免无限等待和循环 Actor 调用。
- Actor Ref 是稳定地址句柄，不是 Active Actor 的强引用。获取、持有或 clone Actor Ref 不启动 Actor；`open()` 主动激活并注册 Session，`on_session_opened` 在激活完成后调用（可"连接即推送"）。
- Actor Ref 只持有 runtime 的弱引用，不延长 runtime 生命周期。runtime 已关闭后，已有 Session 的 `send` 返回 `RuntimeStopped`。
- 当前使用 Rust `tracing` 产生结构化 spans/events，但 library 不安装全局 subscriber。至少记录 Actor Type、Actor ID、lifecycle 阶段与错误类别；不记录 command 参数或返回值。当前不提供 metrics registry、管理端口或自定义 observer trait。
- Actor Type 不注册自定义 factory；约定提供同步 `new(actor_id, Arc<AppState>)` 构造初始内存状态，runtime 在 lazy activation 时直接调用。需要异步执行的初始化放入 activation lifecycle。
- `new` 接收 runtime 的 `ActorId` 与共享 App State；consumer 自行解析业务 ID，并选择保存完整 App State 或提取依赖。
- runtime 为每次 Action 处理创建 `MessageContext`，公开 `actor_id()`、`actor_address()`、`session()` 与 `send()`（定向当前 Session）。
- `CommandContext` 不实现 `Clone`，也不提供 runtime handle、自身 Actor Ref 或跨 Actor 调用能力。Actor 间协调由 consumer 在 runtime 外部完成。
- Actor 通过构造注入的 `ActorRuntime` 暴露自身身份与 `broadcast` 能力；Actor 无法 await 自己的下一条消息（非重入），延迟调用由外部 consumer 调度。
- 每个 CoActor runtime 绑定一个 consumer 定义的强类型 `AppState`，由全部 Actor Type 共用。consumer 自行把数据库 client、HTTP client 与配置组合进该类型；当前不使用 `TypeId -> Any` service locator，也不允许缺失依赖在运行时才暴露。
- `AppState` 必须满足 `Send + Sync + 'static`，因为同一实例会被多个 Active Actor task 并发借用。
- runtime builder 接收 `AppState` 值并以 `Arc<AppState>` 持有；lazy activation 时 clone 同一 Arc 给 Actor 构造器。
- lifecycle 与 `on_message` 接收 `&MessageContext`（标识当前 Actor + 当前 Session 出站句柄）；`ActorRuntime` 在构造时注入，AppState 通过 `runtime.app_state()` 获取。
- `impl Actor<S>` 的 S 与 `Runtime<S>` 的 AppState 一致时注册才编译通过。
- Actor Type 稳定名称由 consumer 在注册时显式传入（`register::<A>(name)`），例如 `register::<RoomActor>("room")`；不从 Rust struct 名或 `type_name` 推导。Client/Server 两侧按同一名称约定寻址。
- mailbox capacity 具有 runtime 默认值，Actor Type 注册时可以覆盖；同一 Actor Type 的所有 Actor ID 使用同一容量，当前不支持逐 Actor ID 动态配置。
- 所有 Action/Event 均携带 Session ID 路由；cluster mode 下消息经 ownership 解析投递到 Owner，Node 间使用 bidi gRPC stream 多路复用。跨节点 failover 会打断 Session，caller 需重新 `open()`。
- 当前消息为字节负载，业务协议（编码、schema 兼容）由 consumer 负责；后续阶段可能引入类型化 Action/Event 与序列化约束。
- 项目仍处于早期 API 探索阶段，暂不决定 `SendError` 是否标记 `#[non_exhaustive]`，也不为尚未稳定的公开 API 承诺向后兼容；在进入稳定发布前统一评估 enum 扩展策略。
- 最小 `CounterActor` vertical slice 验证 macro method API、Actor Ref、按 ID lazy activation、单 Actor 串行、不同 ID 隔离、bounded mailbox、idle passivation 后空状态重启、lifecycle、panic 与 graceful shutdown；不引入 room tick 或 document operation 模型。
- runtime 提供可配置的 `max_active_actors`。达到上限后不再启动新 Actor，并返回 `RuntimeAtCapacity`；已有 Active Actor 继续接收 command，runtime 不主动驱逐实例。passivation 或停止释放容量，当前不提供 Actor Type quota。
- 当前不提供 Actor-scoped timer 或 tick。需要周期驱动时，由 consumer 的 Tokio task 定时调用 Actor Ref；runtime 不定义 timer 是否保持 Actor active、积压合并或 shutdown 语义。
- CoActor library 使用 consumer 当前的 Tokio runtime，只 spawn 自身 Actor tasks；不创建隐藏线程、专用 executor 或嵌套 Tokio runtime。consumer 显式调用并 await CoActor graceful shutdown。
- Actor 为 `Send + 'static`；command 参数、成功返回值及 handler future 为 `Send + 'static`；业务错误为 `Debug + Send + 'static`。不要求 Actor `Sync`，也不支持 `LocalSet` 或 `!Send` Actor。
- activation lifecycle 的具体 consumer error 只记录到日志/tracing；caller 收到无 payload 的 `SendError::ActivationFailed`。method error 类型不额外携带 lifecycle error 泛型。
- Actor macro 生成类型描述，但 consumer 必须通过 runtime builder 显式 `register::<ActorType>()`；当前不使用自动扫描或 link-time inventory。builder 在启动前验证重复 Actor Type 名称。
- consumer 侧调用 `runtime.actor_ref::<ActorType>(actor_id)` 时立即验证类型已注册；未注册返回 `ActorTypeNotRegistered`，不会创建延迟失败的 Ref，也不会启动 Actor。
- lazy activation 期间复用 Active Actor 的同一个 bounded mailbox；触发启动的首条 command 和后续 command 都计入容量，不建立额外等待队列。满载返回 `MailboxFull`；activation 成功后按接纳顺序处理，失败则全部返回 `ActivationFailed`。
- 当前不提供内置 call/reply timeout 或 per-call options。consumer 使用 `tokio::time::timeout` 控制等待；timeout 只放弃 reply，不取消已经进入 mailbox 的 command。
- activation 与 deactivation lifecycle 都是可选 hook；未实现时分别视为立即成功和立即完成。简单 Actor 只需提供 `new(actor_id, Arc<AppState>)` 与 `#[command]` methods，runtime 的 lifecycle 顺序语义不变。
- lifecycle hook 使用固定可选方法名 `on_activate` 与 `on_deactivate`，不增加 `#[activate]`/`#[deactivate]` attribute。macro 校验 context、reason 与返回类型签名。
- `DeactivationReason` 当前只包含 `Idle` 与 `Shutdown`；不提前加入 migration、ownership loss 或 persistence fault 等尚未实现的原因。
- runtime 不要求所有 command 使用通用字节协议；只有 `#[command(remote)]` 进入 Protobuf/gRPC wire boundary。

## Open

- 是否提供独立部署的 platform、service 或 binary；当前 cluster mode 不依赖这些组件。
- 不同负载如何提供 mailbox 满载、durability 和 ACK 策略，而不污染共同核心。
- 本地 KV 引擎需要满足的单文件、事务、checkpoint 与 crash-consistency 能力。
- checkpoint 期间是否允许继续 mutation，以及该能力如何受本地 KV 引擎约束。
- `max_dirty_age` 的默认值。
- 未来是否提供 Durable ACK、dedupe 与外部副作用协调能力。
