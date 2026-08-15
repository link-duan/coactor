# Distributed runtime semantics

本文记录当前已交付的 distributed runtime 语义。它建立在 `runtime-semantics.md` 的 Actor 执行与生命周期语义之上，并补充集群 authority、路由、传输和 stateless Availability Failover 边界。

## Product boundary

- CoActor 保持嵌入 consumer 进程的 Rust library，不要求独立 control plane。
- consumer 必须显式选择 local 或 cluster mode；cluster mode 必须完整配置 distributed dependencies，不能静默退化为 local mode。
- caller 继续只按 Actor Address 使用通用 `ActorRef`（按名字索引）；Session 的 Action/Event 对 Node location 透明。
- 消息为字节负载，业务协议与 schema 兼容由 consumer 管理；Node 间使用 bidi gRPC stream（`Exchange(stream Envelope)`）多路复用，wire envelope 由 runtime 管理。
- 每条 Action 仍按消息执行 ownership 解析；Session 是逻辑路由标签，不绑定物理连接。

## Node authority

- Node ID 用于运维定位；每次 runtime 启动产生唯一 Node Session ID，后者才是 ownership authority 中的实际进程身份。
- Node Lease 包含 session、advertised endpoint、protocol version、expiry 与 advisory capacity sample。
- 默认 lease TTL 为 10 秒，每 TTL/3 续租；ownership 单操作默认 15 秒 deadline，peer connect 默认 3 秒。
- absolute expiry 使用本地 wall clock，本地 self-fence 使用同次操作锚定的 monotonic deadline；部署必须维持正常 NTP，CoActor 不保证任意 clock skew 下没有短暂 Owner overlap。
- Owner runtime 在本地 Action admission 与 Event 发送前检查 Node authority；lease renewal task 在 authority 失效后 fence runtime 并终止 Active Actor tasks。consumer side effect 不获得逐次 mutation fencing。
- Node Lease 失效后，runtime fence 本节点的 Active Actor execution：本地 admission 被拒绝、全部 Active Actor 停止、Session 终止，并通过可 await 的 supervision boundary 报告 termination；library 不退出宿主进程。当前 ingress 对 foreign Owner 的纯转发不属于该 fail-stop 保证，consumer 应在收到 supervision 结果后停止使用该 Node。

## Actor ownership and routing

- 每个 Actor Address 有一个保留历史 epoch 的 Actor Owner Record，内容包括 owned/unowned 状态、Node Session 与 Ownership Epoch。
- 首次 claim 从 epoch 1 开始；release 不删除记录；后续 claim 或 takeover 使用旧记录 ETag CAS 到 epoch + 1。
- 同一地址的并发冷调用合并为一次 owner resolution/acquisition。
- unowned Actor 只有在候选 Node 成功 reserve 本地 Active Actor capacity 后才尝试 claim。
- CAS conditional rejection 表示竞争者改变了 record，调用重新解析 Owner。
- CAS completion ambiguous 时必须 read-back 精确 Node Session 与 Ownership Epoch，最多协调三轮；不能把写超时直接当成失败并重新 claim。
- foreign Owner 的 Node Lease 明确缺失或过期时才允许 takeover；lease 读取失败不得推断 Owner 已死。
- incompatible runtime protocol、未注册 Actor Type 必须返回明确的远程协议错误。
- resolve 不做本地缓存：每次会话建立都新鲜读取 Owner Record 与 Node Lease（解析缓存已随每消息时代移除），杜绝陈旧所有权决策。

## Capacity and passivation

- Node Lease 发布 sampled-at、active/max 与 pressured/draining 等 advisory capacity；它不替代目标 Node 的本地 admission。
- 网关模型下 caller 的 `send()` 只返回传输投递状态；server 侧拒绝（`MailboxFull`、`ActorStopped` 等）经 `SessionError` 异步通知接收流，不终止 Session。
- idle passivation 仅在无存活 Session 时触发（与 local 语义一致）；先完成 deactivation 并彻底停止 Active Actor，再 CAS release Actor Owner Record 为 unowned；release 未确认前不得在别处重新激活。
- graceful shutdown 停止 admission、drain 已接纳消息、终止 Session、执行 shutdown deactivation，最后条件释放当前 Node Lease；Actor Owner Record 保留供更高 epoch takeover。

## Availability Failover

- Owner Node Lease 明确过期或缺失后，新 Node 可以用旧 Actor Owner Record ETag CAS 到 epoch + 1。
- 新 Owner 通过正常 constructor 与 activation lifecycle 从空 CoActor 状态启动。
- **failover 显式打断旧 Session**：旧 Owner 的 Session 记录随其消亡，caller 的接收流终止，必须重新 `open()` 才能在新 Owner 上建立会话。`on_session_opened` 不重放（ADR-0005）。
- 惰性检测：Owner 失效时网关（Server）在转发中感知（`notify_channel_closed` 清理指向该 Owner 的 relay 并回传 `SessionError`）；网关自身失效时 caller 的下一次 `send()` 在连接层返回可感知错误。旧 Owner 静默死亡且无后续活动时，终止可能延迟到下一次活动。
- consumer 可以在 activation 中读取自身数据库重建状态，但其一致性、幂等与 fencing 不属于 CoActor 保证。
- 该能力称为 Availability Failover，不称为 Recovery；本 slice 不提供 Actor Store、Restore Cut 或 durable state。

## Event and failure semantics

- Event 只表示 handler 本地完成且发送时仍通过本地 Node authority check；它不是 Durable ACK，也不证明状态能在 failover 后恢复。
- 每次 Event 发送不执行 S3 Actor Owner Record reread。
- consumer 外部数据库写、HTTP 调用和其他 side effect 不受 CoActor fencing；需要时由 consumer 使用 epoch、CAS、transaction 或 idempotency 控制。
- public API error 使用 `SendError` 的 `MailboxFull`、`ActivationFailed`、`ActorDeactivating`、`RuntimeAtCapacity`、`RuntimeShuttingDown`、`ActorStopped`、`RuntimeStopped`、`NodeFenced`、`RemoteUnavailable`、`OwnershipUnavailable` 与 `RemoteProtocol` 等结构化分类，不暴露 Tonic 或 AWS SDK 具体错误类型。
- caller 可以使用 consumer runtime 的普通 timeout 放弃等待；这不会撤销已被本地或远端 mailbox 接纳的 Action，本 slice 也不传播 per-call wire deadline。

## Verification boundary

- ownership protocol 通过 crate-private backend seam 做确定性测试；fake 可控制 ETag、CAS race、clock、response loss、ambiguous completion 与 lease expiry，该 seam 不是 consumer API。
- 最高层验收以多个真实 CoActor runtime、loopback bidi gRPC endpoint 与共享 ownership storage 运行跨节点 Session 消息。
- AWS adapter 使用本地 HTTP contract server 验证 conditional headers、ETag、错误分类与 SDK retry 行为。
- S3-compatible local endpoint 可用于集成验证，但兼容实现不替代真实 AWS qualification。
- real AWS qualification suite 默认不运行，是首次生产发布条件。在其通过前只能表述为"面向 AWS S3 语义设计"。
