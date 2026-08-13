# Proposed distributed runtime semantics

本文记录 Issue #8 对应的下一 distributed vertical slice。它是尚未实现的目标语义，不是当前版本已经提供的保证；当前实现仍以 `runtime-semantics.md` 为准。

## Product boundary

- CoActor 保持嵌入 consumer 进程的 Rust library，不要求独立 control plane。
- production runtime 必须显式配置 distributed dependencies；仅进程内执行保留为测试能力。
- caller 继续只按 Actor Address 使用 typed Actor Ref，Node location 对业务 API 透明。
- Node 间使用 Tonic gRPC 的通用 Invoke boundary；command payload、reply 与业务错误使用显式 Protobuf message。
- consumer 负责业务 Protobuf schema 与 method rename 的 rolling-upgrade 兼容。

## Node authority

- Node ID 用于运维定位；每次 runtime 启动产生唯一 Node Session ID，后者才是 ownership authority 中的实际进程身份。
- Node Lease 包含 session、advertised endpoint、protocol version、expiry 与 advisory capacity sample。
- 默认 lease TTL 为 10 秒，每 TTL/3 续租；ownership 单操作默认 15 秒 deadline，peer connect 默认 3 秒。
- absolute expiry 使用本地 wall clock，本地 self-fence 使用同次操作锚定的 monotonic deadline；部署必须维持正常 NTP，CoActor 不保证任意 clock skew 下没有短暂 Owner overlap。
- runtime 在 command admission、mutation continuation 与成功 reply 前检查本地 node authority。
- Node Lease 失效后 runtime-wide fail-stop：拒绝新调用、失败未完成调用、停止全部 Active Actor，并通过可 await 的 supervision boundary 报告 fenced termination；library 不退出宿主进程。

## Actor ownership and routing

- 每个 Actor Address 有一个保留历史 epoch 的 Actor Owner Record，内容包括 owned/unowned 状态、Node Session 与 Ownership Epoch。
- 首次 claim 从 epoch 1 开始；release 不删除记录；后续 claim 或 takeover 使用旧记录 ETag CAS 到 epoch + 1。
- 同一地址的并发冷调用合并为一次 owner resolution/acquisition。
- unowned Actor 只有在候选 Node 成功 reserve 本地 Active Actor capacity 后才尝试 claim。
- CAS conditional rejection 表示竞争者改变了 record，调用重新解析 Owner。
- CAS completion ambiguous 时必须 read-back 精确 Node Session 与 Ownership Epoch，最多协调三轮；不能把写超时直接当成失败并重新 claim。
- foreign Owner 的 Node Lease 明确缺失或过期时才允许 takeover；lease 读取失败不得推断 Owner 已死。
- never-connected 与明确 remote-not-owner 可以各安全刷新路由一次；请求可能已到达 peer 后的失败不自动重放，并返回结构化 outcome-unknown error。
- incompatible runtime protocol、未注册 Actor Type 或未知 command 必须返回明确的远程协议错误。

## Capacity and passivation

- Node Lease 发布 sampled-at、active/max 与 pressured/draining 等 advisory capacity；它不替代目标 Node 的本地 admission。
- ingress 本地满时确定性选择一个 live、non-pressured candidate；目标拒绝后最多改选一次，仍失败则返回 runtime capacity error。
- remote Owner 的 mailbox 满载仍返回与本地一致的 mailbox overload error，transport 不建立额外 command queue。
- idle passivation 先完成 deactivation 并彻底停止 Active Actor，再 CAS release Actor Owner Record 为 unowned；release 未确认前不得在别处重新激活。
- graceful shutdown 停止 admission、drain 已接纳 command、执行 shutdown deactivation，最后条件释放当前 Node Lease；Actor Owner Record 保留供更高 epoch takeover。

## Availability Failover

- Owner Node Lease 明确过期或缺失后，新 Node 可以用旧 Actor Owner Record ETag CAS 到 epoch + 1。
- 新 Owner 通过正常 constructor 与 activation lifecycle 从空 CoActor 状态启动。
- consumer 可以在 activation 中读取自身数据库重建状态，但其一致性、幂等与 fencing 不属于 CoActor 保证。
- 该能力称为 Availability Failover，不称为 Recovery；本 slice 不提供 Actor Store、Restore Cut 或 durable state。

## Reply and failure semantics

- Handler Reply 只表示 handler 本地完成且 reply 时仍通过本地 Node authority check；它不是 Durable ACK，也不证明状态能在 failover 后恢复。
- 每次 reply 不执行 S3 Actor Owner Record reread。
- consumer 外部数据库写、HTTP 调用和其他 side effect 不受 CoActor fencing；需要时由 consumer 使用 epoch、CAS、transaction 或 idempotency 控制。
- public call error 保持结构化分类，覆盖 ownership unavailable、node fenced、remote unavailable、remote protocol、runtime capacity 与 outcome unknown，但不暴露 Tonic 或 AWS SDK 具体错误类型；具体 Rust 命名后续决定。
- caller 可以使用 consumer runtime 的普通 timeout 放弃等待；这不会撤销已被本地或远端 mailbox 接纳的 command，本 slice 也不传播 per-call wire deadline。

## Verification boundary

- ownership protocol 以一个可替换的 high-level storage seam 做确定性测试；fake 可控制 ETag、CAS race、clock、response loss、ambiguous completion 与 lease expiry。
- 最高层验收以多个真实 CoActor runtime、loopback gRPC endpoint 与共享 ownership storage 实现运行 typed Actor Ref 调用。
- AWS adapter 使用本地 HTTP contract server 验证 conditional headers、ETag、错误分类与 SDK retry 行为。
- LocalStack 是 CI 必选的对象存储集成环境；MinIO 只可作为可选 smoke test，不属于支持保证。
- real AWS qualification suite 默认不运行，是首次生产发布条件。在其通过前只能表述为“面向 AWS S3 语义设计”。
