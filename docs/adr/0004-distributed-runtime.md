---
status: accepted
---

# Distributed runtime with stateless Availability Failover

CoActor 的下一生产形态是嵌入 consumer 进程的 distributed runtime，而不是以本地 Actor library 为主要用途，也不要求独立 control plane。生产启动必须显式配置 Node identity、peer networking 与 ownership authority；仅进程内执行保留为明确的测试能力，不能在生产配置缺失时静默退化。library 运行在 consumer 的 Tokio runtime 中，Node self-fence 时停止自身服务并通过 supervision boundary 报告 termination，但不退出宿主进程。

Node 间 command 数据面使用 gRPC/Protobuf 的通用 Invoke boundary，typed Actor Ref 继续只按 Actor Address 调用。ownership control plane 使用 AWS S3 conditional operations，并保持 storage boundary 可替换。每次 runtime 启动产生唯一 Node Session；一个可续持的 Node Lease 证明整个 session 的运行资格，Actor Owner Record 通过 CAS 把 Actor Address 绑定到 Node Session 与单调 Ownership Epoch。Node Lease 失效触发 runtime-wide fail-stop；Actor Owner Record 决定 claim、release、routing 与 takeover。相比每个 Actor 独立续租，两层模型避免续租成本随 Active Actor 数量线性增长。

lease 采用 celld-inspired 的务实基线：本地 wall clock 写 absolute expiry，本地 monotonic clock self-fence，正常 NTP 是部署前提，不宣称容忍任意 clock skew。CAS conditional rejection 重新解析 authority；ambiguous mutation 必须 exact read-back 并有界协调。只有能够证明 command 未被 peer 接纳的失败才允许自动重试；可能已经 dispatch 的失败返回 outcome unknown。capacity 随 Node Lease 发布但只作 placement hint，目标 Node 必须重新做本地 admission。

下一 slice 不提供 Actor Store、checkpoint、restore 或 Durable ACK。Owner Node Lease 明确过期或缺失后，新 Node 可以 CAS 到更高 epoch，并从空 CoActor 状态完成 construction 与 activation；consumer 可以自行读取外部数据库，但其一致性和 side-effect fencing 不属于 CoActor 保证。该能力称为 Availability Failover，不称为 Recovery。idle passivation 在 Active Actor 完全停止后把 owner record 标记为 unowned 并保留 epoch；graceful shutdown drain 后释放 Node Lease，保留 Actor Owner Records 供更高 epoch takeover。

AWS adapter 在开发与 CI 中通过 deterministic fake、local HTTP contract tests 与 LocalStack 验证；real-AWS qualification 是首次生产发布条件。qualification 通过前只能声称“面向 AWS S3 语义设计”。
