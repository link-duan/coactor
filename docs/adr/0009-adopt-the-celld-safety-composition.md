# Adopt the celld safety composition

CoActor 后续 distributed persistence 采用 celld 启发的组合安全模型：ownership CAS 决定 owner 并推进 epoch；node 与 Active Actor 在无法及时证明权威时 self-fence；持久状态按 epoch 隔离；异步持久化在计划内退出前形成 durable checkpoint；恢复只读取明确定义的 Restore Cut。该决定只复用安全闭环，不复制 celld 的 SQLite/LTX、V8 runtime 或调度实现。数据组织最初考虑 immutable commit objects，后由 ADR-0011 改为本地单文件 Actor Store 与 epoch-scoped、可覆盖的 durable state object。Durable ACK Gate 保留为更后续边界，Handler Reply 明确不是 durable ACK；首个 persistence slice 接受非零 RPO。任何单项都不能独立证明安全。该决定不属于无存储、单进程的首版范围。
