---
status: accepted
---

# Actor Store and Recovery

未来 persistence slice 向 consumer 提供 Actor-scoped KV，而不强迫业务使用统一 journal、snapshot 或 event-sourcing 模型。每个 Active Actor 使用本地单文件 Actor Store；runtime 将完整文件作为 epoch-scoped durable state object 上传。consumer 不直接接触物理对象布局或 Ownership Epoch，Persistence API 自动把当前 Actor Address 与 epoch 绑定到写入，consumer 绕过该边界的外部写不在 CoActor fencing 保证内。

Actor Store 的本地事务成功后推进 revision。checkpoint 以完整文件为单位严格串行、异步上传，普通 Handler Reply 不等待上传，因此首个 persistence slice 接受非零 RPO，也不提供 Durable ACK。计划内 passivation、Migration 与 shutdown 必须先成功 flush dirty state；持久化故障只停止受影响的 Actor，不推断 command 是否只读。

Recovery 在新 Owner 获得更高 epoch 后选择小于新 epoch 的最大现存 state epoch，并把第一次成功读取固定为 Restore Cut；恢复过程中不追逐旧 Owner 后续上传。缺少历史对象时恢复为空 Store；选定对象损坏时进入 Restore Fault，不静默回退到更早版本。该组合延续 ownership CAS、self-fence、epoch namespace 与固定 Restore Cut 的 celld-inspired safety shape，但不复制 SQLite/LTX 或其 Durable ACK gate。此 ADR 是未来方向，不属于下一 stateless Availability Failover slice。
