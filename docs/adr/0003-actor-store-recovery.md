---
status: accepted
---

# 将 Actor Store 与 Recovery 延后到持久化阶段

持久化与首个分布式可用性阶段明确分离。后续 Actor Store 将提供 Actor-scoped transactional state，不要求 consumer 采用 event sourcing；持久状态必须绑定 Actor 的 fenced Ownership Epoch，consumer 不管理物理 storage identity。

计划中的基础方案异步 checkpoint 完整 Actor Store 文件，因此接受非零 recovery point objective；普通 Event 仍然不是 durable acknowledgement。计划内的 Passivation、Migration 与 Shutdown 必须在完成前 flush dirty state；持久化失败应停止受影响 Actor，而不是虚构成功的 durability guarantee。

Recovery 从更早 Ownership Epoch 选择固定 state version，不追逐 stale Owner 的后续写入。不存在历史状态时恢复空 Store；选定状态损坏属于 Restore Fault，不能静默回退到更旧数据。Storage layout、checkpoint scheduling 与 restore algorithm 仍是未来设计细节，实现前必须在持久化 spec 中进一步明确。
