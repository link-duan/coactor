# MVP runtime semantics

本文定义首个单进程 vertical slice 的可观察语义。未写在“当前保证”中的能力均不能从实现中推导出来。

## Identity and addressing

- 调用方以稳定的 `EntityType + EntityKey` 寻址。
- `EntityType` 是显式稳定名称，不使用 Rust 类型名。
- `EntityKey` 是调用方提供的原始字节；`EntityId` 的稳定编码为 `u32 big-endian type length + type bytes + key bytes`。
- 进程内 `HashMap` 可以使用随机 hash seed，但它只影响本地查找，不定义 Entity ID、分片或持久化 key。

## Activation and execution

- runtime 启动时没有实体 task。首条被接纳的命令为对应 key 创建 activation。
- 每个 activation 拥有一个 mailbox 和一个实体实例。
- 同一 activation 一次只执行一个 command handler；不同实体可以并发。
- 多个并发发送者之间不承诺业务顺序。实际处理顺序是 mailbox 成功接纳的顺序，业务仍需在后续 durable command 中携带 command ID 与业务序号。
- handler 完成后的 reply 只表示本次进程内处理完成，不表示 journal、snapshot 或业务状态已经 durable commit。

## Bounded mailbox

- 每个 activation 的 mailbox 容量固定且大于零。
- 容量只计算排队中的命令，不包含正在执行的命令。
- 当前过载策略是立即拒绝：mailbox 满时返回 `MailboxFull`，不等待、不合并、不静默丢弃。
- 调用方只有在 enqueue 成功后才可认为命令已被进程内 runtime 接纳；进程崩溃仍可能丢失已接纳但未持久化的命令。

## Idle passivation

- activation 仅在 handler 已结束、mailbox 为空且持续一个 idle timeout 没有新命令时尝试 passivate。
- passivation 与路由使用同一个 registry 临界区。退出前，activation 必须确认 registry 仍指向自己的 generation 且 mailbox 仍为空，然后原子移除该路由。
- 如果命令先被旧 mailbox 接纳，passivation 取消并继续处理；如果旧 activation 先移除路由，命令会创建新 activation。两种竞争结果都不得静默丢失命令。
- passivation 后再次访问同一 Entity ID 会产生新 activation。当前没有 restore，因此内存状态从 factory 的初始值重新开始。

## Failure semantics

- 当前没有 durable mailbox、journal、snapshot、command 去重、监督恢复或跨进程投递。
- activation 异常退出后，下一次发送可以替换已关闭的本地路由并重新激活；先前内存状态和未回复命令没有恢复保证。
- 当前不提供 exactly-once、at-least-once、跨节点单 writer、故障转移或迁移保证。

## Reserved boundaries

后续能力必须沿明确边界加入，而不能改写上述 ACK 含义：

- activation factory：未来在实体开始处理 mailbox 前执行 snapshot + journal restore；
- command envelope：未来加入 command ID、schema version 与 ownership epoch；
- durable commit：未来由 storage trait 接收 Entity ID 与 ownership epoch，并在成功提交后产生 durable ACK；
- owner directory：未来只负责 `EntityId -> owner + epoch`，不能把本地 activation registry 当作全局所有权证明；
- handoff：未来必须定义旧 owner 停止接纳、新 owner 获取更高 epoch、排队位置与失败回退，并用双节点测试证明。
