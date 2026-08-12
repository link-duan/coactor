# Make overload and passivation explicit

每个 Active Actor 使用独立的有界 mailbox，capacity 具有 runtime 默认值并可由 Actor Type 覆盖，同一 Actor Type 的所有 Actor ID 使用相同容量。满载时立即返回 `MailboxFull`；idle passivation 通过与路由相同的 registry 临界区执行 generation 检查、mailbox 空检查和路由移除。选择拒绝而不是默认等待或合并，是为了让不同业务显式决定游戏输入合并、关键命令背压或文档 operation 拒绝策略；generation 检查则避免旧 Active Actor 删除已经替换它的新路由。当前 mailbox 是内存队列，因此 enqueue 与 Handler Reply 都不是 durable ACK。

为限制大量不同 Actor ID 在 idle timeout 内同时启动造成的节点资源增长，runtime 还提供全局 `max_active_actors`。达到上限后，新 activation 返回 `RuntimeAtCapacity`；已有实例继续服务，runtime 不主动驱逐 Actor，也不在首版分配 Actor Type quota。
