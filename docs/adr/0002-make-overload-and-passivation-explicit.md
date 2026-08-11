# Make overload and passivation explicit

每个 activation 使用独立的有界 mailbox，满载时立即返回 `MailboxFull`；idle passivation 通过与路由相同的 registry 临界区执行 generation 检查、mailbox 空检查和路由移除。选择拒绝而不是默认等待或合并，是为了让不同业务显式决定游戏输入合并、关键命令背压或文档 operation 拒绝策略；generation 检查则避免旧 activation 删除已经替换它的新路由。当前 mailbox 是内存队列，因此 enqueue 与 handler reply 都不是 durable ACK。
