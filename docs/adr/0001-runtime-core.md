---
status: accepted
---

# Runtime core

CoActor 直接在 consumer 的 Tokio runtime 上实现 keyed Active Actor registry、bounded mailbox 与 lifecycle，而不建立隐藏 executor，也不把核心语义交给现有 Actor remoting 框架。每个 Active Actor 在自己的 task 中串行、非重入地执行 command；不同 Actor 可以并发。handler panic 采用 Actor 级 fail-stop，已接纳但未完成的 command 明确失败，后续调用可以从空状态重新激活。

资源与生命周期行为保持显式：mailbox 满载立即拒绝；runtime 通过 Active Actor 总量限制控制节点资源；idle passivation 只有在 handler 结束且 mailbox 为空时才开始，并在 deactivation 完成后移除路由；graceful shutdown 停止 admission、drain 已接纳 command，再执行 deactivation。`tracing` 负责结构化诊断，但 library 不安装全局 subscriber，也不拥有 metrics 或管理端口。

选择自有单进程核心，是为了让 mailbox admission、passivation race、panic、shutdown 与 Handler Reply 语义可由 CoActor 自己验证。Handler Reply 和 mailbox enqueue 都只表示内存 runtime 行为，不是 durable ACK。详细的当前保证以 runtime semantics 文档为准。
