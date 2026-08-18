---
status: accepted
---

# 嵌入式 Actor runtime

CoActor 将 Actor runtime 实现为嵌入 consumer Tokio 进程的 library。它自行负责 Actor identity、registration、bounded admission、串行且不可重入的执行、lifecycle、failure 与 shutdown 语义，而不是把这些保证委托给 remoting framework 或隐藏 executor。这样既能让 consumer 关心的行为保持明确、可测试，也把进程 supervision 与基础设施 ownership 留给宿主应用。

Consumer 通过 struct-level Actor macro 与手写 `Actor` trait implementation 接入（见 ADR-0007）。Caller 使用稳定的 Actor Type name + Actor ID 寻址 Actor，并通过双向 Session 交换消息（见 ADR-0005）。Actor location、消息 plumbing 与 runtime dispatch 对 consumer 隐藏；Actor 构造时通过 `ActorRuntime` 注入依赖，不使用 service locator。

Mailbox acceptance 与 Event delivery 只表示内存 runtime 的进度。Actor passivate 或失败后可以从空状态重新启动。精确 API 约束、lifecycle 顺序与 error 行为由 runtime semantics 文档维护，不在本 ADR 重复。
