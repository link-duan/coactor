---
status: accepted
---

# Session 双向消息模型

> [ADR-0013](0013-client-direct-owner-placement.md) 移除了 Gateway 故障边界；Session 现在固定在 Client 与 Owner Server 的一条 Transport Connection 上。Owner 或连接失效仍会显式打断 Session，caller 必须重新 `open()`。

CoActor 的消息模型从 request-response command 改为 Session 上的双向消息：caller 通过 `open()` 建立与 Actor 的一次会话，入站 **Action** 与出站 **Event** 均为字节负载、fire-and-forget，业务错误由 Event 表达。每个 Action 都必须携带 Session ID，所有入站都经过 Session，不提供无 Session 的单向 tell。

Session 是逻辑概念（`SessionId` + caller 本地接收流），不是物理连接。`open()` 主动激活 Actor（`on_activate` → 注册 Session → `on_session_opened`，可"连接即推送"）；Actor 侧通过构造注入的 `ActorRuntime` 获得 `broadcast`（全部存活 Session）与身份/AppState 能力，定向推送使用 `MessageContext::send` 或可 clone 的 `SessionHandle`。消息的投递状态由 `send` 同步返回（进入 mailbox 后不确认，at-most-once）；出站与入站 mailbox 均 bounded，满载返回 `MailboxFull`。

生命周期语义由三个相互关联的决策构成：

- **passivation 仅在无存活 Session 时触发**：idle 到期时若 Session 计数大于零则重置 idle 计时，避免会话中断；因此一个僵尸 Session 会钉住 Actor 及其容量，CoActor 不提供 Session 空闲超时，该责任属于 consumer。
- **failover 显式打断 Session**：Owner 失效后旧 Session 终止，caller 必须重新 `open()`（新 Owner 空状态激活）。检测是惰性的——caller 下次 `send()` 时 resolve 到与建立时不同的 Owner 即返回可感知的错误；无法感知的情况（旧 Owner 静默死亡）表现为接收流终止或后续发送失败，不承诺精确的错误类别。
- **字节消息边界**：当前阶段 Action/Event 为 `Vec<u8>`，业务协议（编码、schema 兼容）由 consumer 负责；早期曾考虑 prost 类型化与 oneof 简化 macro，因阶段简化而放弃，若未来恢复类型化需单独记录决策。

Handler Reply、命令级错误分类不复存在；`SendError` 只表达 runtime 投递失败，业务错误走 Event 通道。
