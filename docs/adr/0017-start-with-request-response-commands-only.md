# Start with request-response commands only

CoActor 首版只提供 request-response command，不提供 fire-and-forget API。这样 mailbox 满载、activation 失败、Actor deactivation、handler panic 和 runtime shutdown都能向 caller 返回明确结果，不需要额外设计无人等待时的丢弃、重试和错误观测语义。command 成功进入 mailbox 后，caller 超时、取消或 drop request future 只放弃等待 reply，不撤回 command；Actor 仍按顺序执行，无法投递的 reply 被丢弃。未来若高频游戏输入等场景需要 fire-and-forget，将作为独立能力定义其 admission 与 failure semantics。

首版 generated method 不提供内置 call/reply timeout 或 per-call options。consumer 可用 `tokio::time::timeout` 包裹调用，其效果只是放弃等待 reply，与既定 cancellation 语义一致。
