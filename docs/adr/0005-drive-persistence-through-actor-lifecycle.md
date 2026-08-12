# Keep async Actor lifecycle boundaries

CoActor 首版保留异步 Actor lifecycle：runtime 必须等待 activation lifecycle 成功完成后才能处理 command，并在 idle passivation 与 graceful shutdown 时调用并等待 deactivation lifecycle；panic、异常 task 退出和进程 crash 不保证调用退出 lifecycle。activation lifecycle 返回错误时，实例不进入服务态，当前 mailbox 全部返回 `ActivationFailed` 并移除本地路由；后续 command 可以重新触发 activation，失败实例不调用 deactivation lifecycle。首版为简化实现不提供 activation timeout；永久阻塞的 activation 会持续占用该 Actor Address，并使已接纳 command 保持等待，直到 runtime shutdown、task cancellation 或进程退出。deactivation lifecycle 一旦开始便不可撤销：路由保留到 lifecycle 结束以阻止第二个实例启动，期间新 command 返回可重试的 `ActorDeactivating`，结束后才移除路由。deactivation lifecycle 不向 runtime 返回业务错误；consumer 在方法内部处理、记录或重试清理失败，runtime 只等待其结束。等待受可配置 timeout 约束；超时后 runtime 记录 `DeactivationTimedOut`、取消等待、强制结束实例并移除路由，因此 consumer 不能依赖清理一定完成。首版没有状态存储，因此 lifecycle 可用于 consumer 的外部资源初始化和清理，但不承担 runtime restore 或 checkpoint。后续加入 persistence 时仍沿这一顺序边界扩展。当前以 `on_activate` 与 `on_deactivate(reason)` 描述语义，最终名称、参数和返回类型尚未冻结。

activation lifecycle 的具体 consumer error 仅记录到日志与 tracing，caller 统一收到无 payload 的 `SendError::ActivationFailed`。这样每个 generated Actor method 只需要携带自身 handler error 类型，不需要再叠加 Actor lifecycle error 泛型。

两个 lifecycle hook 都是可选的；未实现 activation hook 等同于立即成功，未实现 deactivation hook 等同于立即完成。简单 Actor 因此只需同步 `new(actor_id, Arc<AppState>)` 和 `#[command]` methods。

hook 使用固定方法名 `on_activate` 与 `on_deactivate`，而不是额外 attribute；macro 只需识别并校验这两个可选签名。

Graceful shutdown 停止接纳新 command，但会先 drain 正在执行及已进入 mailbox 的 command，再以 shutdown reason 执行 deactivation lifecycle。整个流程受全局 shutdown timeout 约束；超时后 runtime 终止剩余实例并使未完成 command 返回 `ActorStopped`。
