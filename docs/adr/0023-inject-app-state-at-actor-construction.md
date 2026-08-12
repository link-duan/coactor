# Inject App State at Actor construction

CoActor runtime 持有一个 consumer 定义的 `Arc<AppState>`，并在每次 lazy activation 时将同一实例 clone 给 Actor 的同步构造器 `new(actor_id, Arc<AppState>)`。Actor 自行决定保存完整 App State 或提取所需依赖；异步初始化仍由 `on_activate(&mut self)` 完成。`on_activate` 与 `on_deactivate(reason)` 不接收 context。

每次 command invocation 单独创建无生命周期、非泛型的 `CommandContext`。首版仅包含当前 `ActorAddress`，公开 `actor_id()` 与 `actor_address()`，且不实现 `Clone`。它不携带 App State，也不提前定义 command ID、deadline、取消信号、runtime handle 或跨 Actor 调用。未来若加入 invocation metadata，应扩展 `CommandContext`，而不是把 Active Actor 级依赖重新放回其中。

这一决定取代 ADR 0021 中通过 `ActorContext<'a, S>` 向 lifecycle 与 command 借用 App State 的 API 形状。代价是 Actor 显式耦合其 Runtime App State 类型并持有一份 Arc 引用；收益是依赖在构造阶段集中注入、command 签名不再暴露生命周期或 App State 泛型，且 context 的名称与 command invocation scope 一致。
