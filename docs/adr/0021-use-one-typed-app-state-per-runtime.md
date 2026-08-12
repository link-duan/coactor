# Use one typed App State per runtime

每个 CoActor runtime 绑定一个 consumer 定义的强类型 `AppState`，并通过 `ActorContext` 向所有 Actor Type 提供只读访问。builder 接收 App State 值，runtime 内部以 `Arc<AppState>` 持有，而 `ActorContext::state()` 正常只返回 `&AppState`，使外层共享所有权对 consumer 隐藏并避免每次 command clone 容器。consumer 自行把数据库 client、HTTP client、配置等共享依赖组合进该类型。首版不实现 `TypeId -> Arc<dyn Any>` service locator，因为它会把缺失依赖和类型错误推迟到运行时，也使 Actor 的真实依赖难以从签名与测试中看出。Actor 自身的可变业务状态仍保存在实例内，不放入 App State。

由于同一 App State 会被多个 Active Actor task 并发借用，它必须满足 `Send + Sync + 'static`。

consumer 的 lifecycle 与 `#[command]` 实现显式接收 `&ActorContext<AppState>`，使共享依赖在实现签名中可见；macro 在 generated Actor Ref method 中隐藏该参数，所以 caller 只传业务参数。context 不永久存入 Actor 实例，也不能跨 command 保存借用。

Actor macro attribute 不再重复声明 App State 类型；macro 从 command 与 lifecycle 的显式 `ActorContext<AppState>` 参数提取类型，并校验同一 Actor Type 内保持一致。这样依赖类型只在实际使用位置表达一次。

首版 Actor Context 不携带 runtime handle，也不提供获取其他 Actor Ref 或跨 Actor 调用。跨 Actor 调用会引入调用环、超时、取消与 shutdown 传播语义，留待实际需求出现后单独设计。

其首版数据格式保持最小：`ActorContext<'a, S>` 只借用当前 `ActorAddress` 与 `AppState`，并提供 `actor_id()`、`actor_address()`、`state()` 只读访问。mailbox、generation、task handle、shutdown token 和 tracing span 等 runtime 控制数据不进入 consumer API。

Context 也不提供自身 Actor Ref；在非重入执行模型中，method await self-call 会等待当前 method 释放执行权而死锁。Actor 内部复用使用普通私有方法，延迟调用由外部 consumer 调度。
