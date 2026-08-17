---
status: accepted
---

# 链式构造 runtime 并直接打开 Session

CoActor 使用 capability injection 取代 mode-specific constructor 与 backend config enum：生产侧使用 `Server::builder(coordination_store)`，caller 使用 `Client::builder(node_directory).build()`，测试使用 `TestServer::builder()`。App State 默认为 `()`，`.with_state(state)` 可以出现在 Actor registration 前后；registration 使用 `.actor::<A>("name")` 或 `ActorConfig`，Server 配置通过 `.bind(...)`、`.advertised_endpoint(...)`、`.default_mailbox_capacity(...)` 等具名链式方法表达。

`ActorAddress` 成为唯一公开地址值：它校验 Actor Type 与 Actor ID，二者都使用 Kubernetes DNS-label 语法。`Client::open(&ActorAddress)` 直接建立 Session；删除 `ActorRef` 以及独立 Rust newtype `ActorType`、`ActorId`，因为它们增加 staging，却没有增加 runtime 行为。`ActorRuntime::state()` 暴露 `&S`，runtime 内部继续持有共享 ownership。

Server startup 保持异步并返回 `ServerStartError`；Client construction 为同步操作并返回 `ClientBuildError`。`Server::wait()` 报告 `ServerFailure::Fenced`，`Server::shutdown(self)` 与 `Client::shutdown(self)` 消费 lifecycle owner。Store implementation 在 builder 阶段保持泛型，构造完成后执行 type erasure，因此 `Server<S>` 与 `Client` 不暴露 backend type。

这是 deliberate breaking replacement，而不是 deprecated compatibility layer：被删除的 local mode、config enum、`ActorRef` 与旧 error boundary 表达了无法在不保留两套竞争 API 的情况下转发的语义。ADR-0008 的 Server/Client separation 继续有效；本 ADR 取代其具体公开构造与 Actor-ref API 形态。
