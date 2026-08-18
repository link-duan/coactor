---
status: accepted
---

# 链式构造 runtime 并直接打开 Session

CoActor 使用 capability injection 取代 mode-specific constructor 与 backend config enum：生产侧使用 `Server::builder(coordination_store)`，caller 使用 `Client::builder(node_directory).build()`，测试使用 `TestServer::builder()`。App State 默认为 `()`，`.with_state(state)` 可以出现在 Actor registration 前后；registration 使用 `.actor::<A>("name")` 或 `ActorConfig`，Server 配置通过 `.advertised_endpoint(...)`、`.shutdown_signal(...)`、`.default_mailbox_capacity(...)` 等具名链式方法表达，监听地址在最终 `.serve(...)` 调用中提供。

`ActorAddress` 成为唯一公开地址值：它校验 Actor Type 与 Actor ID，二者都使用 Kubernetes DNS-label 语法。`Client::open(&ActorAddress)` 直接建立 Session；删除 `ActorRef` 以及独立 Rust newtype `ActorType`、`ActorId`，因为它们增加 staging，却没有增加 runtime 行为。`ActorRuntime::state()` 暴露 `&S`，runtime 内部继续持有共享 ownership。

生产 Server 由异步 `serve(listen_address)` 消费 builder 并持有完整 lifecycle：它完成校验、bind、Node Lease 获取与 runtime 启动，然后持续等待 self-fence、内部任务终止或可选的 `.shutdown_signal(future)` 完成，退出前执行 graceful shutdown。CoActor 不订阅进程 signal；consumer 将 Ctrl-C、CancellationToken 或应用 lifecycle 转为 `Future<Output = ()>`。监听地址只接受 `:port`、IPv4 socket address 或 bracketed IPv6 socket address，不做 DNS 解析；bind address 与 advertised endpoint 保持独立。生产侧删除公开 `start`、`wait`、`shutdown`，其启动期、运行期与 shutdown 期失败统一由 `ServerError` 表达；shutdown timeout 强制停止未退出 Active Actor 并返回错误。`serve` 被取消或 drop 时不保证异步清理，该限制只在 rustdoc 中声明。`TestServer::start()` 保持测试 harness 语义。Client construction 仍为同步操作并返回 `ClientBuildError`，`Client::shutdown(self)` 消费 lifecycle owner。Store implementation 在 builder 阶段保持泛型，构造完成后执行 type erasure，因此公开 `Server<S>` 与 `Client` 不暴露 backend type。

这是 deliberate breaking replacement，而不是 deprecated compatibility layer：被删除的 local mode、config enum、`ActorRef` 与旧 error boundary 表达了无法在不保留两套竞争 API 的情况下转发的语义。ADR-0008 的 Server/Client separation 继续有效；本 ADR 取代其具体公开构造与 Actor-ref API 形态。
