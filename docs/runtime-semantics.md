# Runtime semantics

## Addressing and Session

- `ActorAddress::new(actor_type, actor_id)` 校验并持有两个字符串；两者都必须是 Kubernetes DNS label。
- ActorAddress 不绑定 Client，可用于多个 Client、断线重连与跨进程重建。
- `Client::open(&address)` 直接打开持久双向 Session，不存在中间 staging handle。
- Session 建立失败使用 `OpenError`；已建立 Session 的 Action/Event 投递失败使用 `SendError`。
- 获取或 clone ActorAddress 不会启动 Active Actor；成功 open 才会触发 lookup、placement 或 lazy activation。

## Actor lifecycle

- Actor Type 在 Server builder 上通过 `.actor::<A>(...)` 注册；字符串使用默认策略，`ActorConfig` 提供 per-type mailbox capacity 与 idle timeout override。
- 默认 mailbox capacity 为 32；最大 Active Actor 数为 10,000；默认 idle timeout 为 60 秒；deactivation timeout 为 5 秒；shutdown timeout 为 30 秒。
- 同一 Actor Address 在一个 runtime 内至多有一个 Active Actor。idle passivation 只停止 Active Actor，不删除逻辑 Actor Address。
- Actor panic 会终止当前 Active Actor；后续 open/Action 可创建新的空状态实例。
- Session 是 caller 与 Actor 之间的持久通道。Action 没有同步 reply；Actor 通过 Session Handle 发送 Event。

## State

- App State 是一个 Server 内所有 Actor Type 共享的强类型依赖容器。
- 未调用 `.with_state(...)` 时 State 为 `()`。
- `.with_state(state)` 可以出现在 `.actor(...)` 前或后，但所有注册必须使用同一个 State 类型。
- runtime 内部使用共享所有权；consumer API 只暴露 `ActorRuntime::state() -> &S`，不暴露内部 `Arc<S>`。

## Lifecycle ownership

- `Server`、`Client` 和 `TestServer` 均不可 Clone。
- `shutdown(self)` 消费 lifecycle handle，避免停止后的 runtime 被再次使用或重复 shutdown。
- Client shutdown 终止其现有 Session；Session 后续发送返回 `RuntimeStopped`。
- `Server::wait(&self)` 仅将 self-fence 报告为 `ServerFailure::Fenced`；正常 shutdown 不作为 failure。

## Testing

`TestServer::builder()` 使用 in-memory transport，并共享生产 API 的 State、Actor registration、mailbox、idle、deactivation、capacity 与 shutdown policy。它不接受 bind、advertised endpoint、Node ID 或生产 Coordination Store 配置。
