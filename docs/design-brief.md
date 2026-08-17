# Design brief

CoActor 为业务 key 提供按需运行的 Actor 执行边界。`ActorAddress` 是由经过 Kubernetes DNS-label 校验的 Actor Type 与 Actor ID 组成的稳定纯值。Caller 使用 `Client::open(&ActorAddress)` 建立 Session；Server 按需创建 Active Actor，Actor 通过 Action 接收入站字节，并通过 Event 输出字节。

## Runtime 边界

- `Server::builder(coordination_store)` 接收具体 `CoordinationStore`，只宿主 Actor、参与 placement 与写入 ownership/Node Lease。
- `Client::builder(node_directory).build()` 只接收 `NodeDirectory`，不能取得 Node Lease 或 Actor Owner 写能力。
- `TestServer::builder()` 提供内存 transport 测试环境，不模拟生产 local mode。
- App State 默认为 `()`；`.with_state(state)` 可位于 Actor 注册之前或之后。Actor 只看到 `ActorRuntime::state() -> &S`。
- `.actor::<A>("name")` 使用默认策略；`ActorConfig` 提供单个 Actor Type 的 mailbox 与 idle override。
- `Server`、`Client` 与 `TestServer` 都是非 Clone lifecycle owner，shutdown 消费 handle。

## Coordination

公开扩展边界位于 `coactor::coordination`：

- `NodeDirectory`：只读 live Node snapshot；
- `NodeLeaseStore`：Node Lease 条件创建、续租与删除；
- `ActorOwnerStore`：Actor Owner CAS；
- `CoordinationStore`：组合以上三项 capability。

Mutation outcome 区分 `Applied`、`Conflict` 与携带后端错误的 `Indeterminate`。普通 Coordination failure 暴露稳定的 `Unavailable`、`PermissionDenied`、`InvalidData` kind，同时保留标准 error source chain。

内置 S3 backend 位于 `coactor::coordination::backend::s3`，由 AWS SDK S3 Client、bucket 与 canonical prefix 构造。凭证、region、endpoint、retry、HTTP 与 timeout 均由调用方通过 AWS SDK 配置。

## Node identity

Node ID 是跨重启稳定的逻辑 slot；Node Session ID 是每次 Server start 生成的进程 incarnation。Node Directory 与 Node Lease 按 Node ID 查询，lease body 保存 Session ID 与递增 generation。Actor Owner 同时保存 Node ID、Session ID 与 Ownership Epoch，任何 Session 切换都必须 CAS 并提高 epoch。

S3 key 使用可读结构：

```text
<prefix>/nodes/<node-id>/lease.json
<prefix>/actors/<actor-type>/<actor-id>/ownership.json
```

object key 是 record identity，因此 body 不重复 Node ID 或 Actor Address。重启节点不扫描 ownership；下一次访问时惰性执行 same-node takeover。若该 Node 无法服务，则回到普通 placement 以保持可用性。

## 当前保证与未来工作

当前保证覆盖内存 Actor lifecycle、Session、Node Lease、availability failover 与 S3 Coordination protocol。Actor Store、durable ACK、Recovery 与 Migration 仍是未来持久化工作；普通 Event 不代表持久提交。
