# Quick start

## 定义 Actor

```rust
use coactor::{actor, Actor, ActorRuntime, MessageContext};

#[actor]
struct Counter { count: u64 }

impl Actor<()> for Counter {
    fn new(_: ActorRuntime<()>) -> Self { Self { count: 0 } }

    async fn on_message(&mut self, ctx: &MessageContext, _: &[u8]) {
        self.count += 1;
        ctx.send(self.count.to_string().into_bytes()).await.unwrap();
    }
}
```

不需要共享依赖时，State 默认为 `()`。需要数据库 Client、HTTP Client 或配置时，使用 `.with_state(app_state)`；它可以位于 `.actor(...)` 前或后。Actor 通过 `runtime.state()` 获得 `&S`。

## 测试

```rust
use coactor::{ActorAddress, test_support::TestServer};

let server = TestServer::builder()
    .actor::<Counter>("counter")
    .start()
    .await?;

let address = ActorAddress::new("counter", "counter-7")?;
let mut session = server.client().open(&address).await?;
session.send(b"increment".to_vec()).await?;
let event = session.recv().await.unwrap()?;

server.shutdown().await;
```

Actor Type 和 Actor ID 都使用 Kubernetes DNS-label 语法：1–63 个小写字母、数字或内部连字符，且首尾必须是字母或数字。

## 生产 Server

生产 Server 接收具体的 Coordination Store，而不是 backend 配置 enum。内置 S3 Store 接收已配置好的 AWS SDK S3 Client：

```rust
use coactor::{Server, coordination::backend::s3::S3CoordinationStore};

let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
let store = S3CoordinationStore::new(
    aws_sdk_s3::Client::new(&sdk),
    "my-bucket",
    "coactor/prod",
)?;

let server = Server::builder(store)
    .bind("0.0.0.0:7000".parse()?)
    .advertised_endpoint("actor-0.actor-headless.default.svc:7000")
    .node_id("actor-0") // 可选；省略时使用 canonical advertised endpoint
    .actor::<Counter>("counter")
    .start()
    .await?;
```

- `bind` 是本进程监听的 `SocketAddr`。
- `advertised_endpoint` 是其他节点连接到该 Server 的 canonical `host:port`，不包含 `http://`、路径或 TLS 信息；支持 DNS、IPv4 与 bracketed IPv6。
- `node_id` 是跨进程重启稳定的逻辑节点身份。显式值必须是 Kubernetes DNS label；简单部署可省略并使用 advertised endpoint。
- 每次 `start` 都生成新的 Node Session ID，用于区分占用同一 Node ID 的不同进程 incarnation。

## Client

```rust
use coactor::{ActorAddress, Client, coordination::backend::s3::S3CoordinationStore};

let directory = S3CoordinationStore::new(
    aws_sdk_s3::Client::new(&sdk),
    "my-bucket",
    "coactor/prod",
)?;
let client = Client::builder(directory).build()?;

let address = ActorAddress::new("counter", "counter-7")?;
let mut session = client.open(&address).await?;
session.send(b"increment".to_vec()).await?;

client.shutdown().await;
```

`Client::builder(...).build()` 是同步操作，不访问 Node Directory。打开 Session 时才请求目录快照；S3 Store 自己负责 refresh interval、缓存、singleflight 与 jitter。

完整可执行版本见 [`coactor/examples/cluster_counter.rs`](../coactor/examples/cluster_counter.rs)。

## S3 运维

S3 object key 可直接阅读：

```text
<prefix>/nodes/<node-id>/lease.json
<prefix>/actors/<actor-type>/<actor-id>/ownership.json
```

优雅 shutdown 使用当前 storage revision 条件删除 Node Lease，旧进程不能删除 replacement Session 的租约。crash 残留在 lease 过期后不再有效；部署应配置 S3 Lifecycle 清理过期 Node object。启用 Versioning 的 bucket 还应配置 noncurrent-version expiration，避免旧 Node Lease 版本长期累积。
