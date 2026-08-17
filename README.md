# CoActor

CoActor 是面向 Rust 应用的嵌入式分布式 Actor runtime。业务以经过校验的 `ActorAddress`（Actor Type + Actor ID）标识逻辑 Actor，通过 `Client` 打开持久双向 `Session`；`Server` 按需激活 Actor，并通过 Coordination Store 协调节点租约、放置与 ownership。

## 核心 API

- `Server::builder(coordination_store)`：注入具体 Coordination Store，配置 bind、advertised endpoint、可选稳定 Node ID、State 与 Actor Type。
- `Client::builder(node_directory).build()`：只依赖只读 Node Directory；构造同步且不访问后端。
- `TestServer::builder()`：使用内存 transport 的测试入口，与生产 API 共用 State、Actor 注册和 runtime policy 词汇。
- `ActorAddress::new(actor_type, actor_id)`：一次校验、可跨 Client 复用的纯值。
- `Client::open(&address)`：直接建立 Session；Action/Event 投递失败使用 `SendError`，建链失败使用 `OpenError`。

## 文档

- [快速开始](docs/quick-start.md)
- [设计简述](docs/design-brief.md)
- [本地运行时语义](docs/runtime-semantics.md)
- [分布式运行时语义](docs/distributed-runtime-semantics.md)
- [产品方向](docs/product-direction.md)
- [ADR 索引](docs/adr/README.md)
