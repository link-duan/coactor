---
status: accepted
---

# Server/Client 拆分：网关转发、服务发现与 transport seam

单一 `coactor` crate 同时承载宿主与调用能力，client 侧 consumer 被迫接触 server 机制（Actor macro、注册表、AppState 泛型）。分布式远程能力是主线，本地能力仅为测试验证；因此按 API 边界把 runtime 拆成 `Server` 与 `Client` 两个公开入口（保持单 crate、无 feature 门控，模块边界为未来拆 crate 预留）。

- **Server**（`ServerBuilder`，泛型 `S`）只宿主：Actor Type 注册、生命周期、ownership authority 写入、accept 入站传输、网关转发与按负载放置。不提供 caller 能力。
- **Client**（`ClientBuilder`，非泛型）只调用：Service Discovery 发现候选 Server 节点 → 维护连接池 → 会话经网关节点中继到 owner。不宿主 Actor、不持有 authority 访问（无 lease、无 claim）。
- **actor name 移出 macro**：`#[actor]` 无属性，注册时显式 `register::<A>(name)`；`actor(name, id)` 是纯字符串 API，无本地注册表 eager 校验；Client/Server 两侧 consumer 约定名称寻址。
- **transport 是 crate 私有 seam**：`ServerTransport`/`ClientTransport` 双半部 + 双向 `PeerStream`（`peer_protocol::Envelope` 为消息单元）；`grpc`（网络，prost 序列化仅限此处）与 `inmem`（进程内逻辑转发，无 socket）两个 impl。本地测试经 `test_support::TestServer`（装配 inmem）驱动，不做 loopback 监听。
- **服务发现是公开 trait**：`ServiceDiscovery::resolve() -> Vec<Endpoint>`（poll 式刷新）；内置 `dns`（DNS 名解析多 A 记录，覆盖 K8s headless service 与非 K8s DNS 场景）与 `static-list`。
- **网关转发模型**：转发仅指向 owner（ownership 是正确性边界，转发到非 owner 破坏串行化）；负载感知只作用于放置（激活 `placement_candidates`）。owner 或网关失效都会显式打断会话并暴露给 caller 重新 `open()`，Availability Failover 的状态丢失对 consumer 保持可见。
- **删除同进程 caller 直达路径**（`EventSink::Local`、caller 直接 dispatch）；Server 只有 transport 入站路径。

## Considered Options

- **crate 拆分**（`coactor-client`/`coactor-server`）：被否，阶段上保持单 crate，以 API/模块边界先行，未来拆 crate 时边界已就绪。
- **feature flags** 做编译期隔离：收益与 cfg 维护成本不成比例，被否。
- **单一对称 `Transport` trait**：Client/Server 传输方向天然不对称（Client 只 connect、Server 只 accept），双半部 trait 契约更精确。
- **loopback gRPC 本地测试**：被否，进程内 inmem 逻辑转发更贴近"分布式是主线"，且未来 transport 演进（如序列化层替换）对测试透明。
- **Client 只读 S3 解析 owner**（早期决策）：被网关模型取代，Client 不再持有 authority 访问；Server 是唯一 authority 消费方。

## Consequences

- 会话失效边界扩展：除 owner 失效外，网关失效也会打断会话（Client 在连接层感知并重开）；ADR-0005 的"failover 显式打断 Session、caller 重开"语义保持，检测点部分迁移到网关。
- ADR-0006 的 bidi Envelope 流与 node-pair 多路复用保留（成为 transport seam 的契约）；caller 到 owner 的直连改为 caller → 网关 → owner。
- 开放决策：均衡算法（Client 池与 Server 放置）、非 K8s 发现的正式内置方案。
