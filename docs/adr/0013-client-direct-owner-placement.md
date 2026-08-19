---
status: accepted
---

# Client 直连 Owner 并负责 Placement

Client 在 `open()` 时通过只读 Coordination 能力查询 Actor Owner；已有 live Owner 时直接连接其 Server，unowned/stale 时从 Node Directory 读取负载快照并执行可替换的 p2c + In-flight Placement，再把 `SessionOpen` 发给候选 Server。Server 仍是 ownership mutation 与 fencing 的唯一权威：它只会本地服务、在有容量时执行 ownership CAS，或返回 `NotOwner`/`RuntimeAtCapacity`，不再充当 Gateway、保存 Relay 或向其他 Server 转发 Session 消息。

Client 对 placement 失败节点做排除和有界重试，默认最多发起三次 `SessionOpen`，所有 ownership 查询、建连、claim 与 ack 等待共享一个 `open_timeout` deadline。`NotOwner` 触发重新读取 Ownership Store；已知 live Owner 不可连接时不得绕过 authority 向其他节点 placement。候选继续使用 Node Directory 的协议、draining、pressured 与容量快照过滤，最终 capacity reservation 和 ownership CAS 仍由目标 Server 实时裁决。

每个 Server endpoint 由 Client 维护可配置的多连接池，默认最多四条、按需建立；Session 从打开到关闭固定使用一条连接，以保留消息顺序并把连接故障限制在该连接绑定的 Sessions。Node-to-Node `Peer` 命名改为中性的 `Transport`，因为业务传输拓扑变为 Client Runtime ↔ Server。

本 ADR supersede ADR-0002/0005 中的 Gateway 或 Node-to-Node 业务传输拓扑、ADR-0006 的单条 node-pair stream、ADR-0008 的 Gateway 中继与“Client 不读取 Actor ownership”、ADR-0009 的 Server/Gateway placement 执行位置，以及 ADR-0010 的 Gateway Pool 与 Server Placement 筛选职责。仍然有效的决策包括：Server/Client API 拆分、双向 Envelope transport seam、Session 在 Owner 失效时显式中断、Node Directory 作为唯一节点来源、p2c + Load Ratio + In-flight Placement 算法，以及只有 Server 可以 claim/release ownership。

本决策不改变 Action/Event 的 at-most-once 语义、bounded queue 行为或 `send().await` 的 admission 边界，也不提供 Session 透明迁移。所有 Server 当前必须注册相同 Actor Types；heterogeneous Actor Type Placement 仍需独立设计。
