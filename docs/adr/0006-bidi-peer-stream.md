---
status: accepted
---

# Node 间 bidi gRPC stream

Cluster 模式下 Node 之间的传输从每消息 unary `Invoke` 改为 Node 间多路复用的 bidi gRPC stream：`Exchange(stream Envelope) returns (stream Envelope)`，消息按 `session_id` 路由。节点对之间由发送方懒建立一条流（`ensure_channel` 按 endpoint 缓存复用），出站 Event、入站 Action 与 Session 控制消息（`SessionOpen`/`SessionClose`/`SessionOpenedAck`/`SessionError`）共用同一 envelope。

路由与调度策略保持消息级不变：每个 Action 仍走 ownership 解析（`ClusterRouter::resolve`，含缓存、capacity reservation、failover 重试、self-fence），Session 只是逻辑路由标签，不绑定物理连接。因此 Session 可以在 Owner 变更时被打断（见 ADR-0005 的 failover 语义），而不需要传输层会话迁移。

传输层必须保证的两点：Owner 侧回传 Event 复用入站流的 outbound（不另建连接，避免 async 递归）；runtime shutdown 时强制关闭入站连接 task，避免 tonic serve 等待外部连接导致退出阻塞。替代方案（每 Session 一条流、unary Deliver 回调式）被否决：前者与 ownership 移动性冲突，后者丢失流式背压与多路复用收益。
