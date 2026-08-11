# AGENTS.md

## 工作方式

- 先维护 `docs/` 中的架构决策和语义定义，再扩展实现。
- 对分布式正确性的声明必须有测试或一手资料依据。
- 优先做小而完整的 vertical slice，避免先铺设大量抽象。
- 不宣称 exactly-once；采用 durable commit、幂等 command ID、重试和 fencing。

## 代码提交

- 使用 Conventional Commits。

## 初始技术约束

- Rust stable，Tokio 异步运行时。
- entity key 使用稳定字节表示，不能依赖进程内 hash 随机种子。
- mailbox 必须有界，并定义满载时的拒绝、合并或背压策略。
- 所有持久写必须携带 ownership epoch/fencing token。
- Actor 内存不是协同文档的唯一状态真源。
