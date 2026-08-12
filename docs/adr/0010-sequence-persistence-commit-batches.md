---
status: superseded by ADR-0011
---

# Sequence persistence commit batches

Ownership Epoch 内的每个 sequence 对应一次 Persistence API commit batch，而不是单个 KV operation 或 Actor command。batch 可以包含多个 Actor-scoped KV mutation，并作为 durable proof、Durable ACK Gate 与 restore 的最小提交单位；任一部分未完成时，该 sequence 不得进入 Maximum Durable Prefix。sequence 由 runtime 分配，consumer 只决定何时提交及 batch 内容，因此 command 和 lifecycle method 都可以产生零次、一次或多次提交。
