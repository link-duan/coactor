# Use per-Actor ownership first

后续首个 S3 Ownership Provider 按 Actor Address 独立管理 ownership，每个 Actor 对应一个只保存 owner、epoch、lease 等控制信息的 S3 ownership object；Actor-scoped KV 使用独立 data objects。相比 partition ownership，这使协议和单 Actor 迁移更直接，但 ownership 对象数量与续租请求会随 Active Actor 数量增长；该 slice 明确接受此伸缩成本。ownership 与 data objects 分离也意味着不能用“先检查 owner，再写 KV”证明 fencing，后续协议必须解决跨对象提交问题。只有在测量证明需要时才引入 partition 或 hybrid ownership，并为已有对象布局设计迁移方案。该决定不属于无存储、单进程的首版范围。
