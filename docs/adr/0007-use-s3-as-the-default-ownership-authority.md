# Use S3 as the default ownership authority

Ownership Provider 保持可替换，但 CoActor 后续默认方案提供以 S3 conditional write/CAS 为权威协调点的实现，由 S3 上的线性化对象决定 owner 并推进 Ownership Epoch，不依赖另一个隐藏 coordinator。S3 默认方案的安全边界来自 ownership CAS、node/owner self-fence、epoch-scoped Actor Store 与固定 Restore Cut 的组合，并必须由 S3 一手语义与 stale-owner 集成测试验证；Handler Reply 不是 durable ACK，ADR 本身也不宣称协议已经实现或证明。该决定不属于无存储、单进程的首版范围。
