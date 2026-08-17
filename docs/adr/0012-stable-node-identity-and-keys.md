---
status: accepted
---

# 稳定 Node identity 与可读 Coordination Store key

Node ID 表示稳定的逻辑 Server slot，runtime 生成的 Node Session ID 表示占用该 slot 的单次进程 incarnation。显式 Node ID 使用 Kubernetes DNS-label 语法；省略时默认采用 canonical advertised `host:port` endpoint。Node Directory 按 Node ID 查询；Node Lease 与 Actor Owner Record 保留用于精确 read-back 和 fencing 的 Session ID 与 lease generation。

Coordination Store key 有意采用可读、可逆形式，而不是 hash 或 escaping：

```text
<prefix>/nodes/<node-id>/lease.json
<prefix>/actors/<actor-type>/<actor-id>/ownership.json
```

Actor Type 与 Actor ID 都使用 Kubernetes DNS-label 语法，因此可以安全作为 key segment。Object key 是 record identity：Node Lease body 不重复 Node ID，Actor Owner body 不重复 Actor Type 或 Actor ID。初始 record 不包含 schema-version field；只有持久 schema 出现不兼容变更时才引入。

Node 以相同 Node ID 重启后，不扫描或直接继承全部 Actor ownership。下一次访问惰性执行 same-node takeover，把 ownership 绑定到新 Session ID 并提高 Ownership Epoch；如果该逻辑 Node 不可用或无法接纳 Actor，则 Placement 回退到其他 live Node。这样可以保留 Placement preference，同时防止两个进程 incarnation 共享 authority。

Graceful shutdown 使用 storage revision 条件删除 Node Lease。Crash residue 在 lease 过期后被忽略，由 S3 Lifecycle 而不是 runtime janitor 清理；启用 S3 Versioning 的部署还必须清理 noncurrent Node Lease version。本 ADR 扩展 ADR-0010 的 Node Directory 模型，将稳定 Node ID 而不是进程 Session ID 定义为 directory 与 storage-key identity。
