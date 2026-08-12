# S3 stale-owner 持久写隔离方案评估

## 结论摘要

在“每个 Actor 一个 ownership object、Actor-scoped KV 使用独立 S3 objects”的前提下，S3 只能对**单个 object key**提供原子更新和条件写，不能表达“仅当 ownership object 仍为 epoch E 时，才写另一个 KV object”。AWS 明确说明 S3 的更新以 key 为边界，无法跨 key 原子更新，也无法让一个 key 的更新依赖另一个 key。[S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

因此：

- Active Actor 在失联时停止新写是必要的，但不能撤回已经发出的延迟请求。
- 只保护 ownership object 时，ownership 是 advisory/cooperative 的；不能宣称 Actor KV 已被 storage fencing。
- `If-Match` 用在每个 KV object 上只能提供该 key 的 optimistic concurrency，不能自动证明调用方仍持有 ownership。
- epoch 命名空间能让 stale write 成为不可见 orphan，但单独使用时无法给旧 epoch 建立无歧义的恢复截点。
- 在纯 S3 方案中，最清晰的端到端保证是：先写不可变 data object，再通过同一个 ownership/control object 的 CAS 发布 commit pointer/manifest。物理 orphan 可以存在，但不会成为逻辑已提交状态。
- 如果不接受 control object 参与提交，则应明确降低首版保证：S3 ownership 只控制 Active Actor 的协作状态，不保证独立 KV objects 不被旧 owner 覆盖。

## S3 能提供的基础语义

S3 对成功的对象 `PUT`、覆盖 `PUT`、`DELETE` 以及后续 `GET`、`HEAD`、`LIST` 提供强 read-after-write consistency；单 key 更新是原子的，读取者只会看到旧值或新值，不会看到部分内容。但无条件并发写同一 key 时是 last-writer-wins，跨 key 没有原子更新。[S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

条件写可作为单 key CAS：

- `If-None-Match: *` 在当前 key 不存在时创建，已存在时返回 `412 Precondition Failed`。
- `If-Match: <ETag>` 只在当前对象 ETag 匹配时更新；不匹配返回 `412`。
- 并发删除或其他冲突还可能产生 `409 Conflict` 或 `404 Not Found`，调用方必须重新读取并按语义重试。
- multipart upload 的条件只在 `CompleteMultipartUpload` 发布完整对象时生效；冲突后的某些 `409` 要求整次 multipart upload 重启。

来源：[Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html#conditional-error-response)、[PutObject `If-Match`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfMatch)、[PutObject `If-None-Match`](https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutObject.html#AmazonS3-PutObject-request-header-IfNoneMatch)、[CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html)。

ETag 应被视为 S3 返回的不透明 CAS token，不能假定是内容 MD5；SSE-KMS、SSE-C 和 multipart 等情形下该假定不成立。[Object ETag](https://docs.aws.amazon.com/AmazonS3/latest/API/API_Object.html#AmazonS3-Type-Object-ETag)、[ETag and multipart checksums](https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity-upload.html#checking-object-integrity-etag-and-md5)

## 需要防御的失败场景

```text
1. A 持有 epoch 7，并向独立 KV object 发出 PUT
2. 请求或响应长时间延迟
3. A 的 lease 失效，runtime 将 A 转为 OwnershipLost
4. B 通过 ownership object CAS 获得 epoch 8
5. A 在步骤 1 发出的 PUT 才抵达或才完成
```

步骤 3 能阻止 A **继续发起**写入，但不能取消步骤 1 已离开进程的请求。取消 Rust future、客户端超时或关闭连接都不能作为服务端未提交的证明。

这里必须区分：

- **物理写入**：S3 中确实产生了 object/version，占用存储并可能需要 GC。
- **逻辑提交**：runtime 的读取、恢复和 ACK 规则承认该数据是 Actor 当前状态的一部分。

强 fencing 不一定要阻止所有物理 orphan；它必须阻止 stale owner 的数据成为逻辑已提交状态。

## 方案比较

| 方案 | 延迟旧请求 | 跨 object 原子性 | 可宣称保证 | 复杂度 / 成本 |
|---|---|---|---|---|
| 仅 cooperative lease/state transition | 可能覆盖当前 KV | 无 | 只保证健康 runtime 失权后不再主动发新写 | 最低；正常写约 1 PUT |
| 每个 KV 使用条件写 | 若 KV ETag 未被新 owner 改变，旧 owner 仍可能成功 | 无 | 单 key optimistic concurrency，不是 ownership fencing | 每 key 需要 ETag 管理、冲突刷新与重试 |
| epoch 命名空间 + 不可变 objects | 旧 epoch 写可物理成功，但当前 epoch 读取可忽略 | 无明确恢复截点 | 可隔离新 epoch 的可见数据；单独不足以证明完整恢复 | key/object 数增多，需要 GC 与恢复协议 |
| control object CAS 发布 manifest/commit pointer | data PUT 可成为 orphan；旧 owner 的发布 CAS 失败 | 提交顺序集中在单 key CAS | stale owner 不能发布逻辑状态；获得 ownership 与提交有统一线性化点 | 每次提交至少 data PUT + control CAS，热点集中、需 GC |
| S3 Versioning / Object Lock | 旧写仍可创建新 version | 无 | 历史保留、审计、误删恢复；不是 fencing | 全版本存储计费；治理复杂度增加 |
| 外部 transactional fencer | 取决于是否代理发布；直接 S3 PUT 仍不受外部事务约束 | 事务通常只覆盖外部系统 | 可把 ownership 与状态元数据放在同一事务域；不能原子包含直接 S3 PUT | 最高运维复杂度，但可获得更强事务能力 |

### 1. 仅 cooperative lease/state transition

Active Actor 可维护 `Owned -> OwnershipSuspect -> OwnershipLost` 状态机，在 suspect/lost 时拒绝 mutation 和新的 persistence 调用。这能缩小风险窗口，也应作为所有方案的第一道防线。

它不能处理已在途的请求。若 KV 使用固定 key，旧请求在 B 接管后完成时会直接成为当前可见值。S3 的 ownership CAS 与 KV PUT 位于不同 key，不能组合成一个条件操作。[S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)

**适用定位**：MVP 可用于验证 lifecycle 和失权后的本地行为，但文档必须称其为 cooperative/advisory ownership，不得声称 durable state single-writer 或 storage fencing。

### 2. 每个 KV object 使用 `If-Match`

对 KV object 使用 ETag CAS 可以防止基于旧 KV 版本覆盖新 KV 版本，但 precondition 检查的是**该 KV object 的 ETag**，不是 ownership object 的 epoch。[Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

反例：A 缓存 KV 的 ETag `K1`；B 获得新 ownership 但尚未修改该 KV；A 的 `If-Match: K1` 仍会成功。若要让它真正绑定 ownership，B 必须在接管时改变每个 KV object 的 ETag，或所有 KV 本身必须包含并 CAS 更新 epoch。前者需要枚举和改写整个 Actor keyspace，且 S3 又不能把这些 key 与 ownership 原子切换；后者仍有接管窗口。

**可获得的保证**：单 key optimistic concurrency。它适合防止普通 lost update，但不应单独称为 owner fencing。

### 3. epoch 命名空间 + 不可变 objects

示例布局：

```text
actors/<address-hash>/ownership
actors/<address-hash>/data/<epoch>/<operation-id>
```

写入使用唯一 key 和 `If-None-Match: *`；当前 owner/recovery 只读取 ownership object 指定 epoch 的 namespace。这样 A 的延迟请求即使在 epoch 8 开始后写入 epoch 7，也只是物理 orphan，不会直接覆盖 epoch 8 的当前数据。

但该方案本身没有定义 epoch 7 的最终恢复截点：B 获取 epoch 8 后扫描 epoch 7 时，A 的延迟 object 可能在扫描之前、期间或之后出现。若把它纳入，可能把失权后的未确认写当成已提交状态；若忽略它，又需要证明所有失权前已确认写已包含在基线中。S3 跨 key 无原子 cut，因此需要额外的 sealed checkpoint、commit index 或 manifest 才能闭合恢复语义。

**可获得的保证**：stale data 与新 epoch 的直接可见状态隔离；不能单独保证“所有且仅有失权前已提交写”被恢复。

### 4. ownership/control object CAS 发布 commit pointer/manifest

这是最清晰的 S3-only 逻辑 fencing：

```text
1. owner 写唯一、不可变的 data object
2. owner 以 ownership/control object 的当前 ETag 执行 If-Match CAS
3. CAS 内容保持 owner/epoch，并推进 commit sequence 或 manifest pointer
4. persistence API 仅在 CAS 成功后返回 committed
5. 读取与恢复只跟随 control object 已发布的 manifest
```

ownership 转移也 CAS 同一个 control object。于是 ownership acquisition 与 state publication 在同一个 key 上排序：

- 若 A 的 commit CAS 先成功，B 必须读取新的 ETag/manifest 后再接管，A 的提交属于切换前状态。
- 若 B 的 acquisition CAS 先成功，A 持有旧 ETag 的 commit CAS 返回 `412`/冲突，data object 只成为 orphan。

该方案不阻止旧 owner 上传 bytes，但阻止它们成为逻辑状态；这正是可维护的 fencing 边界。S3 的强一致性和单 key 原子性支持此设计，而“跨 key 无原子更新”决定了 publication 必须集中到该 control key。[S3 data consistency model](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Welcome.html#ConsistencyModel)、[Conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html)

代价是每次 durable commit 至少多一个 control PUT，所有 Actor 内提交都经过一个 CAS 热点，还需要清理 CAS 失败留下的 orphan。考虑到 CoActor 已要求单 Actor 串行处理，这个热点与 Actor 本身的顺序边界一致；但高频 workload 仍需通过 batching/checkpoint 降低请求放大。

### 5. S3 Versioning 与 Object Lock

Versioning 会保存同 key 的多个完整版本，同时写时可能把所有版本都保留下来；覆盖会产生新的 current version。每个版本是完整对象并单独产生存储费用。[S3 Versioning](https://docs.aws.amazon.com/AmazonS3/latest/userguide/Versioning.html)

Object Lock 只保护特定 object version。AWS 明确说明 retention/legal hold 不阻止同 key 创建新 version 或在其上添加 delete marker。[S3 Object Lock](https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-lock-overview.html)

因此二者适合作为审计、误删恢复和历史保护层，却不能判断哪个 Actor epoch 有权创建新版本，也不能替代 fencing。若读取方默认读取 current version，延迟旧写仍可能成为当前版本。

### 6. 外部 transactional fencer

外部系统可以把 ownership epoch 与提交索引放入一个可事务更新的数据模型。例如 DynamoDB 的条件表达式可让单 item 写在条件成立时执行；DynamoDB transactions 可对多个 DynamoDB items/tables 做 ACID、all-or-nothing 更新。[DynamoDB condition expressions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.ConditionExpressions.html)、[DynamoDB transactions](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transactions.html)

但该事务域不包含 S3 object PUT。若 consumer/runtime 仍可直接写“当前 KV key”，外部事务无法撤回或条件约束该 S3 请求。可靠设计仍需采用以下一种：

- S3 data immutable，外部事务只原子发布 pointer；
- 所有持久写经外部 fencer 代理；
- 状态本身也存入同一事务系统，S3 只承载 blob。

这会引入第二个权威系统、跨服务故障处理和更高运维成本。只有在 S3 control-object CAS 的吞吐、大小或事务表达力不能满足需求时，才值得引入。

## 复杂性、可维护性与一致性的权衡

### 最低复杂度

采用 cooperative state transition，并允许 direct KV overwrite。实现和请求成本最低，但必须接受 advisory ownership；其正确性依赖所有 runtime 实例合作，无法防御 zombie/in-flight write。

### 中间方案

采用 epoch namespace + immutable objects，将物理 stale write 隔离为 orphan。它避免 stale overwrite，适合 append-only journal/blob，但还必须补充明确的 commit cut；否则恢复语义仍不闭合。

### 最强的 S3-only 方案

采用 immutable data + control-object CAS publication。它把“上传”和“提交”分离：允许无害 orphan，严格控制逻辑可见性。协议、测试和 GC 比 direct KV 更复杂，但线性化点单一，最容易解释、维护和做 stale-owner fault test。

请求成本也最明确：通常每次提交为一个或多个 data PUT 加一个 control CAS PUT；CAS 冲突和 ETag 刷新会增加请求。AWS 对相应请求计费，失败的条件请求也会产生请求费用；Versioning 的每个完整版本也产生存储费用。[Conditional requests](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html)、[S3 pricing](https://aws.amazon.com/s3/pricing/)

## 对 CoActor 的建议

1. 无论选哪种持久层协议，都保留 Active Actor 的 ownership 状态机，失权后停止接纳 mutation 和发起新 persistence 请求。
2. 首版若坚持“ownership object 只保护 ownership，KV 独立直写”，应把保证写成：
   - runtime 尽力维持单 Active Actor；
   - 失权实例停止新写；
   - 不保证已在途的旧 S3 KV 写不会在接管后可见；
   - 不将该模式用于需要 durable single-writer 保证的 Actor。
3. 若目标仍是“旧 owner 的持久写不能成为当前状态”，优先选择 immutable data + ownership/control-object CAS publication。pointer/commit sequence 属于 control metadata，不要求把 KV 内容放进 ownership object。
4. Versioning 可作为调试、审计和恢复保护层，不能列为 fencing 实现。
5. 对默认 S3 provider 必须测试：A 的 data PUT 被延迟、B 先完成 acquisition CAS、A 随后完成上传与发布；预期 A 的发布失败，B 的恢复只跟随已发布 manifest，同时 orphan 可被后续 GC。

最终决策不是“能否阻止 S3 收到旧 bytes”，而是“是否需要证明旧 bytes 永远不会成为 Actor 的逻辑已提交状态”。若需要该证明，就必须存在一个同时排序 ownership 与 state publication 的权威线性化点。
