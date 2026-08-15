---
status: accepted
---

# 放置均衡：p2c + Load Ratio + In-flight 记账

`PlacementStrategy` 接缝早已存在但默认就地认领，`ClusterRouter::placement_candidates`（按 active 数排序取 top-2）是 dead code。本 ADR 确定真正的放置均衡算法，约束来自两个已观察到的不均衡来源：

1. **Placement Burst（瞬时流量）**：直播开播瞬间上千新 room 同时 open，所有网关读同一份 lease 快照，确定性算法会把流量全部导向同一个"看起来最空"的节点（如刚重启 count=0 的节点），雪崩倾斜。
2. **指标滞后**：lease 每 ~3.3s 刷新一次（TTL 10s / 3），加上传播延迟，决策时刻的负载快照不可信——节点刚被塞满仍显示空、刚 passivate 完仍显示满，双向失真。

## Decision

- **算法：p2c（power of two choices）**。随机抽 2 个候选，选 Load Ratio 低的。随机性把瞬时流量概率均匀分散（herd 免疫）；指标错误时算法自动退化为"近似均匀随机"而非确定性倾斜，提供最坏情况底线——对"放置一次即固定、纠错要等 failover"的系统尤其重要。
- **负载度量：Load Ratio = active_actor_count / max_actor_count**（利用率），而非绝对数量或剩余容量。剩余容量会被大节点误导（max=1000 的节点 90% 占用仍显示 100 个空位）；利用率对异构容量给出稳定排序，相对值对全集群同步偏移不敏感。
- **In-flight Placement 记账**：网关本地 `HashMap<Endpoint, (usize, Instant)>`，决定放置即记账，失败/超时扣减、成功保留；记账带过期窗口（一个 lease 刷新周期 ≈3s），窗口过后 lease 已反映、计数自然归零——避免“成功保留”无限增长导致目标被永久高估（反向不均衡）。参与预测 Load Ratio，防同一网关内并发放置的 herd。
- **不做新鲜度标记、不做保守折扣**：`expires_at ≈ sampled_at + ttl` 的数学关系使新鲜度过滤只比现有 `expires_at > now` 硬过滤提前 ~3.4s，价值边际；且新节点（快照最旧、active=0）恰恰是扩容希望被选中的时刻。
- **接缝：扩展 `PlacementCtx` 输入**。runtime 负责查 lease、硬过滤（有效/协议匹配/非满/非压力/非排空/非本节点）与惰性 TTL 缓存（TTL ≈ renewal_interval，决策不阻塞、无后台任务）；策略变为纯决策（p2c 采样 + in-flight 记账），可注入候选集单测分布。
- **默认启用**：所有 Server 默认装配 p2c；单节点部署因硬过滤排除本节点后候选为空，自然退回"就地认领"现有语义。`with_placement_strategy` 保留为显式覆盖入口。

## Considered Options

- **least-loaded（确定性选最空）**：现状 dead code 的逻辑直接接上最简单，但瞬时流量下全部网关涌向同一节点，且旧快照确定性地误导决策；需要 jitter + 阻尼 + 预测叠加才能达到 p2c 的同等效果，复杂度反而更高。
- **纯随机 / 健康过滤 + 随机**：彻底免疫两个约束（不读软负载），但放弃利用既有 lease 负载信息，长期均衡依赖大数定律，无视容量异构与压力梯度。
- **一致性哈希 + 虚拟节点**：确定性分散、不读负载，但放错无法自适应，不解决"负载均衡"问题本身。
- **加权 p2c（按容量加权采样）**：增加复杂度，收益不匹配，被否。
- **新鲜度标记（sampled_at 降权/剔除）**：见 Decision——与 expires_at 过滤重叠，价值边际。

## Consequences

- 默认行为变化：多节点集群的 unowned actor 将从“网关就地认领”变为“转发到他处”；单节点部署行为不变。
- **网关转发防环（实现中修复的既有缺陷）**：网关把“转发来的” SessionOpen 当新入站再次 place_session，默认策略下会 A→B→A 无限循环直到超时（旧 LocalPlacement 空候选就地认领恰好掩盖了该缺陷）。修复：转发时填充 `envelope.from_node`，目标节点对转发来的会话直接 resolve 认领、不二次放置。
- 关闭 ADR-0008 的“Server 放置”开放决策；Client 池均衡与正式非 K8s 发现方案仍为开放决策。
- 跨网关的 herd 残余风险（p2c 偏置最空节点）被接受，由"转发失败剔除候选 + client 重试"自愈；单网关内的 herd 由 in-flight 记账压掉。
- 缓存引入 ≤ ~3.3s 的额外指标滞后，p2c 容忍；缓存误判（漏掉刚排空的节点）由转发失败剔除兜底。
- 无持续 rebalance：放置失衡只能等 passivation + failover 缓慢纠正，主动 Migration 仍是后续独立决策（CONTEXT.md 已预留）。
- 新增 `rand` direct dependency（已在 lockfile 中作为传递依赖）；dead code 的 `placement_candidates` 被新实现取代。
