//! 放置策略（crate 私有）：网关对未拥有 Actor 决定"放哪"。
//!
//! runtime 收集候选（Node Lease 快照 + 硬过滤 + 惰性缓存），策略是纯决策。
//! 默认 p2c（power of two choices）：随机抽 2 个候选，选 Load Ratio 低的；
//! 配合本地 In-flight Placement 记账防止同一网关内并发放置的 herd（ADR-0009）。

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use async_trait::async_trait;
use parking_lot::Mutex;
use rand::{RngCore, SeedableRng, rngs::StdRng};

use crate::{ActorAddress, transport::Endpoint};

/// In-flight 记账的过期窗口：一个 lease 刷新周期（≈ 默认 renewal_interval）。
/// 窗口内"已决定但未反映到 lease"的放置计入预测负载；窗口过后 lease 已反映，
/// 计数自然归零，避免成功放置被永久高估。
const IN_FLIGHT_TTL: Duration = Duration::from_secs(3);

/// 放置决策上下文：runtime 已硬过滤后的候选节点（Load Ratio 输入）。
pub(crate) struct PlacementCtx {
    pub candidates: Vec<Candidate>,
}

/// 候选节点：端点 + 负载快照（Load Ratio = active/max 的输入）。
#[derive(Clone)]
pub(crate) struct Candidate {
    pub endpoint: Endpoint,
    pub active_actor_count: usize,
    pub max_actor_count: usize,
}

/// 返回有序候选（不含本节点）；空列表 = 就地认领（与无算法时行为一致）。
#[async_trait]
pub(crate) trait PlacementStrategy: Send + Sync {
    async fn candidates(&self, address: &ActorAddress, ctx: &PlacementCtx) -> Vec<Endpoint>;
    /// 网关转发失败后回调：扣减 in-flight 记账。默认无操作。
    fn on_placement_failed(&self, _endpoint: &Endpoint) {}
}

/// 默认：p2c 均衡放置 + in-flight 记账。
pub(crate) struct P2cPlacement {
    rng: Mutex<Box<dyn RngCore + Send>>,
    in_flight: Mutex<HashMap<String, (usize, Instant)>>,
}

impl P2cPlacement {
    pub(crate) fn new() -> Self {
        Self {
            rng: Mutex::new(Box::new(StdRng::from_os_rng())),
            in_flight: Mutex::new(HashMap::new()),
        }
    }

    /// 测试：注入确定性随机源。
    #[cfg(test)]
    pub(crate) fn with_rng(rng: Box<dyn RngCore + Send>) -> Self {
        Self {
            rng: Mutex::new(rng),
            in_flight: Mutex::new(HashMap::new()),
        }
    }
}

impl P2cPlacement {
    fn predicted_ratio(
        candidate: &Candidate,
        in_flight: &HashMap<String, (usize, Instant)>,
    ) -> f64 {
        let extra = in_flight
            .get(candidate.endpoint.as_str())
            .map_or(0, |(count, _)| *count);
        (candidate.active_actor_count as f64 + extra as f64)
            / candidate.max_actor_count.max(1) as f64
    }

    fn pick(&self, ctx: &PlacementCtx) -> Vec<Endpoint> {
        let n = ctx.candidates.len();
        if n == 0 {
            return Vec::new();
        }
        let mut rng = self.rng.lock();
        let mut in_flight = self.in_flight.lock();
        let now = Instant::now();
        in_flight.retain(|_, (_, updated)| now.duration_since(*updated) < IN_FLIGHT_TTL);
        let first = (rng.next_u64() as usize) % n;
        let mut second = (rng.next_u64() as usize) % n;
        while n > 1 && second == first {
            second = (rng.next_u64() as usize) % n;
        }
        let mut picked = vec![first];
        if second != first {
            picked.push(second);
        }
        picked.sort_by(|&left, &right| {
            Self::predicted_ratio(&ctx.candidates[left], &in_flight)
                .partial_cmp(&Self::predicted_ratio(&ctx.candidates[right], &in_flight))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        // 记账：选中的目标 +1（成功保留至窗口过期；失败由 on_placement_failed 扣回）。
        let target = &ctx.candidates[picked[0]];
        in_flight
            .entry(target.endpoint.as_str().to_owned())
            .and_modify(|(count, updated)| {
                *count += 1;
                *updated = now;
            })
            .or_insert((1, now));
        picked
            .into_iter()
            .map(|index| ctx.candidates[index].endpoint.clone())
            .collect()
    }
}

#[async_trait]
impl PlacementStrategy for P2cPlacement {
    async fn candidates(&self, _address: &ActorAddress, ctx: &PlacementCtx) -> Vec<Endpoint> {
        self.pick(ctx)
    }

    fn on_placement_failed(&self, endpoint: &Endpoint) {
        self.in_flight
            .lock()
            .entry(endpoint.as_str().to_owned())
            .and_modify(|(count, _)| *count = count.saturating_sub(1));
    }
}

pub(crate) fn default_placement() -> Arc<dyn PlacementStrategy> {
    Arc::new(P2cPlacement::new())
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{SeedableRng, rngs::StdRng};

    use crate::{ActorAddress, ActorId};

    fn candidate(endpoint: &str, active: usize, max: usize) -> Candidate {
        Candidate {
            endpoint: Endpoint::new(endpoint.to_owned()),
            active_actor_count: active,
            max_actor_count: max,
        }
    }

    fn ctx(candidates: Vec<Candidate>) -> PlacementCtx {
        PlacementCtx { candidates }
    }

    fn address() -> ActorAddress {
        ActorAddress::new("test", ActorId::from("1"))
    }

    /// 两个候选：选 Load Ratio 低的（与采样顺序无关）。
    #[tokio::test]
    async fn picks_less_loaded_of_two() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(1)));
        let c = ctx(vec![candidate("node-a", 90, 100), candidate("node-b", 10, 100)]);
        let picked = placement.candidates(&address(), &c).await;
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0], Endpoint::new("node-b"), "低负载优先");
    }

    /// 异构容量：数量多但容量大（低 ratio）优先于数量少但接近满载。
    #[tokio::test]
    async fn ratio_beats_absolute_count() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(2)));
        let c = ctx(vec![candidate("node-big", 90, 1000), candidate("node-small", 9, 10)]);
        let picked = placement.candidates(&address(), &c).await;
        assert_eq!(picked[0], Endpoint::new("node-big"), "90/1000 < 9/10");
    }

    #[tokio::test]
    async fn single_candidate_is_picked() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(3)));
        let c = ctx(vec![candidate("node-a", 10, 100)]);
        let picked = placement.candidates(&address(), &c).await;
        assert_eq!(picked, vec![Endpoint::new("node-a")]);
    }

    #[tokio::test]
    async fn no_candidates_returns_empty() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(4)));
        let picked = placement.candidates(&address(), &ctx(Vec::new())).await;
        assert!(picked.is_empty(), "空候选 = 就地认领");
    }

    /// in-flight 记账：等负载时首选中者记账抬高，第二次决策转向另一节点。
    #[tokio::test]
    async fn in_flight_bookkeeping_steers_away_from_target() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(5)));
        let c = ctx(vec![candidate("node-a", 10, 100), candidate("node-b", 10, 100)]);
        let first = placement.candidates(&address(), &c).await;
        let second = placement.candidates(&address(), &c).await;
        assert_ne!(first[0], second[0], "已选节点被记账抬高后不再重复选中");
    }

    /// 失败回滚：on_placement_failed 扣回记账，预测负载恢复。
    #[tokio::test]
    async fn failed_placement_rolls_back_bookkeeping() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(6)));
        let c = ctx(vec![candidate("node-a", 10, 100), candidate("node-b", 10, 100)]);
        let picked = placement.candidates(&address(), &c).await;
        placement.on_placement_failed(&picked[0]);
        let in_flight = placement.in_flight.lock();
        assert_eq!(in_flight.get(picked[0].as_str()).copied().unwrap_or((0, Instant::now())).0, 0);
    }
}
