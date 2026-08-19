//! Client 对未拥有 Actor 执行 Placement。
//!
//! Client 从 Node Directory 收集并硬过滤候选，策略只负责排序。默认 p2c
//!（power of two choices）随机抽两个候选并选择较低 Load Ratio，同时使用
//! Client 本地 In-flight Placement 记账缓解并发 herd。

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;
use rand::{RngCore, SeedableRng, rngs::StdRng};

use crate::ActorAddress;

/// In-flight 记账的过期窗口：一个 lease 刷新周期（≈ 默认 renewal_interval）。
/// 窗口内"已决定但未反映到 lease"的放置计入预测负载；窗口过后 lease 已反映，
/// 计数自然归零，避免成功放置被永久高估。
const IN_FLIGHT_TTL: Duration = Duration::from_secs(3);

/// 放置决策上下文：runtime 已硬过滤后的候选节点（Load Ratio 输入）。
pub struct PlacementContext {
    pub candidates: Vec<PlacementCandidate>,
}

/// 候选节点：端点 + 负载快照（Load Ratio = active/max 的输入）。
#[derive(Clone)]
pub struct PlacementCandidate {
    pub endpoint: String,
    pub active_actor_count: usize,
    pub max_actor_count: usize,
}

/// 返回有序候选；空列表表示当前没有可尝试的 Server。
pub trait PlacementStrategy: Send + Sync {
    fn candidates(&self, address: &ActorAddress, ctx: &PlacementContext) -> Vec<String>;
    /// Placement 失败后回调：扣减 in-flight 记账。默认无操作。
    fn on_placement_failed(&self, _endpoint: &str) {}
}

/// 默认：p2c 均衡放置 + in-flight 记账。
pub struct P2cPlacement {
    rng: Mutex<Box<dyn RngCore + Send>>,
    in_flight: Mutex<HashMap<String, (usize, Instant)>>,
}

impl Default for P2cPlacement {
    fn default() -> Self {
        Self::new()
    }
}

impl P2cPlacement {
    pub fn new() -> Self {
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
        candidate: &PlacementCandidate,
        in_flight: &HashMap<String, (usize, Instant)>,
    ) -> f64 {
        let extra = in_flight
            .get(candidate.endpoint.as_str())
            .map_or(0, |(count, _)| *count);
        (candidate.active_actor_count as f64 + extra as f64)
            / candidate.max_actor_count.max(1) as f64
    }

    fn pick(&self, ctx: &PlacementContext) -> Vec<String> {
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

impl PlacementStrategy for P2cPlacement {
    fn candidates(&self, _address: &ActorAddress, ctx: &PlacementContext) -> Vec<String> {
        self.pick(ctx)
    }

    fn on_placement_failed(&self, endpoint: &str) {
        self.in_flight
            .lock()
            .entry(endpoint.to_owned())
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

    use crate::ActorAddress;

    fn candidate(endpoint: &str, active: usize, max: usize) -> PlacementCandidate {
        PlacementCandidate {
            endpoint: endpoint.to_owned(),
            active_actor_count: active,
            max_actor_count: max,
        }
    }

    fn ctx(candidates: Vec<PlacementCandidate>) -> PlacementContext {
        PlacementContext { candidates }
    }

    fn address() -> ActorAddress {
        ActorAddress::new("test", "one").unwrap()
    }

    /// 两个候选：选 Load Ratio 低的（与采样顺序无关）。
    #[tokio::test]
    async fn picks_less_loaded_of_two() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(1)));
        let c = ctx(vec![
            candidate("node-a", 90, 100),
            candidate("node-b", 10, 100),
        ]);
        let picked = placement.candidates(&address(), &c);
        assert_eq!(picked.len(), 2);
        assert_eq!(picked[0], "node-b", "低负载优先");
    }

    /// 异构容量：数量多但容量大（低 ratio）优先于数量少但接近满载。
    #[tokio::test]
    async fn ratio_beats_absolute_count() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(2)));
        let c = ctx(vec![
            candidate("node-big", 90, 1000),
            candidate("node-small", 9, 10),
        ]);
        let picked = placement.candidates(&address(), &c);
        assert_eq!(picked[0], "node-big", "90/1000 < 9/10");
    }

    #[tokio::test]
    async fn single_candidate_is_picked() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(3)));
        let c = ctx(vec![candidate("node-a", 10, 100)]);
        let picked = placement.candidates(&address(), &c);
        assert_eq!(picked, vec!["node-a"]);
    }

    #[tokio::test]
    async fn no_candidates_returns_empty() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(4)));
        let picked = placement.candidates(&address(), &ctx(Vec::new()));
        assert!(picked.is_empty(), "空候选 = 就地认领");
    }

    /// in-flight 记账：等负载时首选中者记账抬高，第二次决策转向另一节点。
    #[tokio::test]
    async fn in_flight_bookkeeping_steers_away_from_target() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(5)));
        let c = ctx(vec![
            candidate("node-a", 10, 100),
            candidate("node-b", 10, 100),
        ]);
        let first = placement.candidates(&address(), &c);
        let second = placement.candidates(&address(), &c);
        assert_ne!(first[0], second[0], "已选节点被记账抬高后不再重复选中");
    }

    /// 失败回滚：on_placement_failed 扣回记账，预测负载恢复。
    #[tokio::test]
    async fn failed_placement_rolls_back_bookkeeping() {
        let placement = P2cPlacement::with_rng(Box::new(StdRng::seed_from_u64(6)));
        let c = ctx(vec![
            candidate("node-a", 10, 100),
            candidate("node-b", 10, 100),
        ]);
        let picked = placement.candidates(&address(), &c);
        placement.on_placement_failed(&picked[0]);
        let in_flight = placement.in_flight.lock();
        assert_eq!(
            in_flight
                .get(picked[0].as_str())
                .copied()
                .unwrap_or((0, Instant::now()))
                .0,
            0
        );
    }
}
