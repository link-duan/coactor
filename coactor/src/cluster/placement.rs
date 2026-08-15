//! 放置策略接缝（crate 私有）：网关对未拥有 Actor 决定"放哪"。
//!
//! 均衡算法（least-loaded、p2c 等）是独立开放决策；本模块只提供接缝与默认
//! 实现（空候选 = 就地认领，与无算法时行为一致）。算法后续以该接缝接入，
//! 接入点仅 Server 侧。

use std::sync::Arc;

use async_trait::async_trait;

use crate::{ActorAddress, transport::Endpoint};

/// 返回有序候选端点（不含本节点）；空列表 = 就地认领。
#[async_trait]
pub(crate) trait PlacementStrategy: Send + Sync {
    async fn candidates(&self, address: &ActorAddress) -> Vec<Endpoint>;
}

/// 默认：不在他处放置，网关就地认领。
#[derive(Default)]
pub(crate) struct LocalPlacement;

#[async_trait]
impl PlacementStrategy for LocalPlacement {
    async fn candidates(&self, _address: &ActorAddress) -> Vec<Endpoint> {
        Vec::new()
    }
}

pub(crate) fn default_placement() -> Arc<dyn PlacementStrategy> {
    Arc::new(LocalPlacement)
}
