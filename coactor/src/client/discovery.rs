//! 服务发现：Client 获取候选 Server（网关）节点列表的公开机制（ADR-0008）。
//!
//! 发现不是正确性边界——发现错了只会连不上/连错节点，可重发现恢复，不破坏
//! 串行化与 fencing；因此 trait 公开，consumer 可在非 K8s 环境自实现接入。

use std::sync::Arc;

use crate::transport::Endpoint;

#[derive(Clone, Debug)]
pub enum DiscoveryError {
    ResolveFailed(String),
}

impl std::fmt::Display for DiscoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ResolveFailed(detail) => write!(formatter, "discovery failed: {detail}"),
        }
    }
}

impl std::error::Error for DiscoveryError {}

/// 候选 Server 节点列表（poll 式）；Client 池据此建池并按会话分配网关节点。
#[async_trait::async_trait]
pub trait ServiceDiscovery: Send + Sync {
    async fn resolve(&self) -> Result<Vec<Endpoint>, DiscoveryError>;
}

/// 静态节点列表：测试、本地部署与兜底。
#[derive(Clone)]
pub struct StaticListDiscovery {
    endpoints: Vec<Endpoint>,
}

impl StaticListDiscovery {
    pub fn new(endpoints: Vec<Endpoint>) -> Arc<Self> {
        Arc::new(Self { endpoints })
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for StaticListDiscovery {
    async fn resolve(&self) -> Result<Vec<Endpoint>, DiscoveryError> {
        Ok(self.endpoints.clone())
    }
}

/// DNS 名解析：对 `host:port` 做多 A 记录解析（覆盖 K8s headless service 与
/// 非 K8s DNS 场景），每个解析结果作为一个候选网关节点。
#[derive(Clone)]
pub struct DnsDiscovery {
    name: String,
}

impl DnsDiscovery {
    pub fn new(name: impl Into<String>) -> Arc<Self> {
        Arc::new(Self { name: name.into() })
    }
}

#[async_trait::async_trait]
impl ServiceDiscovery for DnsDiscovery {
    async fn resolve(&self) -> Result<Vec<Endpoint>, DiscoveryError> {
        let addresses = tokio::net::lookup_host(self.name.as_str())
            .await
            .map_err(|error| DiscoveryError::ResolveFailed(error.to_string()))?;
        let mut endpoints: Vec<Endpoint> = addresses
            .map(|address| Endpoint::new(format!("http://{address}")))
            .collect();
        endpoints.sort();
        endpoints.dedup();
        Ok(endpoints)
    }
}
