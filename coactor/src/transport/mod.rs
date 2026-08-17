//! crate 私有 transport seam：Server/Client 两侧消息传输的对称抽象。
//!
//! 方向不对称（ADR-0008）：Client 只 connect，Server 只 accept；两侧共用
//! 双向 `PeerStream`（`peer_protocol::Envelope` 为消息单元）。`grpc` 是当前
//! 唯一实现，`inmem`（进程内逻辑转发）供 `test_support` 使用。
//!
//! trait 方法使用原生 `async fn`：依赖 `Send` supertrait 让 future 自动 `Send`
//! （与 ADR-0007 对 `Actor` trait 的处理一致）。

pub(crate) mod grpc;
// `inmem` 目前仅测试使用，`test_support::TestServer` 落地后移除本标注。
#[allow(dead_code)]
pub(crate) mod inmem;

use std::sync::Arc;

use crate::peer_protocol::Envelope;

/// 对端节点地址：gRPC 为 `http://host:port`，inmem 为进程内 registry key。
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Endpoint(pub(crate) String);

impl Endpoint {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TransportError {
    ConnectFailed(String),
    BindFailed(String),
    Closed,
    Full,
}

impl std::fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectFailed(detail) => write!(formatter, "connect failed: {detail}"),
            Self::BindFailed(detail) => write!(formatter, "bind failed: {detail}"),
            Self::Closed => formatter.write_str("stream closed"),
            Self::Full => formatter.write_str("send buffer full"),
        }
    }
}

impl std::error::Error for TransportError {}

/// 双向流的可克隆出站半部；owner 侧用它把 Event/ack 回传到 caller。
pub(crate) trait PeerSender: Send + Sync {
    fn try_send(&self, envelope: Envelope) -> Result<(), TransportError>;
}

/// 双向 Envelope 流：caller 建立或 owner accept 后，两侧都经它收发。
#[async_trait::async_trait]
pub(crate) trait PeerStream: Send {
    fn sender(&self) -> Arc<dyn PeerSender>;

    async fn recv(&mut self) -> Option<Envelope>;
}

/// Server 半部：绑定监听并逐个 accept 入站流；`shutdown` 停止接受新连接。
#[async_trait::async_trait]
pub(crate) trait PeerListener: Send {
    async fn accept(&mut self) -> Option<Box<dyn PeerStream>>;

    fn shutdown(&self);
}

pub(crate) trait ServerTransport: Send + Sync {
    /// 绑定并接受入站流。`listener` 仅 gRPC 需要（已绑定的 socket）；
    /// `advertised` 是发布/寻址用的端点标识（inmem 用它作 registry key）。
    fn listen(
        &self,
        advertised: &Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> Result<Box<dyn PeerListener>, TransportError>;
}

#[async_trait::async_trait]
pub(crate) trait ClientTransport: Send + Sync {
    async fn connect(&self, endpoint: &Endpoint) -> Result<Box<dyn PeerStream>, TransportError>;
}
