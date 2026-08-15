//! gRPC transport 实现：node-pair 多路复用的 bidi Envelope 流（ADR-0006）。
//! prost 序列化只存在于本模块。

use std::{pin::Pin, sync::Arc, time::Duration};

use futures_util::{Stream, StreamExt};
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::{
    ClientTransport, Endpoint, PeerListener, PeerSender, PeerStream, ServerTransport,
    TransportError,
};
use crate::peer_protocol;

pub(crate) const PEER_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone, Copy)]
pub(crate) struct GrpcTransport {
    pub(crate) peer_connect_timeout: Duration,
}

impl GrpcTransport {
    pub(crate) fn new(peer_connect_timeout: Duration) -> Self {
        Self {
            peer_connect_timeout,
        }
    }
}

pub(crate) struct GrpcPeerSender {
    sender: mpsc::Sender<peer_protocol::Envelope>,
}

impl PeerSender for GrpcPeerSender {
    fn try_send(&self, envelope: peer_protocol::Envelope) -> Result<(), TransportError> {
        self.sender
            .try_send(envelope)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => TransportError::Full,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            })
    }
}

pub(crate) struct GrpcPeerStream {
    sender: Arc<GrpcPeerSender>,
    inbound: Streaming<peer_protocol::Envelope>,
}

#[async_trait::async_trait]
impl PeerStream for GrpcPeerStream {
    fn sender(&self) -> Arc<dyn PeerSender> {
        self.sender.clone()
    }

    async fn recv(&mut self) -> Option<peer_protocol::Envelope> {
        self.inbound.next().await.and_then(|item| item.ok())
    }
}

/// 入站连接等待队列：`exchange()` 把每个新连接包装成 `PeerStream` 推进来。
pub(crate) struct GrpcPeerListener {
    accepted: mpsc::Receiver<Box<dyn PeerStream>>,
    shutdown: watch::Sender<bool>,
}

#[async_trait::async_trait]
impl PeerListener for GrpcPeerListener {
    async fn accept(&mut self) -> Option<Box<dyn PeerStream>> {
        self.accepted.recv().await
    }

    fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

impl Drop for GrpcPeerListener {
    fn drop(&mut self) {
        // 发送端随 listener drop 而关闭：serve 的 shutdown future 因此结束，停止接受。
        let _ = self.shutdown.send(true);
    }
}

struct GrpcPeerService {
    accepted: mpsc::Sender<Box<dyn PeerStream>>,
}

#[tonic::async_trait]
impl peer_protocol::peer_server::Peer for GrpcPeerService {
    type ExchangeStream =
        Pin<Box<dyn Stream<Item = Result<peer_protocol::Envelope, Status>> + Send>>;

    async fn exchange(
        &self,
        request: Request<Streaming<peer_protocol::Envelope>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let inbound = request.into_inner();
        let (outbound_tx, outbound_rx) = mpsc::channel::<peer_protocol::Envelope>(1);
        let stream: Box<dyn PeerStream> = Box::new(GrpcPeerStream {
            sender: Arc::new(GrpcPeerSender { sender: outbound_tx }),
            inbound,
        });
        self.accepted
            .try_send(stream)
            .map_err(|_| Status::resource_exhausted("peer accept backlog full"))?;
        let outbound = ReceiverStream::new(outbound_rx).map(Ok::<_, Status>);
        Ok(Response::new(Box::pin(outbound)))
    }
}

impl ServerTransport for GrpcTransport {
    fn listen(
        &self,
        _advertised: &Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> Result<Box<dyn PeerListener>, TransportError> {
        let listener = listener.ok_or(TransportError::BindFailed(
            "grpc transport requires a bound socket".into(),
        ))?;
        let (accepted_tx, accepted_rx) = mpsc::channel::<Box<dyn PeerStream>>(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = GrpcPeerService {
            accepted: accepted_tx,
        };
        let task = tokio::spawn(async move {
            let serve = tonic::transport::Server::builder()
                .add_service(peer_protocol::peer_server::PeerServer::new(service))
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    async move {
                        let mut rx = shutdown_rx;
                        let _ = rx.changed().await;
                    },
                );
            let _ = serve.await;
        });
        let _ = task;
        Ok(Box::new(GrpcPeerListener {
            accepted: accepted_rx,
            shutdown: shutdown_tx,
        }))
    }
}

#[async_trait::async_trait]
impl ClientTransport for GrpcTransport {
    async fn connect(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Box<dyn PeerStream>, TransportError> {
        let endpoint_str = endpoint.as_str().to_owned();
        let endpoint = tonic::transport::Endpoint::new(endpoint_str.clone())
            .map_err(|error| TransportError::ConnectFailed(error.to_string()))?;
        let channel = tokio::time::timeout(self.peer_connect_timeout, endpoint.connect())
            .await
            .map_err(|_| TransportError::ConnectFailed("connect timeout".into()))?
            .map_err(|error| TransportError::ConnectFailed(error.to_string()))?;
        let mut client = peer_protocol::peer_client::PeerClient::new(channel);
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<peer_protocol::Envelope>(PEER_CHANNEL_CAPACITY);
        let inbound = peer_protocol::peer_client::PeerClient::exchange(
            &mut client,
            Request::new(ReceiverStream::new(outbound_rx)),
        )
        .await
        .map_err(|error| TransportError::ConnectFailed(error.to_string()))?
        .into_inner();
        Ok(Box::new(GrpcPeerStream {
            sender: Arc::new(GrpcPeerSender { sender: outbound_tx }),
            inbound,
        }) as Box<dyn PeerStream>)
    }
}
