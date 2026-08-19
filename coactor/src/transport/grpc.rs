//! gRPC transport implementation for multiplexed Client-to-Server bidi Envelope streams.
//! prost 序列化只存在于本模块。

use std::{pin::Pin, sync::Arc, time::Duration};

use futures_util::{Stream, StreamExt};
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};

use super::{
    ClientTransport, Endpoint, ServerTransport, TransportError, TransportListener, TransportSender,
    TransportStream,
};
use crate::transport_protocol;

pub(crate) const TRANSPORT_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone, Copy)]
pub(crate) struct GrpcTransport {
    pub(crate) transport_connect_timeout: Duration,
}

impl GrpcTransport {
    pub(crate) fn server() -> Self {
        Self {
            transport_connect_timeout: Duration::ZERO,
        }
    }

    pub(crate) fn new(transport_connect_timeout: Duration) -> Self {
        Self {
            transport_connect_timeout,
        }
    }
}

pub(crate) struct GrpcTransportSender {
    sender: Mutex<Option<mpsc::Sender<transport_protocol::Envelope>>>,
}

impl TransportSender for GrpcTransportSender {
    fn try_send(&self, envelope: transport_protocol::Envelope) -> Result<(), TransportError> {
        self.sender
            .lock()
            .as_ref()
            .ok_or(TransportError::Closed)?
            .try_send(envelope)
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => TransportError::Full,
                mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
            })
    }

    fn close(&self) {
        self.sender.lock().take();
    }
}

pub(crate) struct GrpcTransportStream {
    sender: Arc<GrpcTransportSender>,
    inbound: Streaming<transport_protocol::Envelope>,
}

#[async_trait::async_trait]
impl TransportStream for GrpcTransportStream {
    fn sender(&self) -> Arc<dyn TransportSender> {
        self.sender.clone()
    }

    async fn recv(&mut self) -> Option<transport_protocol::Envelope> {
        self.inbound.next().await.and_then(|item| item.ok())
    }
}

/// 入站连接等待队列：`exchange()` 把每个新连接包装成 `TransportStream` 推进来。
pub(crate) struct GrpcTransportListener {
    accepted: mpsc::Receiver<Box<dyn TransportStream>>,
    shutdown: watch::Sender<bool>,
}

#[async_trait::async_trait]
impl TransportListener for GrpcTransportListener {
    async fn accept(&mut self) -> Option<Box<dyn TransportStream>> {
        self.accepted.recv().await
    }

    fn shutdown(&self) {
        let _ = self.shutdown.send(true);
    }
}

impl Drop for GrpcTransportListener {
    fn drop(&mut self) {
        // 发送端随 listener drop 而关闭：serve 的 shutdown future 因此结束，停止接受。
        let _ = self.shutdown.send(true);
    }
}

struct GrpcTransportService {
    accepted: mpsc::Sender<Box<dyn TransportStream>>,
}

#[tonic::async_trait]
impl transport_protocol::transport_server::Transport for GrpcTransportService {
    type ExchangeStream =
        Pin<Box<dyn Stream<Item = Result<transport_protocol::Envelope, Status>> + Send>>;

    async fn exchange(
        &self,
        request: Request<Streaming<transport_protocol::Envelope>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let inbound = request.into_inner();
        let (outbound_tx, outbound_rx) = mpsc::channel::<transport_protocol::Envelope>(1);
        let stream: Box<dyn TransportStream> = Box::new(GrpcTransportStream {
            sender: Arc::new(GrpcTransportSender {
                sender: Mutex::new(Some(outbound_tx)),
            }),
            inbound,
        });
        self.accepted
            .try_send(stream)
            .map_err(|_| Status::resource_exhausted("transport accept backlog full"))?;
        let outbound = ReceiverStream::new(outbound_rx).map(Ok::<_, Status>);
        Ok(Response::new(Box::pin(outbound)))
    }
}

impl ServerTransport for GrpcTransport {
    fn listen(
        &self,
        _advertised: &Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> Result<Box<dyn TransportListener>, TransportError> {
        let listener = listener.ok_or(TransportError::BindFailed(
            "grpc transport requires a bound socket".into(),
        ))?;
        let (accepted_tx, accepted_rx) = mpsc::channel::<Box<dyn TransportStream>>(32);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let service = GrpcTransportService {
            accepted: accepted_tx,
        };
        let task = tokio::spawn(async move {
            let serve = tonic::transport::Server::builder()
                .add_service(transport_protocol::transport_server::TransportServer::new(
                    service,
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                    let mut rx = shutdown_rx;
                    let _ = rx.changed().await;
                });
            let _ = serve.await;
        });
        drop(task);
        Ok(Box::new(GrpcTransportListener {
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
    ) -> Result<Box<dyn TransportStream>, TransportError> {
        let endpoint_str = endpoint.as_str();
        let endpoint_str = if endpoint_str.contains("://") {
            endpoint_str.to_owned()
        } else {
            format!("http://{endpoint_str}")
        };
        let endpoint = tonic::transport::Endpoint::new(endpoint_str.clone())
            .map_err(|error| TransportError::ConnectFailed(error.to_string()))?;
        let channel = tokio::time::timeout(self.transport_connect_timeout, endpoint.connect())
            .await
            .map_err(|_| TransportError::ConnectFailed("connect timeout".into()))?
            .map_err(|error| TransportError::ConnectFailed(error.to_string()))?;
        let mut client = transport_protocol::transport_client::TransportClient::new(channel);
        let (outbound_tx, outbound_rx) =
            mpsc::channel::<transport_protocol::Envelope>(TRANSPORT_CHANNEL_CAPACITY);
        let inbound = transport_protocol::transport_client::TransportClient::exchange(
            &mut client,
            Request::new(ReceiverStream::new(outbound_rx)),
        )
        .await
        .map_err(|error| TransportError::ConnectFailed(error.to_string()))?
        .into_inner();
        Ok(Box::new(GrpcTransportStream {
            sender: Arc::new(GrpcTransportSender {
                sender: Mutex::new(Some(outbound_tx)),
            }),
            inbound,
        }) as Box<dyn TransportStream>)
    }
}
