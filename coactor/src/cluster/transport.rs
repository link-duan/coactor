use std::{pin::Pin, sync::Arc, time::Duration};

use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::{SendError, __macro::RuntimeInner, peer_protocol};

pub(crate) const PEER_CHANNEL_CAPACITY: usize = 1024;

/// server 侧：处理对方连入的 bidi 流。出站方向保持打开直到入站结束。
pub(crate) struct PeerService<S> {
    pub(crate) runtime: Arc<RuntimeInner<S>>,
}

#[tonic::async_trait]
impl<S> peer_protocol::peer_server::Peer for PeerService<S>
where
    S: Send + Sync + 'static,
{
    type ExchangeStream =
        Pin<Box<dyn Stream<Item = Result<peer_protocol::Envelope, Status>> + Send>>;

    async fn exchange(
        &self,
        request: Request<Streaming<peer_protocol::Envelope>>,
    ) -> Result<Response<Self::ExchangeStream>, Status> {
        let mut inbound = request.into_inner();
        let runtime = self.runtime.clone();
        let (outbound_tx, outbound_rx) = mpsc::channel::<peer_protocol::Envelope>(1);
        let task_runtime = runtime.clone();
        let task = tokio::spawn(async move {
            let runtime = task_runtime;
            while let Some(Ok(envelope)) = inbound.next().await {
                runtime
                    .dispatch_inbound(envelope, Some(outbound_tx.clone()))
                    .await;
            }
            drop(outbound_tx);
        });
        runtime.register_inbound_task(task.abort_handle());
        let outbound =
            ReceiverStream::new(outbound_rx).map(Ok::<peer_protocol::Envelope, Status>);
        Ok(Response::new(Box::pin(outbound)))
    }
}

/// client 侧：建立到对端节点的 bidi 流并复用；返回本端发送端。
pub(crate) async fn connect_channel<S>(
    runtime: &Arc<RuntimeInner<S>>,
    endpoint: &str,
) -> Result<mpsc::Sender<peer_protocol::Envelope>, SendError>
where
    S: Send + Sync + 'static,
{
    let connect_timeout = runtime
        .cluster
        .as_ref()
        .map_or(Duration::from_secs(3), |cluster| cluster.peer_connect_timeout);
    let closed_endpoint = endpoint.to_owned();
    let endpoint = tonic::transport::Endpoint::new(endpoint.to_owned())
        .map_err(|_| SendError::RemoteUnavailable)?;
    let channel = tokio::time::timeout(connect_timeout, endpoint.connect())
        .await
        .map_err(|_| SendError::RemoteUnavailable)?
        .map_err(|_| SendError::RemoteUnavailable)?;
    let mut client = peer_protocol::peer_client::PeerClient::new(channel);
    let (outbound_tx, outbound_rx) =
        mpsc::channel::<peer_protocol::Envelope>(PEER_CHANNEL_CAPACITY);
    let inbound = peer_protocol::peer_client::PeerClient::exchange(
        &mut client,
        Request::new(ReceiverStream::new(outbound_rx)),
    )
    .await
    .map_err(|_| SendError::RemoteUnavailable)?
    .into_inner();
    let runtime = runtime.clone();
    tokio::spawn(async move {
        let mut inbound = inbound;
        while let Some(Ok(envelope)) = inbound.next().await {
            runtime.dispatch_inbound(envelope, None).await;
        }
        runtime.notify_channel_closed(&closed_endpoint).await;
    });
    Ok(outbound_tx)
}
