//! inmem transport：进程内 Envelope 逻辑转发，无 socket、无序列化。
//! 供 `test_support::TestServer` 装配，验证"分布式是主线"下本地测试的形态。

use std::{collections::HashMap, sync::Arc};

use parking_lot::Mutex;
use tokio::sync::mpsc;

use super::{
    ClientTransport, Endpoint, PeerListener, PeerSender, PeerStream, ServerTransport,
    TransportError,
};
use crate::peer_protocol::Envelope;

const INMEM_CHANNEL_CAPACITY: usize = 1024;

/// 进程内端点注册表：`listen` 注册、`connect` 按 key 查表配对。
#[derive(Default)]
pub(crate) struct InmemRegistry {
    endpoints: Mutex<HashMap<String, mpsc::Sender<Box<dyn PeerStream>>>>,
}

impl InmemRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }
}

#[derive(Clone)]
pub(crate) struct InmemTransport {
    registry: Arc<InmemRegistry>,
}

impl InmemTransport {
    pub(crate) fn new(registry: Arc<InmemRegistry>) -> Self {
        Self { registry }
    }
}

pub(crate) struct InmemPeerSender {
    sender: mpsc::Sender<Envelope>,
}

impl PeerSender for InmemPeerSender {
    fn try_send(&self, envelope: Envelope) -> Result<(), TransportError> {
        self.sender.try_send(envelope).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => TransportError::Full,
            mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
        })
    }
}

pub(crate) struct InmemPeerStream {
    sender: Arc<InmemPeerSender>,
    receiver: mpsc::Receiver<Envelope>,
}

#[async_trait::async_trait]
impl PeerStream for InmemPeerStream {
    fn sender(&self) -> Arc<dyn PeerSender> {
        self.sender.clone()
    }

    async fn recv(&mut self) -> Option<Envelope> {
        self.receiver.recv().await
    }
}

pub(crate) struct InmemPeerListener {
    accepted: mpsc::Receiver<Box<dyn PeerStream>>,
    endpoint: String,
    registry: Arc<InmemRegistry>,
}

#[async_trait::async_trait]
impl PeerListener for InmemPeerListener {
    async fn accept(&mut self) -> Option<Box<dyn PeerStream>> {
        self.accepted.recv().await
    }

    fn shutdown(&self) {
        self.registry.endpoints.lock().remove(&self.endpoint);
    }
}

impl ServerTransport for InmemTransport {
    fn listen(
        &self,
        advertised: &Endpoint,
        listener: Option<tokio::net::TcpListener>,
    ) -> Result<Box<dyn PeerListener>, TransportError> {
        debug_assert!(listener.is_none(), "inmem transport has no socket");
        let key = advertised.as_str().to_owned();
        let (accepted_tx, accepted_rx) = mpsc::channel::<Box<dyn PeerStream>>(32);
        if self
            .registry
            .endpoints
            .lock()
            .insert(key.clone(), accepted_tx)
            .is_some()
        {
            return Err(TransportError::BindFailed(format!(
                "inmem endpoint `{key}` already registered"
            )));
        }
        Ok(Box::new(InmemPeerListener {
            accepted: accepted_rx,
            endpoint: key,
            registry: self.registry.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl ClientTransport for InmemTransport {
    async fn connect(&self, endpoint: &Endpoint) -> Result<Box<dyn PeerStream>, TransportError> {
        let key = endpoint.as_str().to_owned();
        let accepted = self
            .registry
            .endpoints
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| TransportError::ConnectFailed(format!("unknown endpoint `{key}`")))?;
        let (client_out_tx, client_out_rx) = mpsc::channel::<Envelope>(INMEM_CHANNEL_CAPACITY);
        let (server_out_tx, server_out_rx) = mpsc::channel::<Envelope>(INMEM_CHANNEL_CAPACITY);
        let server_stream: Box<dyn PeerStream> = Box::new(InmemPeerStream {
            sender: Arc::new(InmemPeerSender {
                sender: server_out_tx,
            }),
            receiver: client_out_rx,
        });
        accepted
            .try_send(server_stream)
            .map_err(|_| TransportError::Closed)?;
        Ok(Box::new(InmemPeerStream {
            sender: Arc::new(InmemPeerSender {
                sender: client_out_tx,
            }),
            receiver: server_out_rx,
        }) as Box<dyn PeerStream>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_protocol::{Envelope, envelope};

    #[tokio::test]
    async fn inmem_transport_forwards_envelopes_both_ways() {
        let registry = InmemRegistry::new();
        let server_transport = InmemTransport::new(registry.clone());
        let client_transport = InmemTransport::new(registry);

        let mut listener = server_transport
            .listen(&Endpoint::new("test-server"), None)
            .expect("listen");

        // 先 connect（把 server 侧流推入 accept 队列），再 accept
        let mut client_stream = client_transport
            .connect(&Endpoint::new("test-server"))
            .await
            .expect("connected");
        let mut server_stream = listener.accept().await.expect("accepted");

        // client → server
        client_stream
            .sender()
            .try_send(Envelope {
                protocol_version: 1,
                actor_type: "echo".into(),
                actor_id: vec![1],
                session_id: vec![0; 16],
                from_node: String::new(),
                kind: Some(envelope::Kind::Action(
                    crate::peer_protocol::ActionMessage {
                        payload: b"ping".to_vec(),
                    },
                )),
            })
            .expect("send");
        let received = server_stream.recv().await.expect("recv");
        assert_eq!(received.actor_type, "echo");

        // server → client（复用入站流 outbound）
        server_stream
            .sender()
            .try_send(Envelope {
                protocol_version: 0,
                actor_type: String::new(),
                actor_id: Vec::new(),
                session_id: vec![0; 16],
                from_node: String::new(),
                kind: Some(envelope::Kind::Event(crate::peer_protocol::EventMessage {
                    payload: b"pong".to_vec(),
                })),
            })
            .expect("send");
        let received = client_stream.recv().await.expect("recv");
        assert!(matches!(received.kind, Some(envelope::Kind::Event(_))));

        // shutdown：accept 返回 None（endpoint 注销）
        listener.shutdown();
        let mut listener = listener;
        assert!(listener.accept().await.is_none());
    }
}
