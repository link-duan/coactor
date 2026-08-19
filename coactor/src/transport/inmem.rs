//! In-memory Transport: logical Envelope forwarding without sockets or serialization.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::{mpsc, watch};

use super::{
    ClientTransport, Endpoint, ServerTransport, TransportError, TransportListener, TransportSender,
    TransportStream,
};
use crate::transport_protocol::Envelope;

const INMEM_CHANNEL_CAPACITY: usize = 1024;

#[derive(Clone)]
struct RecordedEnvelope {
    connection_id: u64,
    session_id: Vec<u8>,
}

struct InmemConnection {
    endpoint: String,
    closed: watch::Sender<bool>,
    flag: Arc<AtomicBool>,
}

/// In-memory endpoint registry and test recording seam.
#[derive(Default)]
pub(crate) struct InmemRegistry {
    endpoints: Mutex<HashMap<String, mpsc::Sender<Box<dyn TransportStream>>>>,
    next_connection_id: AtomicU64,
    connections: Mutex<HashMap<u64, InmemConnection>>,
    envelopes: Mutex<Vec<RecordedEnvelope>>,
}

impl InmemRegistry {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    #[cfg(test)]
    pub(crate) fn connection_ids(&self, endpoint: &str) -> Vec<u64> {
        self.connections
            .lock()
            .iter()
            .filter_map(|(id, connection)| (connection.endpoint == endpoint).then_some(*id))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn session_connection_ids(&self, session_id: crate::SessionId) -> Vec<u64> {
        let bytes = session_id.as_bytes();
        let mut ids = self
            .envelopes
            .lock()
            .iter()
            .filter_map(|record| (record.session_id == bytes).then_some(record.connection_id))
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids.dedup();
        ids
    }

    #[cfg(test)]
    pub(crate) fn connection_session_counts(&self, endpoint: &str) -> Vec<usize> {
        let connections = self.connection_ids(endpoint);
        let records = self.envelopes.lock();
        let mut counts = connections
            .into_iter()
            .map(|connection_id| {
                records
                    .iter()
                    .filter(|record| record.connection_id == connection_id)
                    .map(|record| record.session_id.clone())
                    .collect::<std::collections::HashSet<_>>()
                    .len()
            })
            .collect::<Vec<_>>();
        counts.sort_unstable();
        counts
    }

    #[cfg(test)]
    pub(crate) fn close_connection(&self, connection_id: u64) {
        if let Some(connection) = self.connections.lock().remove(&connection_id) {
            connection.flag.store(true, Ordering::Release);
            let _ = connection.closed.send(true);
        }
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

pub(crate) struct InmemTransportSender {
    sender: mpsc::Sender<Envelope>,
    registry: Arc<InmemRegistry>,
    connection_id: u64,
    record: bool,
    closed: Arc<AtomicBool>,
}

impl TransportSender for InmemTransportSender {
    fn try_send(&self, envelope: Envelope) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(TransportError::Closed);
        }
        if self.record {
            self.registry.envelopes.lock().push(RecordedEnvelope {
                connection_id: self.connection_id,
                session_id: envelope.session_id.clone(),
            });
        }
        self.sender.try_send(envelope).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => TransportError::Full,
            mpsc::error::TrySendError::Closed(_) => TransportError::Closed,
        })
    }

    fn close(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            if let Some(connection) = self.registry.connections.lock().remove(&self.connection_id) {
                connection.flag.store(true, Ordering::Release);
                let _ = connection.closed.send(true);
            }
        }
    }
}

pub(crate) struct InmemTransportStream {
    sender: Arc<InmemTransportSender>,
    receiver: mpsc::Receiver<Envelope>,
    closed: watch::Receiver<bool>,
}

#[async_trait::async_trait]
impl TransportStream for InmemTransportStream {
    fn sender(&self) -> Arc<dyn TransportSender> {
        self.sender.clone()
    }

    async fn recv(&mut self) -> Option<Envelope> {
        tokio::select! {
            biased;
            changed = self.closed.changed() => {
                if changed.is_ok() && *self.closed.borrow() { None } else { self.receiver.recv().await }
            }
            envelope = self.receiver.recv() => envelope,
        }
    }
}

pub(crate) struct InmemTransportListener {
    accepted: mpsc::Receiver<Box<dyn TransportStream>>,
    endpoint: String,
    registry: Arc<InmemRegistry>,
}

#[async_trait::async_trait]
impl TransportListener for InmemTransportListener {
    async fn accept(&mut self) -> Option<Box<dyn TransportStream>> {
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
    ) -> Result<Box<dyn TransportListener>, TransportError> {
        debug_assert!(listener.is_none(), "inmem transport has no socket");
        let key = advertised.as_str().to_owned();
        let (accepted_tx, accepted_rx) = mpsc::channel::<Box<dyn TransportStream>>(32);
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
        Ok(Box::new(InmemTransportListener {
            accepted: accepted_rx,
            endpoint: key,
            registry: self.registry.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl ClientTransport for InmemTransport {
    async fn connect(
        &self,
        endpoint: &Endpoint,
    ) -> Result<Box<dyn TransportStream>, TransportError> {
        let key = endpoint.as_str().to_owned();
        let accepted = self
            .registry
            .endpoints
            .lock()
            .get(&key)
            .cloned()
            .ok_or_else(|| TransportError::ConnectFailed(format!("unknown endpoint `{key}`")))?;
        let connection_id = self
            .registry
            .next_connection_id
            .fetch_add(1, Ordering::Relaxed);
        let (closed_tx, closed_rx) = watch::channel(false);
        let closed = Arc::new(AtomicBool::new(false));
        self.registry.connections.lock().insert(
            connection_id,
            InmemConnection {
                endpoint: key.clone(),
                closed: closed_tx.clone(),
                flag: closed.clone(),
            },
        );
        let (client_out_tx, client_out_rx) = mpsc::channel::<Envelope>(INMEM_CHANNEL_CAPACITY);
        let (server_out_tx, server_out_rx) = mpsc::channel::<Envelope>(INMEM_CHANNEL_CAPACITY);
        let server_stream: Box<dyn TransportStream> = Box::new(InmemTransportStream {
            sender: Arc::new(InmemTransportSender {
                sender: server_out_tx,
                registry: self.registry.clone(),
                connection_id,
                record: false,
                closed: closed.clone(),
            }),
            receiver: client_out_rx,
            closed: closed_rx.clone(),
        });
        if accepted.try_send(server_stream).is_err() {
            self.registry.connections.lock().remove(&connection_id);
            closed.store(true, Ordering::Release);
            let _ = closed_tx.send(true);
            return Err(TransportError::Closed);
        };
        Ok(Box::new(InmemTransportStream {
            sender: Arc::new(InmemTransportSender {
                sender: client_out_tx,
                registry: self.registry.clone(),
                connection_id,
                record: true,
                closed,
            }),
            receiver: server_out_rx,
            closed: closed_rx,
        }) as Box<dyn TransportStream>)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport_protocol::{Envelope, envelope};

    #[tokio::test]
    async fn inmem_transport_forwards_envelopes_both_ways() {
        let registry = InmemRegistry::new();
        let server_transport = InmemTransport::new(registry.clone());
        let client_transport = InmemTransport::new(registry);
        let mut listener = server_transport
            .listen(&Endpoint::new("test-server"), None)
            .expect("listen");
        let mut client_stream = client_transport
            .connect(&Endpoint::new("test-server"))
            .await
            .expect("connected");
        let mut server_stream = listener.accept().await.expect("accepted");
        client_stream
            .sender()
            .try_send(Envelope {
                protocol_version: 1,
                actor_type: "echo".into(),
                actor_id: vec![1],
                session_id: vec![0; 16],
                kind: Some(envelope::Kind::Action(
                    crate::transport_protocol::ActionMessage {
                        payload: b"ping".to_vec(),
                    },
                )),
            })
            .expect("send");
        assert_eq!(server_stream.recv().await.unwrap().actor_type, "echo");
        server_stream
            .sender()
            .try_send(Envelope {
                protocol_version: 0,
                actor_type: String::new(),
                actor_id: Vec::new(),
                session_id: vec![0; 16],
                kind: Some(envelope::Kind::Event(
                    crate::transport_protocol::EventMessage {
                        payload: b"pong".to_vec(),
                    },
                )),
            })
            .expect("send");
        assert!(matches!(
            client_stream.recv().await.unwrap().kind,
            Some(envelope::Kind::Event(_))
        ));
        listener.shutdown();
        let mut listener = listener;
        assert!(listener.accept().await.is_none());
    }
}
