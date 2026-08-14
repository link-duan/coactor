use std::{net::SocketAddr, sync::Arc};

use tokio::sync::Notify;

pub(crate) struct ReplyDroppingProxy {
    address: SocketAddr,
    drop_connection: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl ReplyDroppingProxy {
    pub(crate) async fn start(upstream: SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let drop_connection = Arc::new(Notify::new());
        let task = tokio::spawn({
            let drop_connection = drop_connection.clone();
            async move {
                let (mut downstream, _) = listener.accept().await.unwrap();
                let mut upstream = tokio::net::TcpStream::connect(upstream).await.unwrap();
                tokio::select! {
                    _ = tokio::io::copy_bidirectional(&mut downstream, &mut upstream) => {}
                    _ = drop_connection.notified() => {}
                }
            }
        });
        Self {
            address,
            drop_connection,
            task,
        }
    }

    pub(crate) fn address(&self) -> SocketAddr {
        self.address
    }

    pub(crate) fn endpoint(&self) -> String {
        format!("http://{}", self.address)
    }

    pub(crate) async fn drop_connection(self) {
        self.drop_connection.notify_one();
        self.task.await.unwrap();
    }
}
