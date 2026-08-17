use std::{net::SocketAddr, time::Duration};

use coactor::{
    Actor, ActorAddress, ActorRuntime, Client, MessageContext, Server, actor,
    coordination::backend::s3::S3CoordinationStore,
};

#[actor]
struct CounterActor {
    count: u64,
}

impl Actor<()> for CounterActor {
    fn new(_runtime: ActorRuntime<()>) -> Self {
        Self { count: 0 }
    }

    async fn on_message(&mut self, ctx: &MessageContext, _msg: &[u8]) {
        self.count += 1;
        let _ = ctx.send(self.count.to_string().into_bytes()).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("COACTOR_S3_BUCKET")?;
    let prefix = std::env::var("COACTOR_S3_PREFIX").unwrap_or_default();
    let bind: SocketAddr = std::env::var("COACTOR_BIND")?.parse()?;
    let advertised = std::env::var("COACTOR_ADVERTISED_ENDPOINT")?;
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    let server_store = S3CoordinationStore::new(aws_sdk_s3::Client::new(&sdk), &bucket, &prefix)?;
    let server = Server::builder(server_store)
        .bind(bind)
        .advertised_endpoint(&advertised)
        .actor::<CounterActor>("counter")
        .start()
        .await?;

    let client_store = S3CoordinationStore::new(aws_sdk_s3::Client::new(&sdk), bucket, prefix)?
        .directory_refresh_interval(Duration::from_secs(3));
    let client = Client::builder(client_store).build()?;
    let address = ActorAddress::new("counter", "production-counter")?;
    let mut session = client.open(&address).await?;
    session.send(b"increment".to_vec()).await?;
    println!("{}", String::from_utf8(session.recv().await.unwrap()?)?);

    client.shutdown().await;
    server.shutdown().await;
    Ok(())
}
