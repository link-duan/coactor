use std::net::SocketAddr;

use coactor::{
    Actor, ActorRuntime, MessageContext, Server, actor,
    coordination::backend::s3::S3CoordinationStore,
};

#[actor]
struct Counter {
    value: u64,
}

impl Actor<()> for Counter {
    fn new(_: ActorRuntime<()>) -> Self {
        Self { value: 0 }
    }

    async fn on_message(&mut self, ctx: &MessageContext, _: &[u8]) {
        self.value += 1;
        let _ = ctx.send(self.value.to_string().into_bytes()).await;
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("COACTOR_S3_BUCKET")?;
    let prefix = std::env::var("COACTOR_S3_PREFIX").unwrap_or_default();
    let bind: SocketAddr = std::env::var("COACTOR_BIND")?.parse()?;
    let advertised = std::env::var("COACTOR_ADVERTISED_ENDPOINT")?;
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let store = S3CoordinationStore::new(aws_sdk_s3::Client::new(&sdk), bucket, prefix)?;

    let server = Server::builder(store)
        .bind(bind)
        .advertised_endpoint(&advertised)
        .node_id("counter-server")
        .actor::<Counter>("counter")
        .start()
        .await?;

    let failure = tokio::select! {
        result = server.wait() => result.err(),
        result = tokio::signal::ctrl_c() => {
            result?;
            None
        }
    };

    server.shutdown().await;
    if let Some(error) = failure {
        return Err(error.into());
    }
    Ok(())
}
