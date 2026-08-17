use std::{env, error::Error, net::SocketAddr, time::Duration};

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use coactor::{
    Actor, ActorId, ActorRuntime, ClientBuilder, ClientConfig, CoordinationConfig, MessageContext,
    ServerBuilder, actor,
    cluster::{S3CoordinationConfig, ServerConfig},
};

#[derive(Clone)]
struct AppState;

#[actor]
struct CounterActor {
    _runtime: ActorRuntime<AppState>,
    value: i64,
}

impl Actor<AppState> for CounterActor {
    fn new(runtime: ActorRuntime<AppState>) -> Self {
        Self {
            _runtime: runtime,
            value: 0,
        }
    }

    async fn on_message(&mut self, ctx: &MessageContext, msg: &[u8]) {
        let amount = i64::from_be_bytes(msg.try_into().expect("8-byte Action"));
        self.value += amount;
        let _ = ctx.send(self.value.to_be_bytes().to_vec()).await;
    }
}

fn coordination_config() -> S3CoordinationConfig {
    let session_token = env::var("AWS_SESSION_TOKEN").ok();
    S3CoordinationConfig {
        bucket: env::var("COACTOR_S3_BUCKET").expect("COACTOR_S3_BUCKET"),
        prefix: env::var("COACTOR_S3_PREFIX").unwrap_or_else(|_| "coactor/production".to_owned()),
        region: env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".to_owned()),
        endpoint_url: env::var("COACTOR_S3_ENDPOINT").ok(),
        credentials_provider: SharedCredentialsProvider::new(Credentials::new(
            env::var("AWS_ACCESS_KEY_ID").expect("AWS_ACCESS_KEY_ID"),
            env::var("AWS_SECRET_ACCESS_KEY").expect("AWS_SECRET_ACCESS_KEY"),
            session_token,
            None,
            "environment",
        )),
        request_timeout: Duration::from_secs(5),
    }
}

async fn run_server() -> Result<(), Box<dyn Error>> {
    let bind_address: SocketAddr = env::var("COACTOR_BIND_ADDRESS")?.parse()?;
    let advertised_address: SocketAddr = env::var("COACTOR_ADVERTISED_ADDRESS")?.parse()?;
    let server = ServerBuilder::cluster(
        AppState,
        ServerConfig::new(
            env::var("COACTOR_NODE_ID")?,
            bind_address,
            advertised_address,
            CoordinationConfig::S3(coordination_config()),
        ),
    )
    .register::<CounterActor>("counter")
    .start()
    .await?;

    tokio::signal::ctrl_c().await?;
    server.shutdown().await;
    Ok(())
}

async fn run_client() -> Result<(), Box<dyn Error>> {
    let client = ClientBuilder::new(ClientConfig {
        coordination: CoordinationConfig::S3(coordination_config()),
    })
    .start();
    let mut session = client
        .actor("counter", ActorId::from("production-counter"))
        .open()
        .await?;
    session.send(3i64.to_be_bytes().to_vec()).await?;
    let event = session.recv().await.ok_or("Session closed")??;
    println!("value = {}", i64::from_be_bytes(event.try_into().unwrap()));
    client.shutdown().await;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    match env::args().nth(1).as_deref() {
        Some("server") => run_server().await,
        Some("client") => run_client().await,
        _ => Err("usage: cluster_counter <server|client>".into()),
    }
}
