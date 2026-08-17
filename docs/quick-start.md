# Quick Start

Production cluster mode runs `Server` and `Client` as separate processes backed by the same S3 Coordination Store. Server hosts Actors and acts as a Gateway; Client reads the Node Directory and opens Sessions through a live Gateway.

## Add dependencies

```toml
[dependencies]
coactor = { git = "https://github.com/link-duan/coactor.git" }
aws-credential-types = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

## Define an Actor

```rust
use coactor::{Actor, ActorRuntime, MessageContext, actor};

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
```

The Actor Type name is supplied when Server registers the Actor. It is not derived from the Rust type name.

## Start a Server

```rust
use std::time::Duration;

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use coactor::{CoordinationConfig, ServerBuilder};
use coactor::cluster::{S3CoordinationConfig, ServerConfig};

#[tokio::main]
async fn main() {
    let coordination = S3CoordinationConfig {
        bucket: "my-coactor".to_owned(),
        prefix: "production".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint_url: None,
        credentials_provider: SharedCredentialsProvider::new(Credentials::new(
            "server-access-key",
            "server-secret-key",
            None,
            None,
            "quick-start",
        )),
        request_timeout: Duration::from_secs(5),
    };

    let server = ServerBuilder::cluster(
        AppState,
        ServerConfig::new(
            "node-a",
            "0.0.0.0:7000".parse().unwrap(),
            "10.0.0.12:7000".parse().unwrap(),
            CoordinationConfig::S3(coordination),
        ),
    )
    .register::<CounterActor>("counter")
    .start()
    .await
    .unwrap();

    tokio::signal::ctrl_c().await.unwrap();
    server.shutdown().await;
}
```

- `0.0.0.0:7000` is the local bind address.
- `10.0.0.12:7000` is the advertised address published in the Node Directory and must be reachable by other processes.
- Server credentials need read/write access to Node Leases and Actor Owner Records.

## Start a Client

Client uses the same bucket and prefix, but should use separate read-only credentials.

```rust
use std::time::Duration;

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use coactor::{
    ActorId, ClientBuilder, ClientConfig, CoordinationConfig,
    cluster::S3CoordinationConfig,
};

#[tokio::main]
async fn main() {
    let coordination = S3CoordinationConfig {
        bucket: "my-coactor".to_owned(),
        prefix: "production".to_owned(),
        region: "us-east-1".to_owned(),
        endpoint_url: None,
        credentials_provider: SharedCredentialsProvider::new(Credentials::new(
            "client-readonly-access-key",
            "client-readonly-secret-key",
            None,
            None,
            "quick-start",
        )),
        request_timeout: Duration::from_secs(5),
    };

    let client = ClientBuilder::new(ClientConfig {
        coordination: CoordinationConfig::S3(coordination),
    })
    .start();

    let mut session = client
        .actor("counter", ActorId::from("counter-7"))
        .open()
        .await
        .unwrap();

    session.send(3i64.to_be_bytes().to_vec()).await.unwrap();
    let event = session.recv().await.unwrap().unwrap();
    println!("value = {}", i64::from_be_bytes(event.try_into().unwrap()));

    client.shutdown().await;
}
```

Client does not need a DNS name or static Gateway endpoint list. It builds its Gateway Pool from live Node Sessions in the Node Directory.

The credentials above are intentionally simple placeholders. Production deployments should inject short-lived IAM credentials or another appropriate credential provider rather than embedding secrets in source code.

## Complete executable

A complete compile-checked example that reads deployment values from environment variables is available at:

- [`coactor/examples/cluster_counter.rs`](../coactor/examples/cluster_counter.rs)

Run it as separate Server and Client processes:

```console
cargo run -p coactor --example cluster_counter -- server
cargo run -p coactor --example cluster_counter -- client
```

## Runtime semantics to account for

- Action and Event payloads are bytes; the consumer owns encoding and schema compatibility.
- Events are not durable and do not prove state persistence.
- A live Session blocks passivation.
- Owner or Gateway failure breaks the Session; caller must use `open()` again.
- Availability Failover starts the Actor from empty CoActor state; current cluster mode does not provide Recovery.
- Always await Server and Client shutdown.
