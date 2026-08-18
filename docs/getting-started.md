# Getting started

## Prerequisites

- Rust 1.85 or later
- an existing S3 bucket
- AWS credentials with read, list, write, and delete access under a dedicated prefix
- a Server endpoint reachable by its Client and peers

## Install

```bash
cargo add coactor --git https://github.com/link-duan/coactor
cargo add tokio --features macros,rt-multi-thread,signal
cargo add aws-config@=1.8.1 aws-sdk-s3@=1.96.0
```

## Define an Actor

```rust
use coactor::{Actor, ActorRuntime, MessageContext, actor};

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
```

## Start a Server

The Server process owns Actor registration and hosts Active Actors.

```rust
let sdk = aws_config::load_defaults(
    aws_config::BehaviorVersion::latest(),
).await;

let store = S3CoordinationStore::new(
    aws_sdk_s3::Client::new(&sdk),
    "my-bucket",
    "coactor/dev",
)?;

Server::builder(store)
    .advertised_endpoint("127.0.0.1:7000")
    .node_id("dev-node")
    .actor::<Counter>("counter")
    .serve(":7000")
    .await?;
```

## Connect a Client

The Client process does not register or host Actors. It reads the Node Directory and opens Sessions through the available Servers.

```rust
let sdk = aws_config::load_defaults(
    aws_config::BehaviorVersion::latest(),
).await;

let directory = S3CoordinationStore::new(
    aws_sdk_s3::Client::new(&sdk),
    "my-bucket",
    "coactor/dev",
)?;
let client = Client::builder(directory).build()?;

let address = ActorAddress::new("counter", "counter-7")?;
let mut session = client.open(&address).await?;
session.send(b"increment".to_vec()).await?;

if let Some(Ok(event)) = session.recv().await {
    println!("{}", String::from_utf8(event)?);
}

client.shutdown().await;
```

Complete examples:

- [`coactor/examples/counter_server.rs`](../coactor/examples/counter_server.rs)
- [`coactor/examples/counter_client.rs`](../coactor/examples/counter_client.rs)

Configure AWS credentials and the shared coordination prefix:

```bash
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export COACTOR_S3_BUCKET=my-existing-bucket
export COACTOR_S3_PREFIX=coactor/dev
```

Start the Server in one terminal:

```bash
export COACTOR_BIND=127.0.0.1:7000
export COACTOR_ADVERTISED_ENDPOINT=127.0.0.1:7000
cargo run -p coactor --example counter_server
```

Then run the Client in another terminal:

```bash
cargo run -p coactor --example counter_client
```

A successful `send` confirms transport admission, not Actor processing. Read [Runtime model and guarantees](runtime.md) before relying on failure behavior.
