# Testing Actors

`TestServer` is an in-memory Actor test harness. It is not a local production mode and does not exercise S3 coordination or the gRPC network path.

```rust
let server = TestServer::builder()
    .actor::<Counter>("counter")
    .start()
    .await?;

let address = ActorAddress::new("counter", "counter-7")?;
let mut session = server.client().open(&address).await?;
session.send(b"increment".to_vec()).await?;

assert_eq!(session.recv().await.unwrap()?, b"1");
server.shutdown().await;
```

Use it for Actor behavior and lifecycle tests. Use the production runtime for coordination and network integration tests.

Example binaries are compiled explicitly in CI:

```bash
cargo check --locked -p coactor --examples
```

The [command-line chat example](examples/command-line-chat.md) is covered by a production-path integration test that starts RustFS with Docker Compose, runs the real Server and Clients, verifies message and `Left` Event delivery, and checks Server stdout:

```bash
python3 scripts/test-chat-example.py
```

The script owns the repository's Compose stack for the duration of the test and removes its RustFS volume afterward.
