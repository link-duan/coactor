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
