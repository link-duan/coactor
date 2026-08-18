# CoActor

CoActor is an embedded distributed Actor runtime for Rust applications. It addresses Actors by stable business keys, activates them on demand, and coordinates ownership across Server nodes.

> CoActor `0.1.0` provides in-memory Actor execution and availability failover.

## Why CoActor?

- **Fits existing Rust services.** CoActor runs inside the application's Tokio process; there is no separate Actor service or mandatory control plane to operate.
- **Uses stable business identities.** Callers address an Actor by Actor Type and Actor ID without knowing which Server currently hosts it.
- **Activates Actors on demand.** The runtime creates Active Actors when Sessions open and passivates idle Actors while preserving their logical addresses.
- **Supports server push.** Bidirectional Sessions carry fire-and-forget Actions and independently emitted Events, including broadcasts to connected callers.
- **Makes distributed failure explicit.** Leases, ownership fencing, and Session interruption prevent a stale Owner from being treated as current instead of hiding failover behind ambiguous success.

## Example

### Server

```rust
Server::builder(coordination_store)
    .advertised_endpoint("actor-0.internal:7000")
    .actor::<CounterActor>("counter")
    .serve(":7000")
    .await?;
```

### Client

```rust
let client = Client::builder(node_directory).build()?;
let address = ActorAddress::new("counter", "counter-7")?;
let mut session = client.open(&address).await?;

session.send(b"increment".to_vec()).await?;
let event = session.recv().await;
```

Actions and Events are in-memory, at-most-once byte messages. A successful send confirms transport admission only. Owner or Gateway failure ends the Session; callers reopen it, and the new Owner starts with empty CoActor-managed state.

## Documentation

- [Getting started](docs/getting-started.md)
- [Runtime model and guarantees](docs/runtime.md)
- [S3 deployment](docs/s3.md)
- [Testing Actors](docs/testing.md)

## License

Licensed under either Apache-2.0 or MIT, at your option.
