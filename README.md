# CoActor

> An embedded distributed Actor framework for Rust applications.

[![Rust 1.85+](https://img.shields.io/badge/rust-1.85%2B-orange.svg)](https://www.rust-lang.org)
[![Unit test coverage](https://codecov.io/gh/link-duan/coactor/graph/badge.svg?flag=unit)](https://codecov.io/gh/link-duan/coactor?flags=unit)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](#license)

CoActor brings location-transparent Actors to applications that already run on Tokio. It embeds Actor execution directly into your Server process, addresses Actors by stable business keys, activates them on demand, and coordinates ownership across Server nodes.

> **Current status:** CoActor is in the early stages of development.

## Why CoActor?

CoActor is designed for applications that need Actor semantics without introducing a separate Actor service or mandatory control plane.

- **Embedded by design** — run the framework inside an existing Rust/Tokio service.
- **Stable addressing** — address an Actor by its Actor Type and Actor ID, independent of its current Server.
- **On-demand lifecycle** — activate Actors when Sessions open and passivate idle Actors while retaining their logical addresses.
- **Bidirectional Sessions** — send fire-and-forget Actions and receive independently emitted Events, including broadcasts to connected callers.
- **Explicit failure semantics** — leases, ownership fencing, and Session interruption make failover behavior observable instead of turning stale ownership into ambiguous success.
- **Direct owner connections** — Clients resolve ownership and connect directly to the current Owner; Servers do not act as message-forwarding gateways.

## Quick start

### Host an Actor on a Server

```rust
Server::builder(coordination_store)
    .advertised_endpoint("actor-0.internal:7000")
    .actor::<CounterActor>("counter")
    .serve(":7000")
    .await?;
```

### Open a Session from a Client

```rust
let client = Client::builder(coordination_store).build()?;
let address = ActorAddress::new("counter", "counter-7")?;
let mut session = client.open(&address).await?;

session.send(b"increment".to_vec()).await?;
let event = session.recv().await;
```

For a complete setup—including S3 coordination, credentials, and runnable examples—see [Getting started](https://github.com/link-duan/coactor/blob/main/docs/getting-started.md).

## Programming model

An Actor is a stateful Rust type registered under a stable Actor Type name. Callers construct an [`ActorAddress`](https://docs.rs/coactor/latest/coactor/struct.ActorAddress.html) from that type name and an Actor ID, then exchange byte messages through a bidirectional Session.

The framework manages:

1. Actor registration and activation;
2. bounded message admission and serialized Actor execution;
3. Session lifecycle and server push;
4. ownership placement, leases, and fencing across Server nodes; and
5. graceful shutdown and failure propagation.

## Delivery and failure semantics

Actions and Events are in-memory, at-most-once byte messages. A successful `send` confirms transport admission only; it does not confirm that the Actor has processed the message.

When an Owner or transport connection fails, the Session ends and the caller must reopen it. After failover, the new Owner starts with empty CoActor-managed state. These guarantees are intentional—read [Runtime model and guarantees](docs/runtime.md) before building application-level reliability or recovery on top of CoActor.

## Documentation

- [Getting started](https://github.com/link-duan/coactor/blob/main/docs/getting-started.md) — install, define an Actor, and run a Server and Client
- [Runtime model and guarantees](https://github.com/link-duan/coactor/blob/main/docs/runtime.md) — lifecycle, delivery, ownership, and failure behavior
- [Command-line chat example](https://github.com/link-duan/coactor/blob/main/docs/examples/command-line-chat.md) — bidirectional Sessions and broadcast Events
- [S3 deployment](https://github.com/link-duan/coactor/blob/main/docs/s3.md) — configure the shared Coordination Store
- [Testing Actors](https://github.com/link-duan/coactor/blob/main/docs/testing.md) — test Actor behavior and lifecycle
- [Architecture Decision Records](https://github.com/link-duan/coactor/blob/main/docs/adr/README.md) — design decisions behind the framework
- [Release guide](https://github.com/link-duan/coactor/blob/main/docs/releasing.md) — publish both crates through GitHub Actions

## Examples

The repository includes runnable examples for a counter and a multi-client chat room:

```bash
cargo run -p coactor --example counter_server
cargo run -p coactor --example counter_client
```

See the [command-line chat guide](https://github.com/link-duan/coactor/blob/main/docs/examples/command-line-chat.md) for the full multi-process example.

## Requirements

- Rust 1.85 or later
- An S3-compatible bucket for distributed coordination

## License

Licensed under the [MIT License](LICENSE).
