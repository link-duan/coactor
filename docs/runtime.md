# Runtime model and guarantees

## Addressing

An Actor is a logical execution boundary identified by an `ActorAddress`: an Actor Type plus an Actor ID. Both components use Kubernetes DNS-label syntax. Creating or cloning an address does not activate the Actor.

A Server registers Actor Types explicitly. A Client opens a persistent bidirectional Session to an address:

```rust
let address = ActorAddress::new("room", "room-7")?;
let mut session = client.open(&address).await?;
```

## Messages and execution

Actions are byte messages sent by the caller. Events are byte messages pushed by the Actor. They are not request and response values.

For one Active Actor, CoActor processes Actions serially and non-reentrantly. Delivery is in-memory and at-most-once:

- `Session::send` success confirms transport admission only;
- an Event does not prove durable storage;
- CoActor does not retransmit or deduplicate messages.

Consumers own message serialization, schema compatibility, and business-level errors.

## Lifecycle

Opening a Session activates the Actor when necessary. Idle passivation stops the in-memory Active Actor without deleting its logical address. A live Session prevents idle passivation.

An Actor panic stops the current Active Actor. A later open can create a new instance with empty CoActor-managed state.

App State is one strongly typed dependency value shared by all Actor Types in a Server. It defaults to `()` and can be supplied with `.with_state(...)`.

## Distributed ownership

Each Server maintains a renewable lease. If it can no longer prove its authority before the lease deadline, it self-fences and stops serving Actors.

CoActor allows only the current Owner to serve an Actor. A takeover fences the previous Owner before the replacement begins serving it.

Clients reach Actors through Gateway nodes. Gateway or Owner failure interrupts the Session.

## Placement and failover

An unowned or stale Actor is placed on a live Server using current capacity information. The ownership claim fixes that placement; CoActor does not continuously rebalance active Actors.

Owner or Gateway failure interrupts the Session. The caller must open a new Session. A replacement Owner currently starts with empty CoActor-managed state: this is availability failover, not state recovery.

## Current limits

CoActor does not currently provide:

- Actor state persistence or Recovery;
- durable acknowledgements;
- Session migration across failures;
- continuous placement rebalancing;
- a built-in Coordination backend other than S3.
