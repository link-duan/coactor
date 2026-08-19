# Runtime model and guarantees

## Addressing

An Actor is a logical execution boundary identified by an `ActorAddress`: an Actor Type plus an Actor ID. Both components use Kubernetes DNS-label syntax. Creating or cloning an address does not activate the Actor.

A Server registers Actor Types explicitly. A Client opens a bidirectional Session to an address:

```rust
let address = ActorAddress::new("room", "room-7")?;
let mut session = client.open(&address).await?;
```

## Messages and execution

Actions are byte messages sent by the caller. Events are byte messages pushed by the Actor. They are not request and response values.

For one Active Actor, CoActor processes Actions serially and non-reentrantly. Delivery is in-memory and at-most-once:

- `Session::send` success confirms transport admission only;
- CoActor does not retransmit or deduplicate messages.

Consumers own message serialization, schema compatibility, and business-level errors.

## Lifecycle

Opening a Session activates the Actor when necessary. Idle passivation stops the in-memory Active Actor without deleting its logical address. A live Session prevents idle passivation.

An Actor panic stops the current Active Actor. A later open can create a new instance with empty CoActor-managed state.

App State is one strongly typed dependency value shared by all Actor Types in a Server. It defaults to `()` and can be supplied with `.with_state(...)`.

## Distributed ownership

Each Server maintains a renewable lease. If it can no longer prove its authority before the lease deadline, it self-fences and stops serving Actors.

CoActor allows only the current Owner to serve an Actor. A takeover fences the previous Owner before the replacement begins serving it.

When opening a Session, the Client reads the Actor Owner from its read-only Coordination capabilities. A live Owner is contacted directly. If the Actor is unowned or its prior Owner lease is stale, the Client performs Placement and sends `SessionOpen` to the selected Server; only that Server may reserve capacity and claim ownership.

Servers never forward Session traffic to other Servers. A target either serves locally, claims an unowned Actor, or rejects the request with `NotOwner` or `RuntimeAtCapacity`.

## Placement and failover

Client Placement uses the Node Directory load snapshot, the configured Placement Strategy, and Client-local In-flight Placement accounting. The default strategy is p2c. Draining, pressured, full, protocol-incompatible, and invalid endpoint records are excluded before attempts; target Server capacity and ownership CAS remain the final admission checks.

Placement retries exclude failed candidates and remain bounded by both the configured maximum number of `SessionOpen` messages sent to Servers and the single `open_timeout` deadline. `NotOwner` causes a fresh ownership read. Failure to connect to an unchanged live Owner does not permit Placement on another Server.

The ownership claim fixes placement; CoActor does not continuously rebalance Active Actors. Owner or Transport Connection failure interrupts the Session. The caller must open a new Session, and a replacement Owner starts with empty CoActor-managed state.

## Transport connections

The Client maintains a bounded connection pool for each Server endpoint. Connections are created lazily, and each Session is bound to one connection from open through close. Multiple Sessions may share a connection; losing one connection terminates only the Sessions bound to it.

## Limits

CoActor does not provide:

- Session migration across failures;
- continuous placement rebalancing;
- a built-in Coordination backend other than S3.
