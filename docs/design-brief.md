# CoActor design brief

## Problem

CoActor is an embedded Rust runtime for stateful workloads addressed by stable business identity. Game rooms and collaborative documents are validation scenarios, while the shared runtime owns Actor identity, serial execution, lifecycle, Session semantics, routing, ownership and fencing.

The current runtime deliberately separates availability from durability. It can move service to a new Owner after failure, but the new Active Actor starts from empty CoActor-managed state.

## Current architecture

```text
consumer application
    │ runtime.actor("room", id).open() → Session
    ▼
runtime facade
    ├── local registry, admission, Session registry, mailbox and lifecycle
    └── cluster routing
        ├── bidi gRPC peer stream (node-pair multiplexed)
        └── S3 Ownership Authority
            ├── Node Lease
            └── Actor Owner Record + Ownership Epoch
```

- `RuntimeBuilder::local` and `RuntimeBuilder::cluster` explicitly select the operating mode; both start asynchronously through `start()`.
- Actor Refs are stable weak handles obtained by Actor Type name + Actor ID; `open()` establishes a persistent bidirectional Session (Action in, Event out, byte payloads).
- `#[coactor::actor(name = "...")]` marks the Actor struct; consumers implement the `Actor` trait with native `async fn` methods. `ActorRuntime` injects identity, AppState and `broadcast` at construction; `MessageContext` carries the current Session for directed Event delivery.
- A local Active Actor owns a bounded mailbox and executes one message at a time, including across handler `.await` points.
- Passivation fires only when no live Session remains for the Actor; failover explicitly breaks Sessions (callers re-`open()`), detected lazily on the next send.
- Cluster mode uses Node Leases for runtime authority and Actor Owner Records for per-address routing and monotonically fenced takeover.
- The public cluster API uses the built-in S3 authority. The backend abstraction is crate-private and exists for deterministic protocol tests.

## Delivered guarantees

- stable `ActorType + ActorId` addressing with name-based lookup;
- lazy activation via `open()`, bounded admission and serial non-reentrant handling;
- persistent bidirectional Sessions with fire-and-forget Action delivery and bounded outbound backpressure;
- activation/deactivation/session lifecycle hooks, idle passivation gated on live Sessions, and graceful shutdown;
- structured local and remote failure categories;
- location-transparent Session dispatch over multiplexed bidi gRPC streams;
- S3 conditional-write ownership, advisory placement and stateless Availability Failover;
- Owner-side self-fencing that stops local Active Actor execution and Event delivery when Node authority expires or is lost.

Event delivery remains non-durable. CoActor does not currently provide Actor Store persistence, Recovery, Migration, message deduplication, exactly-once delivery, actor-scoped timers, or fencing for consumer-managed external side effects.

## Future direction

The next persistence stage may introduce an Actor-scoped transactional KV, epoch-isolated Actor Store objects, asynchronous checkpoints and a fixed Restore Cut. Durable ACK, message deduplication and planned Migration remain separate later decisions; they must not be inferred from the current ownership protocol. Typed (prost) Action/Event payloads are a separate later decision recorded in ADR-0005.

## Acceptance questions

1. Does one Owner serialize messages for an Actor Address while its authority remains valid?
2. Are passivation and new-message races free from silent message loss?
3. Do ambiguous ownership writes require exact bounded read-back rather than blind replay?
4. Does lease loss stop mutation and successful Event delivery without terminating the host process?
5. Is a live Session guaranteed to block passivation, and does a closed Session allow it?
6. Does failover break Sessions such that callers re-establish them on the new Owner?
7. After failover, is empty-state activation described as Availability Failover rather than Recovery?

## Research background

Existing Rust Actor frameworks informed the in-process execution model, but CoActor keeps distributed lifecycle, ownership and fencing semantics inside its own runtime boundary. Detailed historical research is retained outside this design brief; current guarantees are defined by the runtime semantics documents and accepted ADRs.
