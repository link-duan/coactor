# CoActor design brief

## Problem

CoActor is an embedded Rust runtime for stateful workloads addressed by stable business identity. Game rooms and collaborative documents are validation scenarios, while the shared runtime owns Actor identity, serial execution, lifecycle, routing, ownership and fencing semantics.

The current runtime deliberately separates availability from durability. It can move service to a new Owner after failure, but the new Active Actor starts from empty CoActor-managed state.

## Current architecture

```text
consumer application
    │ generated typed Actor Ref
    ▼
runtime facade
    ├── local registry, admission, mailbox and lifecycle
    └── cluster routing
        ├── gRPC peer transport
        └── S3 Ownership Authority
            ├── Node Lease
            └── Actor Owner Record + Ownership Epoch
```

- `RuntimeBuilder::local` and `RuntimeBuilder::cluster` explicitly select the operating mode; both start asynchronously through `start()`.
- Actor Refs are stable weak handles. Obtaining or cloning one neither activates an Actor nor keeps the runtime alive.
- A local Active Actor owns a bounded mailbox and executes one command at a time, including across handler `.await` points.
- Remote-enabled commands use `#[coactor::command(remote)]` with one `prost::Message + Default` request and matching Protobuf success/error payloads. Plain commands remain local-only.
- Cluster mode uses Node Leases for runtime authority and Actor Owner Records for per-address routing and monotonically fenced takeover.
- The public cluster API uses the built-in S3 authority. The backend abstraction is crate-private and exists for deterministic protocol tests.

## Delivered guarantees

- stable `ActorType + ActorId` addressing;
- lazy activation, bounded admission and serial non-reentrant handling;
- activation/deactivation lifecycle, idle passivation and graceful shutdown;
- structured local and remote failure categories;
- location-transparent dispatch for remote-enabled commands;
- S3 conditional-write ownership, advisory placement and stateless Availability Failover;
- Owner-side self-fencing that stops local Active Actor execution and successful replies when Node authority expires or is lost.

Handler Reply remains non-durable. CoActor does not currently provide Actor Store persistence, Recovery, Migration, command deduplication, exactly-once delivery, actor-scoped timers, or fencing for consumer-managed external side effects.

## Future direction

The next persistence stage may introduce an Actor-scoped transactional KV, epoch-isolated Actor Store objects, asynchronous checkpoints and a fixed Restore Cut. Durable ACK, command deduplication and planned Migration remain separate later decisions; they must not be inferred from the current ownership protocol.

## Acceptance questions

1. Does one Owner serialize commands for an Actor Address while its authority remains valid?
2. Are passivation and new-command races free from silent command loss?
3. Do ambiguous ownership writes require exact bounded read-back rather than blind replay?
4. Does lease loss stop mutation and successful replies without terminating the host process?
5. Are requests replayed only when the runtime can prove the remote handler did not accept them?
6. After failover, is empty-state activation described as Availability Failover rather than Recovery?

## Research background

Existing Rust Actor frameworks informed the in-process execution model, but CoActor keeps distributed lifecycle, ownership and fencing semantics inside its own runtime boundary. Detailed historical research is retained outside this design brief; current guarantees are defined by the runtime semantics documents and accepted ADRs.
