# CoActor design brief

## Problem

CoActor is an embedded Rust runtime for stateful workloads addressed by stable business identity. Game rooms and collaborative documents are validation scenarios, while the shared runtime owns Actor identity, serial execution, lifecycle, Session semantics, routing, ownership and fencing.

The current runtime deliberately separates availability from durability. It can move service to a new Owner after failure, but the new Active Actor starts from empty CoActor-managed state.

## Current architecture

```text
consumer application
    │ Client.actor("room", id).open() → Session      │ Server 侧：Actor 宿主
    ▼                                                ▼
Client (调用方)                                Server (宿主 + 网关)
    ├── Service Discovery → 网关池 → 会话经网关中继     ├── registry, mailbox, lifecycle
    └── transport seam (connect 半部)                └── 网关放置/转发 + ownership
        │                                            ├── transport seam (accept 半部)
        └──────── 双向 Envelope 流（gRPC / inmem）─────┘
                            S3 Ownership Authority
                                ├── Node Lease
                                └── Actor Owner Record + Ownership Epoch
```

- `ServerBuilder::local`（测试便利，经 `test_support::TestServer`）与 `ServerBuilder::cluster`（分布式）分别选择运行形态；两者都经 `start()` 异步启动。`Client` 是独立于 `Server` 的调用方入口（ADR-0008）。
- Actor Refs are stable weak handles obtained by Actor Type name + Actor ID; `open()` establishes a persistent bidirectional Session (Action in, Event out, byte payloads).
- `#[coactor::actor]` marks the Actor struct and generates the type-erased dispatch impl; the Actor Type name is passed explicitly at registration (`register::<A>(name)`). Consumers implement the `Actor` trait with native `async fn` methods. `ActorRuntime` injects identity, AppState and `broadcast` at construction; `MessageContext` carries the current Session for directed Event delivery.
- A local Active Actor owns a bounded mailbox and executes one message at a time, including across handler `.await` points.
- Passivation fires only when no live Session remains for the Actor; failover explicitly breaks Sessions (callers re-`open()`), detected lazily on the next send.
- Cluster mode uses Node Leases for runtime authority and Actor Owner Records for per-address routing and monotonically fenced takeover.
- Gateways resolve ownership on session ingress: claim unowned/stale actors (placement strategy, bounded retry) or forward sessions to the current Owner over a per-session relay; Owner or gateway failure explicitly breaks the Session (ADR-0008).
- The public cluster API uses the built-in S3 authority. The transport and backend abstractions are crate-private seams: grpc + inmem transport, S3 backend with deterministic fakes.
- Callers never touch the authority: Client discovers gateways (Service Discovery), keeps a pool, and opens Sessions through a gateway; transport-level open failures fail over to another pool node.

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
