---
status: accepted
---

# Embedded Actor runtime with a typed API

CoActor implements its Actor runtime as a library embedded in the consumer's Tokio process. It owns Actor identity, registration, bounded admission, serial non-reentrant execution, lifecycle, failure and shutdown semantics instead of delegating those guarantees to a remoting framework or a hidden executor. This keeps the behavior that matters to consumers explicit and testable while leaving process supervision and infrastructure ownership to the host application.

Consumers interact through generated typed Actor Refs and request-response commands. Actor location, message plumbing and runtime dispatch remain hidden; dependencies are injected when an Actor is constructed rather than discovered through a service locator. A Ref is a stable address handle, not ownership of an Active Actor or the runtime, so retaining it neither activates an Actor nor prevents passivation.

Mailbox acceptance and Handler Reply describe only in-memory runtime progress. They are not durable acknowledgements, and an Actor that passivates or fails can restart from empty state until persistence is introduced. Exact API constraints, lifecycle ordering and error behavior belong in the runtime semantics document rather than this ADR.
