---
status: accepted
---

# Distributed runtime with stateless Availability Failover

CoActor extends the embedded runtime into a distributed production runtime without introducing a mandatory control-plane service. Production operation requires explicit peer and ownership configuration rather than silently falling back to local-only behavior. Typed Actor calls remain location-transparent; gRPC and Protobuf provide the peer transport, while an ownership authority coordinates placement and fencing. CoActor owns the Actor-level rules and uses mature infrastructure only for the lower-level capabilities it actually guarantees.

The first production ownership adapter targets AWS S3 conditional operations behind a replaceable storage boundary. A renewable lease establishes whether a runtime session may serve work, and per-Actor ownership records establish routing and monotonically fenced takeover. Lease loss stops the affected runtime session from serving Actors and reports termination through the host's supervision boundary; the library does not terminate the host process. This two-level model avoids renewal work growing linearly with the number of Active Actors.

The design intentionally assumes ordinary clock synchronization and uses monotonic local time for self-fencing; it does not claim tolerance of arbitrary clock skew. Ambiguous storage mutations require bounded read-back reconciliation, and a command is automatically retried only when the runtime can prove that the peer did not accept it. Capacity information is a placement hint, not a reservation.

This stage provides Availability Failover, not Recovery: after confirmed owner loss, a new owner starts the Actor from empty CoActor-managed state. Development and CI validate the adapter with deterministic fakes, local HTTP contract tests and an S3-compatible emulator; qualification against real AWS is a release gate rather than a prerequisite for everyday testing. Detailed wire, lease and takeover rules belong in the distributed runtime semantics document.
