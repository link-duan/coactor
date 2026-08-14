---
status: accepted
---

# Built-in S3 ownership authority

CoActor's public cluster API binds ownership to the built-in S3 authority instead of exposing a consumer-supplied provider interface. During early development, this keeps the supported configuration and correctness boundary small while Node Lease and Actor Owner Record semantics evolve together. A crate-private `OwnershipBackend` seam remains solely to isolate the protocol implementation and enable deterministic tests; supporting another authority requires an intentional runtime change rather than an application plugin.
