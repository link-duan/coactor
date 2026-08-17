---
status: accepted
---

# Chainable runtime construction and direct Session opening

CoActor replaces mode-specific constructors and backend config enums with capability injection: production uses `Server::builder(coordination_store)`, callers use `Client::builder(node_directory).build()`, and tests use `TestServer::builder()`. App State defaults to `()` and `.with_state(state)` may appear before or after Actor registration; registration uses `.actor::<A>("name")` or an `ActorConfig`, while Server configuration is expressed through named chainable methods such as `.bind(...)`, `.advertised_endpoint(...)`, and `.default_mailbox_capacity(...)`.

`ActorAddress` becomes the only public address value: it validates the Actor Type and Actor ID strings, both of which use Kubernetes DNS-label syntax. `Client::open(&ActorAddress)` establishes a Session directly; `ActorRef` and the separate Rust newtypes `ActorType` and `ActorId` are removed because they add staging without additional runtime behavior. `ActorRuntime::state()` exposes `&S` while the runtime retains internal shared ownership.

Server startup remains asynchronous and returns `ServerStartError`; Client construction is synchronous and returns `ClientBuildError`. `Server::wait()` reports `ServerFailure::Fenced`, and `Server::shutdown(self)` and `Client::shutdown(self)` consume their lifecycle owner. Store implementations remain generic through the builder and are type-erased after construction, so `Server<S>` and `Client` do not expose backend types.

This is a deliberate breaking replacement rather than a deprecated compatibility layer: the removed local mode, config enums, `ActorRef`, and old error boundaries encode semantics that cannot be forwarded without retaining two competing APIs. ADR-0008's Server/Client separation remains valid; this ADR supersedes its concrete public construction and Actor-ref API shape.
