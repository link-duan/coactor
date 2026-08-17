# Distributed runtime semantics

## Construction and capability boundaries

Production Server uses `Server::builder(coordination_store)` and receives a concrete type implementing `CoordinationStore`. The store type remains generic through the builder and is erased when startup returns `Server<S>`.

Client uses `Client::builder(node_directory).build()`. It holds only the read-only `NodeDirectory` capability and cannot acquire/renew Node Leases or mutate Actor Owner records. Build is synchronous and performs no directory I/O; directory access begins when opening a Session.

## Network identity

- `bind(SocketAddr)` specifies the local listener.
- `advertised_endpoint(&str)` specifies canonical `host:port` used by peers. DNS, IPv4 and bracketed IPv6 are accepted; schemes, paths, missing/zero ports are rejected.
- An optional explicit Node ID must be a Kubernetes DNS label. If omitted, the canonical advertised endpoint is the Node ID.
- Node ID is stable logical placement identity. Every Server start creates a distinct Node Session ID.

## Node Lease

Node Lease is stored and queried by Node ID. Its body contains Node Session ID, lease generation, advertised endpoint, protocol version and load sample, but does not repeat Node ID. Initial acquisition uses conditional create; takeover of an expired object uses its prior storage revision; renewal uses conditional replace and increments generation.

The default TTL is 10 seconds. Renewal interval is derived internally as TTL / 3 and jittered. Coordination operation timeout defaults to 15 seconds; peer connect timeout defaults to 3 seconds.

An indeterminate mutation is confirmed by exact read-back of Node ID key, Session ID and generation. Conflict is a protocol outcome rather than a backend error. Failure to prove authority before the lease deadline self-fences the runtime.

Graceful shutdown conditionally deletes the current Node Lease using its storage revision. A stale process cannot delete a replacement process's lease.

## Ownership and placement

Actor Owner records bind Actor Address to Node ID, Node Session ID and Ownership Epoch. Any Session ID change requires Actor Owner CAS and a higher epoch.

When a Node restarts under the same Node ID, it does not scan Actor Owner records. On the next access, a stale record for the same Node ID is lazily taken over by the new Session with a higher epoch. If the preferred logical Node is unavailable or cannot admit the Actor, the gateway falls back to ordinary placement among live Nodes.

Placement uses live Node Directory snapshots, protocol compatibility, draining/pressure/capacity filters and p2c load-ratio selection with in-flight placement bookkeeping. Ownership CAS fixes the one-time decision; CoActor does not continuously rebalance active placement.

## S3 schema and operations

```text
<prefix>/nodes/<node-id>/lease.json
<prefix>/actors/<actor-type>/<actor-id>/ownership.json
```

Actor Type, Actor ID and explicit Node ID constraints make path escaping unnecessary. Key identity is not duplicated in record bodies, and the initial format has no stored schema-version field.

The S3 Store owns Node Directory polling, cache age, singleflight refresh and jitter. A zero refresh interval disables effective caching while remaining valid.

Crash residue is ignored after lease expiry. Operators should configure Lifecycle expiration for expired Node objects; versioned buckets must also expire noncurrent versions. The runtime does not run a distributed janitor.

## Current limits

Coordination provides availability fencing, not Actor state durability. Recovery, Migration, Actor Store persistence and durable ACK remain out of scope. Nodes are assumed to deploy a homogeneous Actor Type set; capability advertisement and heterogeneous placement filtering are not implemented.
