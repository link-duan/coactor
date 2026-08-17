---
status: accepted
---

# Stable Node identity and readable Coordination Store keys

A Node ID identifies a stable logical Server slot, while the runtime-generated Node Session ID identifies one process incarnation occupying that slot. An explicit Node ID uses Kubernetes DNS-label syntax; when omitted it defaults to the canonical advertised `host:port` endpoint. Node Directory lookup is keyed by Node ID, while Node Lease and Actor Owner records retain the Session ID and lease generation needed for exact read-back and fencing.

Coordination Store keys are deliberately readable and reversible rather than hashed or escaped:

```text
<prefix>/nodes/<node-id>/lease.json
<prefix>/actors/<actor-type>/<actor-id>/ownership.json
```

Actor Type and Actor ID both use Kubernetes DNS-label syntax, so each is a safe key segment. The object key is the record identity: Node Lease bodies do not repeat Node ID, and Actor Owner bodies do not repeat Actor Type or Actor ID. Initial records do not carry a schema-version field; one will be introduced only when an incompatible stored schema requires it.

When a Node restarts under the same Node ID, Actor ownership is not eagerly scanned or inherited. The next access performs a lazy same-node takeover that binds the new Session ID and increments Ownership Epoch; if the logical Node is unavailable or cannot admit the Actor, placement falls back to another live Node. This preserves placement preference without allowing two process incarnations to share authority.

Graceful shutdown conditionally deletes the Node Lease using its storage revision. Crash residue is ignored after lease expiry and cleaned by S3 Lifecycle rather than a runtime janitor; deployments using S3 Versioning must also expire noncurrent Node Lease versions. This ADR extends ADR-0010's Node Directory model by making stable Node ID, rather than process Session ID, the directory and storage-key identity.
