---
status: accepted
---

# Actor Store and Recovery as a later persistence stage

Persistence is deliberately separate from the first distributed availability stage. A later Actor Store will provide Actor-scoped transactional state without requiring consumers to adopt event sourcing, and will bind persisted state to the Actor's fenced ownership epoch so consumers do not manage physical storage identities themselves.

The planned baseline checkpoints complete Actor Store files asynchronously and therefore accept a non-zero recovery point objective; an ordinary Handler Reply remains non-durable. Planned passivation, migration and shutdown must flush dirty state before completing, while a persistence failure stops the affected Actor rather than inventing a successful durability guarantee.

Recovery selects a fixed state version from an earlier ownership epoch and does not chase writes from a stale owner. Missing history restores an empty Store, while corrupted selected state is a restore fault rather than a reason to silently choose older data. Storage layout, checkpoint scheduling and restore algorithms remain future design details and should be refined in the persistence specification before implementation.
