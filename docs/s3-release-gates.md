# S3 release gates

CoActor's ownership adapter is **designed for AWS S3 semantics**. Local development and merge-gating CI do not prove equivalence with the real AWS service.

## Merge gates

CI runs the ignored S3 ownership-semantics tests explicitly against two endpoints:

- LocalStack: the strict gate, including conditional Node Lease deletion;
- MinIO: an additional compatibility smoke gate, not a supported production authority.

Both endpoints run the complete Node Lease and Actor Owner Record lifecycle plus multiple real Runtime instances over loopback gRPC. The multi-node gate covers typed remote calls, cold claims, lease renewal, bounded-capacity placement, runtime fencing, idle ownership release, graceful shutdown, and higher-epoch Availability Failover.

The tests never fall back to the deterministic fake. Explicit execution requires an endpoint and bucket:

```console
COACTOR_S3_ENDPOINT=http://127.0.0.1:4566 \
COACTOR_S3_BUCKET=coactor-local \
COACTOR_S3_REQUIRE_CONDITIONAL_DELETE=1 \
cargo test -p coactor --lib node_and_actor_records_obey_conditional_s3_updates -- --ignored --test-threads=1

COACTOR_S3_ENDPOINT=http://127.0.0.1:4566 \
COACTOR_S3_BUCKET=coactor-local \
COACTOR_S3_REQUIRE_CONDITIONAL_DELETE=1 \
cargo test -p coactor --lib multiple_runtimes_preserve_ownership_lifecycle_through_s3 -- --ignored --test-threads=1
```

Omit `COACTOR_S3_REQUIRE_CONDITIONAL_DELETE` only for a non-authoritative compatibility smoke endpoint that does not implement conditional `DeleteObject`. AWS qualification always requires the strict behavior.

## Real AWS qualification

Real AWS qualification is ignored by default and is not a daily CI requirement. Run it with temporary credentials and a dedicated existing bucket:

```console
COACTOR_AWS_QUALIFICATION=1 \
COACTOR_AWS_QUALIFICATION_BUCKET=my-isolated-qualification-bucket \
COACTOR_AWS_QUALIFICATION_PREFIX=coactor/qualification/my-run \
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
AWS_SESSION_TOKEN=... \
cargo test -p coactor --lib aws_s3_supports_ambiguous_write_reconciliation_and_takeover -- --ignored --test-threads=1
```

Each execution appends a UUID below the supplied prefix. A successful run deletes all objects it created. Use a run-specific prefix so an interrupted process can be cleaned safely without touching unrelated data.

The qualification verifies the protocol assumptions that matter to CoActor:

- `If-None-Match` and `If-Match` conditional writes;
- ETag preservation and guarded updates/deletes;
- immediate read/list consistency after successful writes;
- exact read-back after an applied mutation whose response is treated as lost;
- runtime self-fencing after lease loss;
- higher-epoch takeover with empty-state Availability Failover.

Passing this suite is a condition for the first production release. Until it has passed in the intended AWS deployment configuration, documentation and release claims must continue to say “designed for AWS S3 semantics,” not “verified against AWS S3.”
