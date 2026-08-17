# S3 release gates

CoActor's built-in Coordination Store is designed around AWS S3 conditional-write semantics. The local programmable HTTP fixture verifies request contracts, but it does not prove equivalence with the real AWS service.

## Merge gates

Regular tests verify:

- exact readable keys for Node Lease and Actor Owner records;
- identity omission from record bodies;
- `If-None-Match` create and `If-Match` replace/delete behavior;
- Node Lease generation advancement;
- conflict versus indeterminate mutation classification;
- exact read-back after a response-loss simulation;
- Node Directory cache freshness and singleflight behavior.

The S3 Store is created from an AWS SDK S3 Client. Test and deployment code configure credentials, region, endpoint, retry, HTTP and timeout through the AWS SDK rather than CoActor-specific credential fields.

## Real AWS qualification

Before a production release, run the same protocol against a dedicated existing AWS bucket with temporary credentials and a run-specific canonical prefix. Qualification must verify:

- conditional create, replace and delete;
- ETag/revision preservation;
- read/list consistency after successful writes;
- exact read-back after an applied mutation whose response is lost;
- expired Node Lease takeover by storage revision;
- graceful conditional lease deletion;
- higher-epoch Actor ownership takeover under a new Node Session ID;
- runtime self-fencing after lease authority is lost.

Real AWS qualification remains an external release gate rather than a regular local or CI requirement. Until it passes in the intended deployment configuration, release claims should say “designed for AWS S3 semantics,” not “verified against AWS S3.”

## Lifecycle policy

Crash residue is ignored after Node Lease expiry; CoActor does not run a distributed janitor. Production buckets should configure Lifecycle expiration for expired Node objects. If S3 Versioning is enabled, configure noncurrent-version expiration as well so replaced and deleted lease versions do not accumulate indefinitely.
