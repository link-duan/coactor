use std::{
    net::SocketAddr,
    panic::{AssertUnwindSafe, resume_unwind},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::Region, error::ProvideErrorMetadata};
use aws_smithy_http_client::{
    Builder as HttpClientBuilder,
    tls::{self, rustls_provider::CryptoMode},
};
use aws_smithy_runtime_api::client::{
    http::{
        HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
        SharedHttpConnector,
    },
    result::ConnectorError,
    runtime_components::RuntimeComponents,
};
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};
use coactor::{
    ActorAddress, ActorId, ActorOwner, ActorOwnerRecord, ActorTypeConfig, ClusterRuntimeConfig,
    CommandContext, LeaseMutation, LeaseTiming, NodeLease, OwnershipBackend, RuntimeBuilder,
    RuntimeTerminationReason, S3OwnershipBackend, S3OwnershipConfig, VersionedActorOwnerRecord,
    VersionedNodeLease, actor,
};
use futures_util::FutureExt;
use tokio::sync::Notify;

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} must be set when this explicitly enabled S3 semantics test runs")
    })
}

fn unique_prefix(suite: &str) -> String {
    format!("coactor/{suite}/{}", uuid::Uuid::new_v4())
}

fn sdk_client(config: &S3OwnershipConfig) -> Client {
    let mut sdk = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new(config.region.clone()))
        .credentials_provider(config.credentials_provider.clone())
        .force_path_style(true)
        .retry_config(RetryConfig::disabled())
        .timeout_config(
            TimeoutConfig::builder()
                .operation_attempt_timeout(config.request_timeout)
                .build(),
        );
    if let Some(endpoint) = &config.endpoint_url {
        sdk = sdk.endpoint_url(endpoint);
    }
    Client::from_conf(sdk.build())
}

async fn ensure_bucket(client: &Client, bucket: &str) {
    match client.create_bucket().bucket(bucket).send().await {
        Ok(_) => {}
        Err(error)
            if error
                .as_service_error()
                .and_then(|error| error.code())
                .is_some_and(|code| {
                    code == "BucketAlreadyOwnedByYou" || code == "BucketAlreadyExists"
                }) => {}
        Err(error) => panic!("failed to create configured S3 bucket {bucket}: {error:?}"),
    }
}

async fn delete_prefix(client: &Client, bucket: &str, prefix: &str) -> Result<(), String> {
    let output = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await
        .map_err(|error| format!("failed to list cleanup prefix {prefix}: {error:?}"))?;
    for object in output.contents() {
        if let Some(key) = object.key() {
            client
                .delete_object()
                .bucket(bucket)
                .key(key)
                .send()
                .await
                .map_err(|error| {
                    format!("failed to clean qualification object {key}: {error:?}")
                })?;
        }
    }
    Ok(())
}

fn configured_s3(suite: &str) -> S3OwnershipConfig {
    let endpoint = required_env("COACTOR_S3_ENDPOINT");
    let bucket = required_env("COACTOR_S3_BUCKET");
    S3OwnershipConfig::local(bucket, unique_prefix(suite), endpoint)
}

fn aws_qualification_config(suite: &str) -> S3OwnershipConfig {
    assert_eq!(
        required_env("COACTOR_AWS_QUALIFICATION"),
        "1",
        "set COACTOR_AWS_QUALIFICATION=1 to acknowledge real AWS charges and cleanup"
    );
    let access_key = required_env("AWS_ACCESS_KEY_ID");
    let secret_key = required_env("AWS_SECRET_ACCESS_KEY");
    let session_token = std::env::var("AWS_SESSION_TOKEN").ok();
    let base_prefix = required_env("COACTOR_AWS_QUALIFICATION_PREFIX");
    S3OwnershipConfig {
        bucket: required_env("COACTOR_AWS_QUALIFICATION_BUCKET"),
        prefix: format!(
            "{}/{}/{}",
            base_prefix.trim_matches('/'),
            suite,
            uuid::Uuid::new_v4()
        ),
        region: required_env("AWS_REGION"),
        endpoint_url: None,
        credentials_provider: SharedCredentialsProvider::new(Credentials::new(
            access_key,
            secret_key,
            session_token,
            None,
            "coactor-aws-qualification",
        )),
        request_timeout: Duration::from_secs(15),
        http_client: None,
    }
}

fn node_lease(node_id: &str, session_id: &str, expires_at_unix_ms: u64) -> NodeLease {
    serde_json::from_value(serde_json::json!({
        "node_id": node_id,
        "session_id": session_id,
        "advertised_address": "127.0.0.1:41001",
        "protocol_version": 1,
        "expires_at_unix_ms": expires_at_unix_ms,
        "sampled_at_unix_ms": 1,
        "active_actor_count": 0,
        "max_actor_count": 4,
        "pressured": false,
        "draining": false
    }))
    .unwrap()
}

async fn run_complete_object_lifecycle(
    config: S3OwnershipConfig,
    create_bucket: bool,
    strict_conditional_delete: bool,
) {
    let client = sdk_client(&config);
    if create_bucket {
        ensure_bucket(&client, &config.bucket).await;
    }
    let storage = S3OwnershipBackend::new(config.clone());
    let first = node_lease("node-a", "session-a", 10_000);

    let LeaseMutation::Applied { etag: first_etag } = storage
        .acquire_node_lease(first.clone())
        .await
        .expect("initial lease PUT must reach the configured S3 endpoint")
    else {
        panic!("initial lease PUT was not applied")
    };
    assert_eq!(
        storage.acquire_node_lease(first.clone()).await.unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage.read_node_lease(&first.session_id).await.unwrap(),
        Some(VersionedNodeLease {
            lease: first.clone(),
            etag: first_etag.clone(),
        })
    );

    let renewed = node_lease("node-a", "session-a", 20_000);
    assert_eq!(
        storage
            .renew_node_lease(renewed.clone(), "\"stale-etag\"")
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    let LeaseMutation::Applied { etag: renewed_etag } = storage
        .renew_node_lease(renewed.clone(), &first_etag)
        .await
        .unwrap()
    else {
        panic!("lease renewal was not applied")
    };
    assert_ne!(
        first_etag, renewed_etag,
        "changed object content must produce a new ETag"
    );
    assert!(
        storage
            .list_node_leases()
            .await
            .unwrap()
            .iter()
            .any(|entry| { entry.lease == renewed && entry.etag == renewed_etag })
    );

    let address = ActorAddress::new("qualification", ActorId::from("actor-1"));
    let claimed = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: first.session_id.clone(),
        }),
        ownership_epoch: 1,
    };
    let LeaseMutation::Applied { etag: owner_etag } = storage
        .claim_actor_owner(&address, claimed.clone(), None)
        .await
        .unwrap()
    else {
        panic!("cold Actor claim was not applied")
    };
    assert_eq!(
        storage.read_actor_owner(&address).await.unwrap(),
        Some(VersionedActorOwnerRecord {
            record: claimed.clone(),
            etag: owner_etag.clone(),
        })
    );
    assert_eq!(
        storage
            .claim_actor_owner(
                &address,
                ActorOwnerRecord::unowned(1),
                Some("\"stale-etag\"")
            )
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    let LeaseMutation::Applied {
        etag: released_etag,
    } = storage
        .release_actor_owner(
            &address,
            &VersionedActorOwnerRecord {
                record: claimed,
                etag: owner_etag,
            },
        )
        .await
        .unwrap()
    else {
        panic!("Actor owner release was not applied")
    };
    assert_eq!(
        storage.read_actor_owner(&address).await.unwrap(),
        Some(VersionedActorOwnerRecord {
            record: ActorOwnerRecord::unowned(1),
            etag: released_etag,
        })
    );

    if strict_conditional_delete {
        assert_eq!(
            storage
                .release_node_lease(&renewed.session_id, &first_etag)
                .await
                .unwrap(),
            LeaseMutation::ConditionalRejected
        );
    }
    assert!(matches!(
        storage
            .release_node_lease(&renewed.session_id, &renewed_etag)
            .await
            .unwrap(),
        LeaseMutation::Applied { .. }
    ));
    assert_eq!(
        storage.read_node_lease(&renewed.session_id).await.unwrap(),
        None
    );
}

async fn assert_complete_object_lifecycle(
    config: S3OwnershipConfig,
    create_bucket: bool,
    strict_conditional_delete: bool,
) {
    let client = sdk_client(&config);
    let outcome = AssertUnwindSafe(run_complete_object_lifecycle(
        config.clone(),
        create_bucket,
        strict_conditional_delete,
    ))
    .catch_unwind()
    .await;
    let cleanup = delete_prefix(&client, &config.bucket, &config.prefix).await;
    finish_after_cleanup(outcome, cleanup);
}

fn finish_after_cleanup(
    outcome: Result<(), Box<dyn std::any::Any + Send>>,
    cleanup: Result<(), String>,
) {
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => {}
        (Ok(()), Err(error)) => panic!("{error}"),
        (Err(payload), Ok(())) => resume_unwind(payload),
        (Err(payload), Err(error)) => {
            eprintln!("S3 cleanup also failed: {error}");
            resume_unwind(payload);
        }
    }
}

#[derive(Clone)]
struct OwnershipProbeState {
    node_id: &'static str,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl OwnershipProbeState {
    fn new(node_id: &'static str) -> Self {
        Self {
            node_id,
            entered: Arc::new(Notify::new()),
            release: Arc::new(Notify::new()),
        }
    }
}

#[derive(Clone, PartialEq, prost::Message)]
struct GateRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct GateResponse {
    #[prost(string, tag = "1")]
    node_id: String,
    #[prost(int64, tag = "2")]
    value: i64,
}

struct OwnershipProbeActor {
    state: Arc<OwnershipProbeState>,
    value: i64,
}

#[actor(name = "ownership-probe")]
impl OwnershipProbeActor {
    pub fn new(_actor_id: ActorId, state: Arc<OwnershipProbeState>) -> Self {
        Self { state, value: 0 }
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: GateRequest) -> GateResponse {
        self.value += request.amount;
        GateResponse {
            node_id: self.state.node_id.to_owned(),
            value: self.value,
        }
    }

    #[coactor::command(remote)]
    pub async fn block(
        &mut self,
        _context: &CommandContext,
        _request: GateRequest,
    ) -> GateResponse {
        self.state.entered.notify_one();
        self.state.release.notified().await;
        GateResponse {
            node_id: self.state.node_id.to_owned(),
            value: self.value,
        }
    }
}

struct IdleOwnershipProbeActor;

#[actor(name = "idle-ownership-probe")]
impl IdleOwnershipProbeActor {
    pub fn new(_actor_id: ActorId, _state: Arc<OwnershipProbeState>) -> Self {
        Self
    }

    #[coactor::command(remote)]
    pub async fn ping(&mut self, _context: &CommandContext, _request: GateRequest) -> GateResponse {
        GateResponse {
            node_id: "idle".to_owned(),
            value: 1,
        }
    }
}

fn unused_loopback_address() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    address
}

fn fast_cluster_config(
    node_id: &str,
    address: SocketAddr,
    ownership: S3OwnershipConfig,
) -> coactor::ClusterConfig {
    coactor::ClusterConfig::new(node_id, address, address, ownership).lease_timing(LeaseTiming {
        ttl: Duration::from_secs(2),
        renewal_interval: Duration::from_millis(400),
        operation_timeout: Duration::from_secs(2),
        peer_connect_timeout: Duration::from_millis(300),
    })
}

async fn lease_for_node(storage: &dyn OwnershipBackend, node_id: &str) -> VersionedNodeLease {
    storage
        .list_node_leases()
        .await
        .unwrap()
        .into_iter()
        .find(|entry| entry.lease.node_id == node_id)
        .unwrap_or_else(|| panic!("no Node Lease found for {node_id}"))
}

async fn wait_for_owner(
    storage: &dyn OwnershipBackend,
    address: &ActorAddress,
    predicate: impl Fn(&ActorOwnerRecord) -> bool,
) -> VersionedActorOwnerRecord {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Some(record) = storage.read_actor_owner(address).await.unwrap()
                && predicate(&record.record)
            {
                return record;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for Actor Owner Record {address:?}"))
}

#[tokio::test]
#[ignore = "requires an explicitly configured S3 endpoint"]
async fn node_and_actor_records_obey_conditional_s3_updates() {
    let strict_conditional_delete =
        std::env::var("COACTOR_S3_REQUIRE_CONDITIONAL_DELETE").is_ok_and(|value| value == "1");
    assert_complete_object_lifecycle(
        configured_s3("object-lifecycle"),
        true,
        strict_conditional_delete,
    )
    .await;
}

#[tokio::test]
#[ignore = "requires an explicitly configured S3 endpoint"]
async fn multiple_runtimes_preserve_ownership_lifecycle_through_s3() {
    let mut config = configured_s3("multi-node");
    let client = sdk_client(&config);
    ensure_bucket(&client, &config.bucket).await;
    config.http_client = Some(SharedHttpClient::new(DropAppliedWriteResponseClient::new()));
    let storage = S3OwnershipBackend::new(config.clone());

    let state_a = OwnershipProbeState::new("node-a");
    let address_a = unused_loopback_address();
    let runtime_a = RuntimeBuilder::cluster(
        state_a.clone(),
        fast_cluster_config("node-a", address_a, config.clone()),
    )
    .max_active_actors(1)
    .idle_timeout(Duration::from_millis(700))
    .register::<OwnershipProbeActor>()
    .register_with::<IdleOwnershipProbeActor>(
        ActorTypeConfig::new().idle_timeout(Duration::from_millis(150)),
    )
    .start()
    .await
    .expect("node-a must acquire its S3-backed Node Lease");

    let state_b = OwnershipProbeState::new("node-b");
    let address_b = unused_loopback_address();
    let runtime_b = RuntimeBuilder::cluster(
        state_b.clone(),
        fast_cluster_config("node-b", address_b, config.clone()),
    )
    .max_active_actors(4)
    .idle_timeout(Duration::from_secs(5))
    .register::<OwnershipProbeActor>()
    .register_with::<IdleOwnershipProbeActor>(
        ActorTypeConfig::new().idle_timeout(Duration::from_millis(150)),
    )
    .start()
    .await
    .expect("node-b must acquire its S3-backed Node Lease");
    let initial_a = lease_for_node(&storage, "node-a").await;

    let remote_a = runtime_a
        .actor_ref::<OwnershipProbeActor>(ActorId::from("remote"))
        .unwrap();
    assert_eq!(
        remote_a.add(GateRequest { amount: 2 }).await.unwrap(),
        GateResponse {
            node_id: "node-a".to_owned(),
            value: 2,
        }
    );
    let remote_b = runtime_b
        .actor_ref::<OwnershipProbeActor>(ActorId::from("remote"))
        .unwrap();
    assert_eq!(
        remote_b.add(GateRequest { amount: 3 }).await.unwrap(),
        GateResponse {
            node_id: "node-a".to_owned(),
            value: 5,
        },
        "typed Actor Ref must route to the cold-call winner over loopback gRPC"
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    let renewed_a = lease_for_node(&storage, "node-a").await;
    assert!(
        renewed_a.lease.expires_at_unix_ms > initial_a.lease.expires_at_unix_ms,
        "renewal must advance the published lease expiry"
    );
    assert_ne!(
        renewed_a.etag, initial_a.etag,
        "renewal must replace the S3 object under an ETag guard"
    );

    let hold = runtime_a
        .actor_ref::<OwnershipProbeActor>(ActorId::from("remote"))
        .unwrap();
    let blocked = tokio::spawn(async move { hold.block(GateRequest { amount: 0 }).await });
    state_a.entered.notified().await;
    let placed = runtime_a
        .actor_ref::<OwnershipProbeActor>(ActorId::from("placed"))
        .unwrap()
        .add(GateRequest { amount: 1 })
        .await
        .unwrap();
    assert_eq!(
        placed.node_id, "node-b",
        "a full ingress must place one cold Actor on the advertised-capacity peer"
    );
    state_a.release.notify_one();
    blocked.await.unwrap().unwrap();
    wait_for_owner(
        &storage,
        &ActorAddress::new("ownership-probe", ActorId::from("remote")),
        |record| record.owner.is_none(),
    )
    .await;

    let idle_id = ActorId::from("idle-release");
    runtime_a
        .actor_ref::<IdleOwnershipProbeActor>(idle_id.clone())
        .unwrap()
        .ping(GateRequest { amount: 0 })
        .await
        .unwrap();
    let idle_address = ActorAddress::new("idle-ownership-probe", idle_id);
    wait_for_owner(&storage, &idle_address, |record| record.owner.is_none()).await;

    let failover_id = ActorId::from("failover");
    let failover_b = runtime_b
        .actor_ref::<OwnershipProbeActor>(failover_id.clone())
        .unwrap();
    assert_eq!(
        failover_b
            .add(GateRequest { amount: 2 })
            .await
            .unwrap()
            .value,
        2
    );
    let failover_address = ActorAddress::new("ownership-probe", failover_id);
    let owned_by_b = wait_for_owner(&storage, &failover_address, |record| {
        record
            .owner
            .as_ref()
            .is_some_and(|owner| owner.node_id == "node-b")
    })
    .await;
    assert_eq!(owned_by_b.record.ownership_epoch, 1);

    let lease_b = lease_for_node(&storage, "node-b").await;
    client
        .delete_object()
        .bucket(&config.bucket)
        .key(format!(
            "{}/nodes/{}.json",
            config.prefix, lease_b.lease.session_id
        ))
        .if_match(lease_b.etag)
        .send()
        .await
        .expect("test must definitively remove node-b authority through S3");
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(3),
            runtime_b.supervision().unwrap().terminated()
        )
        .await
        .expect("node-b must self-fence after its renewal is conditionally rejected")
        .reason,
        RuntimeTerminationReason::Fenced
    );

    let replacement = runtime_a
        .actor_ref::<OwnershipProbeActor>(ActorId::from("failover"))
        .unwrap()
        .add(GateRequest { amount: 7 })
        .await
        .unwrap();
    assert_eq!(
        replacement,
        GateResponse {
            node_id: "node-a".to_owned(),
            value: 7,
        },
        "Availability Failover must activate empty state on the higher-epoch Owner"
    );
    let taken_over = wait_for_owner(&storage, &failover_address, |record| {
        record.ownership_epoch == 2
            && record
                .owner
                .as_ref()
                .is_some_and(|owner| owner.node_id == "node-a")
    })
    .await;
    assert_eq!(taken_over.record.ownership_epoch, 2);

    let lease_a = lease_for_node(&storage, "node-a").await;
    runtime_a.shutdown().await;
    assert_eq!(
        storage
            .read_node_lease(&lease_a.lease.session_id)
            .await
            .unwrap(),
        None
    );
    assert!(
        storage
            .read_actor_owner(&failover_address)
            .await
            .unwrap()
            .is_some(),
        "graceful shutdown must retain Actor Owner Records for takeover"
    );
    runtime_b.shutdown().await;
    delete_prefix(&client, &config.bucket, &config.prefix)
        .await
        .unwrap();
}

#[derive(Debug)]
struct DropAppliedWriteResponseClient {
    inner: SharedHttpClient,
    node_write: Arc<AtomicBool>,
    actor_write: Arc<AtomicBool>,
}

impl DropAppliedWriteResponseClient {
    fn new() -> Self {
        Self {
            inner: HttpClientBuilder::new()
                .tls_provider(tls::Provider::Rustls(CryptoMode::AwsLc))
                .build_https(),
            node_write: Arc::new(AtomicBool::new(true)),
            actor_write: Arc::new(AtomicBool::new(true)),
        }
    }
}

impl HttpClient for DropAppliedWriteResponseClient {
    fn http_connector(
        &self,
        settings: &HttpConnectorSettings,
        components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(DropAppliedWriteResponseConnector {
            inner: self.inner.http_connector(settings, components),
            node_write: self.node_write.clone(),
            actor_write: self.actor_write.clone(),
        })
    }
}

#[derive(Debug)]
struct DropAppliedWriteResponseConnector {
    inner: SharedHttpConnector,
    node_write: Arc<AtomicBool>,
    actor_write: Arc<AtomicBool>,
}

impl HttpConnector for DropAppliedWriteResponseConnector {
    fn call(
        &self,
        request: aws_smithy_runtime_api::client::orchestrator::HttpRequest,
    ) -> HttpConnectorFuture {
        let drop_response = if request.method() == "PUT" {
            let path = request.uri();
            if path.contains("/nodes/") {
                self.node_write.swap(false, Ordering::SeqCst)
            } else if path.contains("/actors/") {
                self.actor_write.swap(false, Ordering::SeqCst)
            } else {
                false
            }
        } else {
            false
        };
        let inner = self.inner.clone();
        HttpConnectorFuture::new(async move {
            let response = inner.call(request).await?;
            if drop_response {
                Err(ConnectorError::io(Box::new(std::io::Error::new(
                    std::io::ErrorKind::ConnectionReset,
                    "qualification dropped an applied S3 write response",
                ))))
            } else {
                Ok(response)
            }
        })
    }
}

#[tokio::test]
#[ignore = "requires explicit temporary real-AWS credentials and an isolated bucket prefix"]
async fn aws_s3_supports_ambiguous_write_reconciliation_and_takeover() {
    let config = aws_qualification_config("protocol");
    let client = sdk_client(&config);
    assert_complete_object_lifecycle(config.clone(), false, true).await;

    let runtime_prefix = format!("{}/runtime", config.prefix);
    let mut runtime_config = config.clone();
    runtime_config.prefix = runtime_prefix.clone();
    runtime_config.http_client = Some(SharedHttpClient::new(DropAppliedWriteResponseClient::new()));
    let storage = Arc::new(S3OwnershipBackend::new(runtime_config.clone()));
    let mut runtime = None;
    let mut replacement = None;
    let outcome = AssertUnwindSafe(async {
        let address = unused_loopback_address();
        runtime = Some(
            RuntimeBuilder::local(OwnershipProbeState::new("aws-a"))
                .register::<OwnershipProbeActor>()
                .cluster_with_backend(
                    ClusterRuntimeConfig::new("aws-a", address, address).lease_timing(
                        LeaseTiming {
                            ttl: Duration::from_secs(10),
                            renewal_interval: Duration::from_secs(3),
                            operation_timeout: Duration::from_secs(5),
                            peer_connect_timeout: Duration::from_secs(2),
                        },
                    ),
                    storage.clone(),
                )
                .unwrap()
                .start()
                .await
                .expect("a lost Node Lease response must reconcile by exact real-S3 read-back"),
        );
        let runtime_ref = runtime.as_ref().unwrap();
        let actor_id = ActorId::from("ambiguous-claim");
        assert_eq!(
            runtime_ref
                .actor_ref::<OwnershipProbeActor>(actor_id.clone())
                .unwrap()
                .add(GateRequest { amount: 4 })
                .await
                .expect("a lost Actor claim response must reconcile by exact read-back")
                .value,
            4
        );

        let lease = lease_for_node(storage.as_ref(), "aws-a").await;
        client
            .delete_object()
            .bucket(&runtime_config.bucket)
            .key(format!(
                "{runtime_prefix}/nodes/{}.json",
                lease.lease.session_id
            ))
            .if_match(lease.etag)
            .send()
            .await
            .expect("qualification must remove the old Owner lease with its real AWS ETag");
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(12),
                runtime_ref.supervision().unwrap().terminated()
            )
            .await
            .expect("old Owner must self-fence after real AWS rejects renewal")
            .reason,
            RuntimeTerminationReason::Fenced
        );

        let replacement_address = unused_loopback_address();
        replacement = Some(
            RuntimeBuilder::cluster(
                OwnershipProbeState::new("aws-b"),
                fast_cluster_config("aws-b", replacement_address, runtime_config.clone()),
            )
            .register::<OwnershipProbeActor>()
            .start()
            .await
            .unwrap(),
        );
        let response = replacement
            .as_ref()
            .unwrap()
            .actor_ref::<OwnershipProbeActor>(actor_id.clone())
            .unwrap()
            .add(GateRequest { amount: 9 })
            .await
            .unwrap();
        assert_eq!(response.node_id, "aws-b");
        assert_eq!(response.value, 9);
        let owner_address = ActorAddress::new("ownership-probe", actor_id);
        let owner = wait_for_owner(storage.as_ref(), &owner_address, |record| {
            record.ownership_epoch == 2
        })
        .await;
        assert_eq!(owner.record.ownership_epoch, 2);
    })
    .catch_unwind()
    .await;

    if let Some(replacement) = replacement {
        replacement.shutdown().await;
    }
    if let Some(runtime) = runtime {
        runtime.shutdown().await;
    }
    let cleanup = delete_prefix(&client, &runtime_config.bucket, &runtime_config.prefix).await;
    finish_after_cleanup(outcome, cleanup);
}
