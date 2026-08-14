use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use axum::{
    Router,
    body::{Body, to_bytes},
    extract::State,
    http::{HeaderMap, Method, Request, Response, StatusCode},
    routing::any,
};
use coactor::{
    ActorAddress, ActorId, ActorOwner, ActorOwnerRecord, AmbiguousMutation, CommandContext,
    LeaseMutation, LeaseTiming, NodeLease, OwnershipBackend, OwnershipBackendError, RuntimeBuilder,
    S3OwnershipBackend, S3OwnershipConfig, SendError, VersionedActorOwnerRecord,
    VersionedNodeLease, actor, cluster::ClusterConfig,
};
use tokio::{io::AsyncReadExt, sync::Notify};

#[path = "support/s3_fixture.rs"]
mod s3_fixture;

use crate::http_fixture::ReplyDroppingProxy;
use s3_fixture::{ProgrammableS3, unused_loopback_address};

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone)]
struct ProgrammedResponse {
    status: StatusCode,
    headers: Vec<(&'static str, &'static str)>,
    body: String,
}

#[derive(Clone, Default)]
struct ServerState {
    requests: Arc<Mutex<Vec<CapturedRequest>>>,
    responses: Arc<Mutex<VecDeque<ProgrammedResponse>>>,
}

async fn handle(State(state): State<ServerState>, request: Request<Body>) -> Response<Body> {
    let (parts, body) = request.into_parts();
    let body = to_bytes(body, usize::MAX).await.unwrap().to_vec();
    state.requests.lock().unwrap().push(CapturedRequest {
        method: parts.method,
        path: parts.uri.path().to_owned(),
        headers: parts.headers,
        body,
    });
    let response = state.responses.lock().unwrap().pop_front().unwrap();
    let mut builder = Response::builder().status(response.status);
    for (name, value) in response.headers {
        builder = builder.header(name, value);
    }
    builder.body(Body::from(response.body)).unwrap()
}

async fn contract_server(
    responses: impl IntoIterator<Item = ProgrammedResponse>,
) -> (String, ServerState, tokio::task::JoinHandle<()>) {
    let state = ServerState::default();
    state.responses.lock().unwrap().extend(responses);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = Router::new()
        .fallback(any(handle))
        .with_state(state.clone());
    let task = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    (format!("http://{address}"), state, task)
}

fn response(
    status: StatusCode,
    etag: Option<&'static str>,
    body: impl Into<String>,
) -> ProgrammedResponse {
    ProgrammedResponse {
        status,
        headers: etag.into_iter().map(|etag| ("etag", etag)).collect(),
        body: body.into(),
    }
}

fn lease() -> NodeLease {
    serde_json::from_value(serde_json::json!({
        "node_id": "node-a",
        "session_id": "session-a",
        "advertised_address": "127.0.0.1:41001",
        "protocol_version": 1,
        "expires_at_unix_ms": 12345
    }))
    .unwrap()
}

fn storage(endpoint: String) -> S3OwnershipBackend {
    S3OwnershipBackend::new(S3OwnershipConfig::local(
        "lease-bucket",
        "contract-prefix",
        endpoint,
    ))
}

#[test]
fn local_configuration_redacts_test_credentials() {
    let config =
        S3OwnershipConfig::local("lease-bucket", "contract-prefix", "http://127.0.0.1:9000");

    let debug = format!("{config:?}");

    assert!(!debug.contains("test-access-key"));
    assert!(!debug.contains("test-secret-key"));
}

#[tokio::test]
async fn public_cluster_start_uses_the_built_in_s3_authority() {
    let (endpoint, server, task) = contract_server([
        response(StatusCode::OK, Some("\"lease-1\""), ""),
        response(StatusCode::NO_CONTENT, None, ""),
    ])
    .await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let ownership = S3OwnershipConfig::local("lease-bucket", "contract-prefix", endpoint);
    let config = ClusterConfig::new("node-a", address, address, ownership);

    let runtime = RuntimeBuilder::cluster((), config).start().await.unwrap();
    runtime.shutdown().await;

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, Method::PUT);
    assert!(requests[0].path.contains("/nodes/"));
    assert_eq!(requests[1].method, Method::DELETE);
    drop(requests);
    task.abort();
}

async fn assert_node_lease_behavior(storage: &dyn OwnershipBackend) {
    let current = lease();
    let acquired = storage.acquire_node_lease(current.clone()).await.unwrap();
    let LeaseMutation::Applied { etag: first_etag } = acquired else {
        panic!("acquire must apply");
    };
    assert_eq!(
        storage.acquire_node_lease(current.clone()).await.unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage.read_node_lease(&current.session_id).await.unwrap(),
        Some(VersionedNodeLease {
            lease: current.clone(),
            etag: first_etag.clone(),
        })
    );
    assert_eq!(
        storage
            .renew_node_lease(current.clone(), "stale-etag")
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    let renewed = storage
        .renew_node_lease(current.clone(), &first_etag)
        .await
        .unwrap();
    let LeaseMutation::Applied { etag: second_etag } = renewed else {
        panic!("renew must apply");
    };
    assert_eq!(
        storage
            .release_node_lease(&current.session_id, &first_etag)
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage
            .release_node_lease(&current.session_id, &second_etag)
            .await
            .unwrap(),
        LeaseMutation::Applied { etag: second_etag }
    );
    assert_eq!(
        storage.read_node_lease(&current.session_id).await.unwrap(),
        None
    );
}

#[derive(Default)]
struct MemoryLeaseStorage {
    entries: Mutex<HashMap<String, VersionedNodeLease>>,
    next_etag: Mutex<u64>,
}

impl MemoryLeaseStorage {
    fn etag(&self) -> String {
        let mut next = self.next_etag.lock().unwrap();
        *next += 1;
        format!("etag-{next}")
    }
}

#[async_trait]
impl OwnershipBackend for MemoryLeaseStorage {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let key = lease.session_id.as_str().to_owned();
        let mut entries = self.entries.lock().unwrap();
        if entries.contains_key(&key) {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let etag = self.etag();
        entries.insert(
            key,
            VersionedNodeLease {
                lease,
                etag: etag.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag })
    }

    async fn read_node_lease(
        &self,
        session_id: &coactor::NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipBackendError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .get(session_id.as_str())
            .cloned())
    }

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipBackendError> {
        Ok(self.entries.lock().unwrap().values().cloned().collect())
    }

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let key = lease.session_id.as_str().to_owned();
        let mut entries = self.entries.lock().unwrap();
        if !entries.get(&key).is_some_and(|entry| entry.etag == etag) {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        let etag = self.etag();
        entries.insert(
            key,
            VersionedNodeLease {
                lease,
                etag: etag.clone(),
            },
        );
        Ok(LeaseMutation::Applied { etag })
    }

    async fn release_node_lease(
        &self,
        session_id: &coactor::NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        let mut entries = self.entries.lock().unwrap();
        if !entries
            .get(session_id.as_str())
            .is_some_and(|entry| entry.etag == etag)
        {
            return Ok(LeaseMutation::ConditionalRejected);
        }
        entries.remove(session_id.as_str());
        Ok(LeaseMutation::Applied {
            etag: etag.to_owned(),
        })
    }

    async fn read_actor_owner(
        &self,
        _address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, OwnershipBackendError> {
        Err(OwnershipBackendError::Failed)
    }

    async fn claim_actor_owner(
        &self,
        _address: &ActorAddress,
        _record: ActorOwnerRecord,
        _etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipBackendError> {
        Err(OwnershipBackendError::Failed)
    }
}

#[tokio::test]
async fn aws_adapter_runs_the_node_lease_contract_with_conditional_requests() {
    let current = lease();
    let (endpoint, server, task) = contract_server([
        response(StatusCode::OK, Some("\"etag-1\""), ""),
        response(
            StatusCode::OK,
            Some("\"etag-1\""),
            serde_json::to_string(&current).unwrap(),
        ),
        response(StatusCode::OK, Some("\"etag-2\""), ""),
        response(StatusCode::NO_CONTENT, None, ""),
    ])
    .await;
    let storage = storage(endpoint);

    assert_eq!(
        storage.acquire_node_lease(current.clone()).await.unwrap(),
        LeaseMutation::Applied {
            etag: "\"etag-1\"".into()
        }
    );
    assert_eq!(
        storage.read_node_lease(&current.session_id).await.unwrap(),
        Some(VersionedNodeLease {
            lease: current.clone(),
            etag: "\"etag-1\"".into()
        })
    );
    assert_eq!(
        storage
            .renew_node_lease(current.clone(), "\"etag-1\"")
            .await
            .unwrap(),
        LeaseMutation::Applied {
            etag: "\"etag-2\"".into()
        }
    );
    assert_eq!(
        storage
            .release_node_lease(&current.session_id, "\"etag-2\"")
            .await
            .unwrap(),
        LeaseMutation::Applied {
            etag: "\"etag-2\"".into()
        }
    );

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 4);
    assert_eq!(requests[0].method, Method::PUT);
    assert_eq!(
        requests[0].path,
        "/lease-bucket/contract-prefix/nodes/session-a.json"
    );
    assert_eq!(requests[0].headers["if-none-match"], "*");
    assert!(
        requests[0].headers["authorization"]
            .to_str()
            .unwrap()
            .contains("Credential=test-access-key/")
    );
    assert_eq!(
        serde_json::from_slice::<NodeLease>(&requests[0].body).unwrap(),
        current
    );
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(requests[2].method, Method::PUT);
    assert_eq!(requests[2].headers["if-match"], "\"etag-1\"");
    assert_eq!(requests[3].method, Method::DELETE);
    assert_eq!(requests[3].headers["if-match"], "\"etag-2\"");
    drop(requests);
    task.abort();
}

#[tokio::test]
async fn aws_adapter_runs_the_actor_owner_contract_with_conditional_requests() {
    let address = ActorAddress::new("room", ActorId::from("room-7"));
    let claimed = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: lease().session_id,
        }),
        ownership_epoch: 3,
    };
    let released = ActorOwnerRecord::unowned(3);
    let (endpoint, server, task) = contract_server([
        response(StatusCode::OK, Some("\"owner-1\""), ""),
        response(
            StatusCode::OK,
            Some("\"owner-1\""),
            serde_json::to_string(&claimed).unwrap(),
        ),
        response(StatusCode::OK, Some("\"owner-2\""), ""),
    ])
    .await;
    let storage = storage(endpoint);

    assert_eq!(
        storage
            .claim_actor_owner(&address, claimed.clone(), None)
            .await
            .unwrap(),
        LeaseMutation::Applied {
            etag: "\"owner-1\"".into()
        }
    );
    assert_eq!(
        storage.read_actor_owner(&address).await.unwrap(),
        Some(VersionedActorOwnerRecord {
            record: claimed.clone(),
            etag: "\"owner-1\"".into(),
        })
    );
    assert_eq!(
        storage
            .claim_actor_owner(&address, released.clone(), Some("\"owner-1\""))
            .await
            .unwrap(),
        LeaseMutation::Applied {
            etag: "\"owner-2\"".into()
        }
    );

    let expected_path = format!(
        "/lease-bucket/contract-prefix/actors/{}/ownership.json",
        hex::encode(address.to_bytes())
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert_eq!(requests[0].method, Method::PUT);
    assert_eq!(requests[0].path, expected_path);
    assert_eq!(requests[0].headers["if-none-match"], "*");
    assert_eq!(
        serde_json::from_slice::<ActorOwnerRecord>(&requests[0].body).unwrap(),
        claimed
    );
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(requests[2].method, Method::PUT);
    assert_eq!(requests[2].headers["if-match"], "\"owner-1\"");
    assert_eq!(
        serde_json::from_slice::<ActorOwnerRecord>(&requests[2].body).unwrap(),
        released
    );
    drop(requests);
    task.abort();
}

#[tokio::test]
async fn aws_adapter_keeps_actor_owner_conflicts_and_failures_distinct() {
    let (endpoint, _, task) = contract_server([
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "<Error><Code>InternalError</Code></Error>",
        ),
    ])
    .await;
    let storage = storage(endpoint);
    let address = ActorAddress::new("room", ActorId::from("room-7"));
    let owner = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: lease().session_id,
        }),
        ownership_epoch: 4,
    };

    assert_eq!(
        storage
            .claim_actor_owner(&address, owner.clone(), Some("\"stale\""))
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage
            .claim_actor_owner(&address, owner, Some("\"current\""))
            .await,
        Err(OwnershipBackendError::Failed)
    );
    task.abort();
}

#[tokio::test]
async fn aws_adapter_lists_node_capacity_samples_under_the_node_prefix() {
    let current = lease();
    let listed = "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>contract-prefix/nodes/session-a.json</Key></Contents></ListBucketResult>".to_owned();
    let (endpoint, server, task) = contract_server([
        response(StatusCode::OK, None, listed),
        response(
            StatusCode::OK,
            Some("\"etag-1\""),
            serde_json::to_string(&current).unwrap(),
        ),
    ])
    .await;
    let storage = storage(endpoint);

    assert_eq!(
        storage.list_node_leases().await.unwrap(),
        vec![VersionedNodeLease {
            lease: current,
            etag: "\"etag-1\"".to_owned(),
        }]
    );
    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, Method::GET);
    assert_eq!(requests[0].path, "/lease-bucket/");
    assert_eq!(requests[1].method, Method::GET);
    assert_eq!(
        requests[1].path,
        "/lease-bucket/contract-prefix/nodes/session-a.json"
    );
    drop(requests);
    task.abort();
}

#[tokio::test]
async fn aws_adapter_keeps_conditional_and_definite_failures_distinct() {
    let (endpoint, _, task) = contract_server([
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "<Error><Code>InternalError</Code></Error>",
        ),
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "<Error><Code>InternalError</Code></Error>",
        ),
    ])
    .await;
    let storage = storage(endpoint);
    let lease = lease();

    assert_eq!(
        storage.acquire_node_lease(lease.clone()).await.unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage.acquire_node_lease(lease.clone()).await,
        Err(OwnershipBackendError::Failed)
    );
    assert_eq!(
        storage.read_node_lease(&lease.session_id).await,
        Err(OwnershipBackendError::Unavailable)
    );
    task.abort();
}

#[tokio::test]
async fn aws_adapter_treats_a_malformed_lease_as_a_definite_read_failure() {
    let (endpoint, _, task) = contract_server([response(
        StatusCode::OK,
        Some("\"etag-invalid\""),
        "not-json",
    )])
    .await;
    let storage = storage(endpoint);

    assert_eq!(
        storage.read_node_lease(&lease().session_id).await,
        Err(OwnershipBackendError::Failed)
    );
    task.abort();
}

#[tokio::test]
async fn aws_adapter_keeps_release_conflicts_and_failures_distinct() {
    let (endpoint, server, task) = contract_server([
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(
            StatusCode::INTERNAL_SERVER_ERROR,
            None,
            "<Error><Code>InternalError</Code></Error>",
        ),
    ])
    .await;
    let storage = storage(endpoint);
    let session_id = lease().session_id;

    assert_eq!(
        storage
            .release_node_lease(&session_id, "\"stale-etag\"")
            .await
            .unwrap(),
        LeaseMutation::ConditionalRejected
    );
    assert_eq!(
        storage
            .release_node_lease(&session_id, "\"current-etag\"")
            .await,
        Err(OwnershipBackendError::Failed)
    );

    let requests = server.requests.lock().unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(requests[0].method, Method::DELETE);
    assert_eq!(requests[0].headers["if-match"], "\"stale-etag\"");
    assert_eq!(requests[1].headers["if-match"], "\"current-etag\"");
    drop(requests);
    task.abort();
}

#[tokio::test]
async fn aws_adapter_surfaces_a_lost_write_response_as_ambiguous_without_replay() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let accepted = Arc::new(Mutex::new(0usize));
    let accepted_by_server = accepted.clone();
    let task = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        *accepted_by_server.lock().unwrap() += 1;
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let read = stream.read(&mut buffer).await.unwrap();
            assert!(read > 0, "the SDK closed before sending the request");
            request.extend_from_slice(&buffer[..read]);
            let Some(header_end) = request.windows(4).position(|window| window == b"\r\n\r\n")
            else {
                continue;
            };
            let headers = std::str::from_utf8(&request[..header_end]).unwrap();
            let content_length = headers
                .lines()
                .find_map(|line| {
                    line.split_once(':').and_then(|(name, value)| {
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                })
                .unwrap();
            if request.len() >= header_end + 4 + content_length {
                break;
            }
        }
        drop(stream);
        if let Ok(Ok((_retry, _))) =
            tokio::time::timeout(Duration::from_millis(200), listener.accept()).await
        {
            *accepted_by_server.lock().unwrap() += 1;
        }
    });
    let storage = storage(format!("http://{address}"));

    assert_eq!(
        storage.acquire_node_lease(lease()).await.unwrap(),
        LeaseMutation::Ambiguous(AmbiguousMutation::ResponseLost)
    );
    task.await.unwrap();
    assert_eq!(
        *accepted.lock().unwrap(),
        1,
        "the SDK must not replay the write"
    );
}

#[tokio::test]
async fn aws_adapter_reports_request_timeout_separately_from_response_loss() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (_stream, _) = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
    });
    let mut config = S3OwnershipConfig::local(
        "lease-bucket",
        "contract-prefix",
        format!("http://{address}"),
    );
    config.request_timeout = Duration::from_millis(50);
    let storage = S3OwnershipBackend::new(config);

    assert_eq!(
        storage.acquire_node_lease(lease()).await.unwrap(),
        LeaseMutation::Ambiguous(AmbiguousMutation::Timeout)
    );
    task.abort();
}

#[tokio::test]
async fn node_lease_behavior_contract_passes_for_memory_and_aws_adapters() {
    assert_node_lease_behavior(&MemoryLeaseStorage::default()).await;

    let current = lease();
    let (endpoint, _, task) = contract_server([
        response(StatusCode::OK, Some("\"etag-1\""), ""),
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(
            StatusCode::OK,
            Some("\"etag-1\""),
            serde_json::to_string(&current).unwrap(),
        ),
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(StatusCode::OK, Some("\"etag-2\""), ""),
        response(
            StatusCode::PRECONDITION_FAILED,
            None,
            "<Error><Code>PreconditionFailed</Code></Error>",
        ),
        response(StatusCode::NO_CONTENT, None, ""),
        response(
            StatusCode::NOT_FOUND,
            None,
            "<Error><Code>NoSuchKey</Code></Error>",
        ),
    ])
    .await;
    assert_node_lease_behavior(&storage(endpoint)).await;
    task.abort();
}

fn cluster_config(node_id: &str, address: SocketAddr, endpoint: &str) -> ClusterConfig {
    ClusterConfig::new(
        node_id,
        address,
        address,
        S3OwnershipConfig::local("lease-bucket", "runtime-prefix", endpoint),
    )
    .lease_timing(LeaseTiming {
        ttl: Duration::from_secs(60),
        renewal_interval: Duration::from_secs(30),
        operation_timeout: Duration::from_secs(2),
        peer_connect_timeout: Duration::from_millis(200),
    })
}

#[derive(Clone, PartialEq, prost::Message)]
struct S3AddRequest {
    #[prost(int64, tag = "1")]
    amount: i64,
}

#[derive(Clone, PartialEq, prost::Message)]
struct S3AddResponse {
    #[prost(int64, tag = "1")]
    value: i64,
}

struct S3CounterActor {
    value: i64,
}

#[actor(name = "s3-counter")]
impl S3CounterActor {
    pub fn new(_actor_id: ActorId, _state: Arc<()>) -> Self {
        Self { value: 0 }
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: S3AddRequest) -> S3AddResponse {
        self.value += request.amount;
        S3AddResponse { value: self.value }
    }
}

#[derive(Clone, Default)]
struct OutcomeState {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

struct OutcomeActor {
    state: Arc<OutcomeState>,
    value: i64,
}

#[actor(name = "s3-outcome")]
impl OutcomeActor {
    pub fn new(_actor_id: ActorId, state: Arc<OutcomeState>) -> Self {
        Self { state, value: 0 }
    }

    #[coactor::command(remote)]
    pub async fn add(&mut self, _context: &CommandContext, request: S3AddRequest) -> S3AddResponse {
        self.value += request.amount;
        S3AddResponse { value: self.value }
    }

    #[coactor::command(remote)]
    pub async fn blocked_add(
        &mut self,
        _context: &CommandContext,
        request: S3AddRequest,
    ) -> S3AddResponse {
        self.state.entered.notify_one();
        self.state.release.notified().await;
        self.value += request.amount;
        S3AddResponse { value: self.value }
    }
}

#[tokio::test]
async fn public_s3_adapter_reconciles_lost_claim_and_release_responses() {
    let (endpoint, state, task) = ProgrammableS3::start("lease-bucket").await;
    let address = unused_loopback_address();
    let actor_address = ActorAddress::new("s3-counter", ActorId::from("passivate"));
    let actor_key = format!(
        "runtime-prefix/actors/{}/ownership.json",
        hex::encode(actor_address.to_bytes())
    );
    state.drop_next_put_response(&actor_key);
    let runtime = RuntimeBuilder::cluster((), cluster_config("node-a", address, &endpoint))
        .idle_timeout(Duration::from_millis(50))
        .register::<S3CounterActor>()
        .start()
        .await
        .unwrap();
    let counter = runtime
        .actor_ref::<S3CounterActor>(ActorId::from("passivate"))
        .unwrap();

    assert_eq!(
        counter.add(S3AddRequest { amount: 1 }).await.unwrap(),
        S3AddResponse { value: 1 }
    );
    state.drop_next_put_response(&actor_key);

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let record = state
                .object_body(&actor_key)
                .map(|body| serde_json::from_slice::<ActorOwnerRecord>(&body).unwrap());
            if record.is_some_and(|record| record.owner.is_none() && record.ownership_epoch == 1) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        state.put_count(&actor_key),
        2,
        "lost claim and release responses must be reconciled by read-back, not PUT replay"
    );

    runtime.shutdown().await;
    task.abort();
}

#[tokio::test]
async fn public_s3_adapter_runs_remote_calls_and_higher_epoch_availability_failover() {
    let (endpoint, state, task) = ProgrammableS3::start("lease-bucket").await;
    let first_address = unused_loopback_address();
    let first = RuntimeBuilder::cluster((), cluster_config("node-a", first_address, &endpoint))
        .register::<S3CounterActor>()
        .start()
        .await
        .unwrap();
    let counter = first
        .actor_ref::<S3CounterActor>(ActorId::from("shared"))
        .unwrap();
    assert_eq!(
        counter.add(S3AddRequest { amount: 2 }).await.unwrap(),
        S3AddResponse { value: 2 }
    );

    let second_address = unused_loopback_address();
    let second = RuntimeBuilder::cluster((), cluster_config("node-b", second_address, &endpoint))
        .register::<S3CounterActor>()
        .start()
        .await
        .unwrap();
    let remote_counter = second
        .actor_ref::<S3CounterActor>(ActorId::from("shared"))
        .unwrap();
    assert_eq!(
        remote_counter
            .add(S3AddRequest { amount: 3 })
            .await
            .unwrap(),
        S3AddResponse { value: 5 }
    );

    let actor_key = format!(
        "runtime-prefix/actors/{}/ownership.json",
        hex::encode(ActorAddress::new("s3-counter", ActorId::from("shared")).to_bytes())
    );
    state.drop_next_put_response(&actor_key);
    first.shutdown().await;
    assert_eq!(
        remote_counter
            .add(S3AddRequest { amount: 7 })
            .await
            .unwrap(),
        S3AddResponse { value: 7 },
        "the replacement Owner must start from empty state"
    );

    let owner: ActorOwnerRecord =
        serde_json::from_slice(&state.object_body(&actor_key).unwrap()).unwrap();
    assert_eq!(owner.ownership_epoch, 2);
    assert_eq!(owner.owner.unwrap().node_id, "node-b");
    assert_eq!(
        state.put_count(&actor_key),
        2,
        "cold claim and lost-response takeover must each issue exactly one PUT"
    );

    second.shutdown().await;
    task.abort();
}

#[tokio::test]
async fn public_s3_adapter_keeps_post_dispatch_failures_unknown_without_replay() {
    let (endpoint, state, task) = ProgrammableS3::start("lease-bucket").await;
    let owner_state = OutcomeState::default();
    let owner_address = unused_loopback_address();
    let owner = RuntimeBuilder::cluster(
        owner_state.clone(),
        cluster_config("node-a", owner_address, &endpoint),
    )
    .register::<OutcomeActor>()
    .start()
    .await
    .unwrap();
    let owner_counter = owner
        .actor_ref::<OutcomeActor>(ActorId::from("unknown"))
        .unwrap();
    owner_counter.add(S3AddRequest { amount: 0 }).await.unwrap();

    let proxy = ReplyDroppingProxy::start(owner_address).await;
    let (lease_key, lease_body) = state
        .find_object(|key, body| {
            key.starts_with("runtime-prefix/nodes/")
                && serde_json::from_slice::<NodeLease>(body)
                    .is_ok_and(|lease| lease.node_id == "node-a")
        })
        .unwrap();
    let mut lease: NodeLease = serde_json::from_slice(&lease_body).unwrap();
    lease.advertised_address = proxy.address();
    state.replace_object_body(&lease_key, serde_json::to_vec(&lease).unwrap());

    let client_address = unused_loopback_address();
    let client = RuntimeBuilder::cluster(
        OutcomeState::default(),
        cluster_config("node-b", client_address, &endpoint),
    )
    .register::<OutcomeActor>()
    .start()
    .await
    .unwrap();
    let remote_counter = client
        .actor_ref::<OutcomeActor>(ActorId::from("unknown"))
        .unwrap();
    let call =
        tokio::spawn(async move { remote_counter.blocked_add(S3AddRequest { amount: 1 }).await });
    owner_state.entered.notified().await;
    proxy.drop_connection().await;
    assert_eq!(call.await.unwrap(), Err(SendError::OutcomeUnknown));
    owner_state.release.notify_one();
    assert_eq!(
        owner_counter.add(S3AddRequest { amount: 1 }).await.unwrap(),
        S3AddResponse { value: 2 },
        "the ambiguous command must execute exactly once"
    );

    owner.shutdown().await;
    client.shutdown().await;
    task.abort();
}
