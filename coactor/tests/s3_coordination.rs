#[path = "support/s3_fixture.rs"]
mod s3_fixture;

use std::time::Duration;

use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::Region};
use aws_smithy_types::retry::RetryConfig;

use self::s3_fixture::ProgrammableS3;
use crate::{
    ActorAddress, ActorOwner, ActorOwnerRecord, ActorOwnerStore, MutationOutcome, NodeDirectory,
    NodeLeaseStore, NodeRecord, NodeSessionId, S3CoordinationStore,
    coordination::backend::s3::S3StoreConfigError,
};

fn client(endpoint: String) -> Client {
    Client::from_conf(
        aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new("us-east-1"))
            .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
                "test", "test", None, None, "test",
            )))
            .endpoint_url(endpoint)
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .build(),
    )
}

async fn store_with_interval(
    interval: Duration,
) -> (
    S3CoordinationStore,
    ProgrammableS3,
    tokio::task::JoinHandle<()>,
) {
    let (endpoint, fixture, task) = ProgrammableS3::start("coactor").await;
    let store = S3CoordinationStore::new(client(endpoint), "coactor", "coordination-contract")
        .unwrap()
        .directory_refresh_interval(interval);
    (store, fixture, task)
}

async fn store() -> (
    S3CoordinationStore,
    ProgrammableS3,
    tokio::task::JoinHandle<()>,
) {
    store_with_interval(Duration::from_secs(3)).await
}

fn node() -> NodeRecord {
    NodeRecord {
        node_id: "node-a".to_owned(),
        session_id: NodeSessionId::generate(),
        advertised_endpoint: "127.0.0.1:7000".to_owned(),
        protocol_version: crate::TRANSPORT_PROTOCOL_VERSION,
        lease_generation: 0,
        sampled_at_unix_ms: crate::cluster::wall_time_millis(),
        active_actor_count: 3,
        max_actor_count: 100,
        pressured: false,
        draining: false,
    }
}

#[test]
fn s3_store_rejects_empty_bucket_and_noncanonical_prefix() {
    let sdk = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            "test", "test", None, None, "test",
        )))
        .build();
    assert!(matches!(
        S3CoordinationStore::new(Client::from_conf(sdk.clone()), "", ""),
        Err(S3StoreConfigError::EmptyBucket)
    ));
    assert!(matches!(
        S3CoordinationStore::new(Client::from_conf(sdk), "bucket", "/bad"),
        Err(S3StoreConfigError::InvalidPrefix)
    ));
}

#[tokio::test]
async fn expired_node_lease_is_replaced_under_the_same_node_id() {
    let (store, _, task) = store().await;
    let first = node();
    assert!(matches!(
        store
            .acquire_node(first, Duration::from_millis(10))
            .await
            .unwrap(),
        MutationOutcome::Applied(_)
    ));
    tokio::time::sleep(Duration::from_millis(20)).await;
    let replacement = node();
    assert!(matches!(
        store
            .acquire_node(replacement.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
        MutationOutcome::Applied(_)
    ));
    assert_eq!(store.read_node("node-a").await.unwrap(), Some(replacement));
    task.abort();
}

#[tokio::test]
async fn node_lease_uses_readable_key_and_omits_node_id_from_body() {
    let (store, fixture, task) = store().await;
    let expected = node();
    let revision = match store
        .acquire_node(expected.clone(), Duration::from_secs(1))
        .await
        .unwrap()
    {
        MutationOutcome::Applied(revision) => revision,
        other => panic!("unexpected outcome: {other:?}"),
    };
    let key = "coordination-contract/nodes/node-a/lease.json";
    let body = fixture.object_body(key).unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("node_id").is_none());
    assert_eq!(store.read_node("node-a").await.unwrap(), Some(expected));
    assert!(matches!(
        store.release_node("node-a", &revision).await.unwrap(),
        MutationOutcome::Applied(())
    ));
    assert!(fixture.object_body(key).is_none());
    task.abort();
}

#[tokio::test]
async fn renew_advances_generation_and_uses_conditional_revision() {
    let (store, _, task) = store().await;
    let mut expected = node();
    let revision = match store
        .acquire_node(expected.clone(), Duration::from_secs(1))
        .await
        .unwrap()
    {
        MutationOutcome::Applied(revision) => revision,
        other => panic!("unexpected outcome: {other:?}"),
    };
    expected.lease_generation = 1;
    let next = match store
        .renew_node(expected.clone(), Duration::from_secs(1), &revision)
        .await
        .unwrap()
    {
        MutationOutcome::Applied(revision) => revision,
        other => panic!("unexpected outcome: {other:?}"),
    };
    assert_ne!(next, revision);
    assert!(matches!(
        store
            .renew_node(expected, Duration::from_secs(1), &revision)
            .await
            .unwrap(),
        MutationOutcome::Conflict
    ));
    task.abort();
}

#[tokio::test]
async fn actor_owner_key_is_readable_and_body_omits_actor_identity() {
    let (store, fixture, task) = store().await;
    let address = ActorAddress::new("room", "room-one").unwrap();
    let record = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: NodeSessionId::generate(),
        }),
        ownership_epoch: 7,
    };
    assert!(matches!(
        store
            .compare_exchange_actor_owner(&address, record, None)
            .await
            .unwrap(),
        MutationOutcome::Applied(_)
    ));
    let body = fixture
        .object_body("coordination-contract/actors/room/room-one/ownership.json")
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json.get("actor_type").is_none());
    assert!(json.get("actor_id").is_none());
    task.abort();
}

#[tokio::test]
async fn directory_refresh_is_singleflight_and_zero_interval_disables_cache() {
    let (store, fixture, task) = store_with_interval(Duration::from_secs(60)).await;
    let store = std::sync::Arc::new(store);
    let calls = (0..8).map(|_| {
        let store = store.clone();
        tokio::spawn(async move { store.list_nodes().await.unwrap() })
    });
    for call in calls {
        call.await.unwrap();
    }
    assert_eq!(fixture.list_count(), 1);
    task.abort();

    let (store, fixture, task) = store_with_interval(Duration::ZERO).await;
    store.list_nodes().await.unwrap();
    store.list_nodes().await.unwrap();
    assert_eq!(fixture.list_count(), 2);
    task.abort();
}

#[tokio::test]
async fn lost_write_response_is_indeterminate_and_exact_read_back_is_available() {
    let (store, fixture, task) = store().await;
    fixture.drop_next_put_response("coordination-contract/nodes/node-a/lease.json");
    let expected = node();
    assert!(matches!(
        store
            .acquire_node(expected.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
        MutationOutcome::Indeterminate(_)
    ));
    assert_eq!(
        store.read_node_lease("node-a").await.unwrap().unwrap().0,
        expected
    );
    task.abort();
}
