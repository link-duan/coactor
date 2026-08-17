#[path = "support/s3_fixture.rs"]
mod s3_fixture;

use std::time::Duration;

use crate::{
    ActorAddress, ActorId, ActorOwner, ActorOwnerRecord, ActorOwnerStore, LeaseMutation, Mutation,
    NodeDirectory, NodeLeaseStore, NodeRecord, NodeSessionId, S3CoordinationConfig,
    S3CoordinationStore,
};

use self::s3_fixture::ProgrammableS3;

fn node(session_id: NodeSessionId) -> NodeRecord {
    NodeRecord {
        node_id: "node-a".to_owned(),
        session_id,
        advertised_address: "http://127.0.0.1:7000".to_owned(),
        protocol_version: crate::PEER_PROTOCOL_VERSION,
        lease_generation: 0,
        sampled_at_unix_ms: crate::cluster::wall_time_millis(),
        active_actor_count: 3,
        max_actor_count: 100,
        pressured: false,
        draining: false,
    }
}

async fn store() -> (
    S3CoordinationStore,
    ProgrammableS3,
    tokio::task::JoinHandle<()>,
) {
    let (endpoint, fixture, task) = ProgrammableS3::start("coactor").await;
    (
        S3CoordinationStore::new(S3CoordinationConfig::local(
            "coactor",
            "coordination-contract",
            endpoint,
        )),
        fixture,
        task,
    )
}

#[tokio::test]
async fn s3_node_directory_only_returns_live_node_sessions() {
    let (store, _, task) = store().await;
    let session_id = NodeSessionId::generate();
    let expected = node(session_id.clone());
    let acquired = store
        .acquire_node(expected.clone(), Duration::from_millis(30))
        .await
        .unwrap();
    assert!(matches!(acquired, LeaseMutation::Applied { .. }));
    assert_eq!(
        store.read_node(&session_id).await.unwrap(),
        Some(expected.clone())
    );
    assert_eq!(store.list_nodes().await.unwrap(), vec![expected]);

    tokio::time::sleep(Duration::from_millis(40)).await;
    assert_eq!(store.read_node(&session_id).await.unwrap(), None);
    assert!(store.list_nodes().await.unwrap().is_empty());
    task.abort();
}

#[tokio::test]
async fn s3_node_lease_uses_opaque_token_for_renew_and_release() {
    let (store, _, task) = store().await;
    let session_id = NodeSessionId::generate();
    let mut expected = node(session_id.clone());
    let token = match store
        .acquire_node(expected.clone(), Duration::from_secs(1))
        .await
        .unwrap()
    {
        LeaseMutation::Applied { token } => token,
        other => panic!("unexpected acquire outcome: {other:?}"),
    };
    expected.active_actor_count = 4;
    let next = match store
        .renew_node(expected.clone(), Duration::from_secs(1), &token)
        .await
        .unwrap()
    {
        LeaseMutation::Applied { token } => token,
        other => panic!("unexpected renew outcome: {other:?}"),
    };
    assert_eq!(store.read_node(&session_id).await.unwrap(), Some(expected));
    assert_eq!(
        store.release_node(&session_id, &token).await.unwrap(),
        LeaseMutation::Conflict,
        "stale token must not release the Node Lease"
    );
    assert!(matches!(
        store.release_node(&session_id, &next).await.unwrap(),
        LeaseMutation::Applied { .. }
    ));
    assert_eq!(store.read_node(&session_id).await.unwrap(), None);
    task.abort();
}

#[tokio::test]
async fn s3_ambiguous_node_lease_write_can_be_reconciled_by_exact_read_back() {
    let (store, fixture, task) = store().await;
    let session_id = NodeSessionId::generate();
    let expected = node(session_id.clone());
    let key = format!("coordination-contract/nodes/{}.json", session_id.as_str());
    fixture.drop_next_put_response(&key);

    assert!(matches!(
        store
            .acquire_node(expected.clone(), Duration::from_secs(1))
            .await
            .unwrap(),
        LeaseMutation::Ambiguous(_)
    ));
    let (confirmed, _) = store
        .read_node_lease(&session_id)
        .await
        .unwrap()
        .expect("applied write must be readable");
    assert_eq!(confirmed, expected);
    assert_eq!(
        fixture.put_count(&key),
        1,
        "ambiguous write is not replayed"
    );
    task.abort();
}

#[tokio::test]
async fn s3_actor_owner_compare_exchange_uses_opaque_revision() {
    let (store, _, task) = store().await;
    let address = ActorAddress::new("room", ActorId::from("1"));
    let record = ActorOwnerRecord {
        owner: Some(ActorOwner {
            node_id: "node-a".to_owned(),
            session_id: NodeSessionId::generate(),
        }),
        ownership_epoch: 1,
    };
    let revision = match store
        .compare_exchange_actor_owner(&address, record.clone(), None)
        .await
        .unwrap()
    {
        Mutation::Applied { revision } => revision,
        other => panic!("unexpected claim outcome: {other:?}"),
    };
    assert_eq!(
        store
            .compare_exchange_actor_owner(&address, record.clone(), None)
            .await
            .unwrap(),
        Mutation::Conflict
    );
    let released = ActorOwnerRecord::unowned(1);
    assert!(matches!(
        store
            .compare_exchange_actor_owner(&address, released.clone(), Some(&revision))
            .await
            .unwrap(),
        Mutation::Applied { .. }
    ));
    assert_eq!(
        store
            .read_actor_owner(&address)
            .await
            .unwrap()
            .unwrap()
            .record,
        released
    );
    task.abort();
}
