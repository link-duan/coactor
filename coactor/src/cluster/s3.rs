use std::{error::Error, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_sdk_s3::{Client, primitives::ByteStream};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use super::wall_time_millis;
use crate::coordination::CoordinationErrorKind;
use crate::{
    ActorAddress, ActorOwnerReader, ActorOwnerRecord, ActorOwnerStore, CoordinationError,
    MutationOutcome, NodeDirectory, NodeLeaseStore, NodeRecord, Revision,
    VersionedActorOwnerRecord,
};

#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum S3StoreConfigError {
    #[error("S3 bucket must not be empty")]
    EmptyBucket,
    #[error(
        "S3 prefix must be empty or contain canonical non-dot path segments without leading/trailing slashes"
    )]
    InvalidPrefix,
}

struct DirectoryCache {
    nodes: Vec<NodeRecord>,
    refresh_at: Option<tokio::time::Instant>,
}

/// AWS S3-backed implementation of the Coordination Store capabilities.
pub struct S3CoordinationStore {
    client: Client,
    bucket: Arc<str>,
    prefix: Arc<str>,
    directory_refresh_interval: Duration,
    directory_cache: tokio::sync::Mutex<DirectoryCache>,
}

impl S3CoordinationStore {
    /// Creates a store from an AWS SDK Client, bucket, and canonical object prefix.
    pub fn new(
        client: Client,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
    ) -> Result<Self, S3StoreConfigError> {
        let bucket = bucket.into();
        let prefix = prefix.into();
        if bucket.trim().is_empty() {
            return Err(S3StoreConfigError::EmptyBucket);
        }
        if !prefix.is_empty()
            && (prefix.starts_with('/')
                || prefix.ends_with('/')
                || prefix
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == ".."))
        {
            return Err(S3StoreConfigError::InvalidPrefix);
        }
        Ok(Self {
            client,
            bucket: bucket.into(),
            prefix: prefix.into(),
            directory_refresh_interval: Duration::from_secs(3),
            directory_cache: tokio::sync::Mutex::new(DirectoryCache {
                nodes: Vec::new(),
                refresh_at: None,
            }),
        })
    }

    /// Sets the Node Directory cache refresh interval.
    ///
    /// A zero duration is valid and effectively disables caching.
    pub fn directory_refresh_interval(mut self, interval: Duration) -> Self {
        self.directory_refresh_interval = interval;
        self
    }

    fn key(&self, suffix: &str) -> String {
        if self.prefix.is_empty() {
            suffix.to_owned()
        } else {
            format!("{}/{suffix}", self.prefix)
        }
    }
    fn node_key(&self, node_id: &str) -> String {
        self.key(&format!("nodes/{node_id}/lease.json"))
    }
    fn actor_key(&self, address: &ActorAddress) -> String {
        self.key(&format!(
            "actors/{}/{}/ownership.json",
            address.actor_type(),
            address.actor_id()
        ))
    }
    fn nodes_prefix(&self) -> String {
        self.key("nodes/")
    }

    async fn read_node_key(
        &self,
        node_id: &str,
        include_expired: bool,
    ) -> Result<Option<(NodeRecord, Revision, u64)>, CoordinationError> {
        let output = match self
            .client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(self.node_key(node_id))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if error.as_service_error().is_some_and(|e| e.is_no_such_key()) => {
                return Ok(None);
            }
            Err(error) => return Err(classify_read_error(error)),
        };
        let revision = output
            .e_tag
            .ok_or_else(|| CoordinationError::from_kind(CoordinationErrorKind::InvalidData))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::Unavailable, error))?
            .into_bytes();
        let mut lease: StoredNodeLease = serde_json::from_slice(&bytes)
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::InvalidData, error))?;
        lease.node.node_id = node_id.to_owned();
        if !include_expired && lease.expires_at_unix_ms <= wall_time_millis() {
            return Ok(None);
        }
        Ok(Some((
            lease.node,
            Revision::new(revision),
            lease.expires_at_unix_ms,
        )))
    }

    async fn put_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        revision: Option<&Revision>,
    ) -> Result<MutationOutcome<Revision>, CoordinationError> {
        let key = self.node_key(&node.node_id);
        let lease = StoredNodeLease {
            node,
            expires_at_unix_ms: wall_time_millis()
                .saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
        };
        let body = serde_json::to_vec(&lease)
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::InvalidData, error))?;
        let mut request = self
            .client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .body(ByteStream::from(body));
        request = match revision {
            Some(revision) => request.if_match(revision.as_str()),
            None => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(output) => output
                .e_tag
                .map(|etag| MutationOutcome::Applied(Revision::new(etag)))
                .ok_or_else(|| CoordinationError::from_kind(CoordinationErrorKind::InvalidData)),
            Err(error) => classify_write_error(error),
        }
    }

    async fn read_actor_key(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        let output = match self
            .client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(self.actor_key(address))
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if error.as_service_error().is_some_and(|e| e.is_no_such_key()) => {
                return Ok(None);
            }
            Err(error) => return Err(classify_read_error(error)),
        };
        let revision = output
            .e_tag
            .ok_or_else(|| CoordinationError::from_kind(CoordinationErrorKind::InvalidData))?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::Unavailable, error))?
            .into_bytes();
        let record = serde_json::from_slice(&bytes)
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::InvalidData, error))?;
        Ok(Some(VersionedActorOwnerRecord {
            record,
            revision: Revision::new(revision),
        }))
    }

    async fn fetch_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        let prefix = self.nodes_prefix();
        let mut continuation_token = None;
        let mut nodes = Vec::new();
        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(self.bucket.as_ref())
                .prefix(&prefix);
            if let Some(token) = continuation_token {
                request = request.continuation_token(token);
            }
            let output = request.send().await.map_err(classify_read_error)?;
            for object in output.contents() {
                let Some(key) = object.key() else { continue };
                let Some(rest) = key.strip_prefix(&prefix) else {
                    continue;
                };
                let Some(node_id) = rest.strip_suffix("/lease.json") else {
                    continue;
                };
                if node_id.is_empty() || node_id.contains('/') {
                    continue;
                }
                if let Some((node, _, _)) = self.read_node_key(node_id, false).await? {
                    nodes.push(node);
                }
            }
            if !output.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = output.next_continuation_token;
            if continuation_token.is_none() {
                return Err(CoordinationError::from_kind(
                    CoordinationErrorKind::InvalidData,
                ));
            }
        }
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(nodes)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredNodeLease {
    #[serde(flatten)]
    node: NodeRecord,
    expires_at_unix_ms: u64,
}

#[async_trait]
impl NodeDirectory for S3CoordinationStore {
    async fn read_node(&self, node_id: &str) -> Result<Option<NodeRecord>, CoordinationError> {
        Ok(self
            .read_node_key(node_id, false)
            .await?
            .map(|(node, _, _)| node))
    }
    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        let mut cache = self.directory_cache.lock().await;
        if cache
            .refresh_at
            .is_some_and(|at| tokio::time::Instant::now() < at)
        {
            return Ok(cache.nodes.clone());
        }
        let nodes = self.fetch_nodes().await?;
        let jitter = 0.8 + (rand::random::<u16>() % 401) as f64 / 1000.0;
        cache.refresh_at =
            Some(tokio::time::Instant::now() + self.directory_refresh_interval.mul_f64(jitter));
        cache.nodes = nodes.clone();
        Ok(nodes)
    }
}

#[async_trait]
impl NodeLeaseStore for S3CoordinationStore {
    async fn read_node_lease(
        &self,
        node_id: &str,
    ) -> Result<Option<(NodeRecord, Revision)>, CoordinationError> {
        Ok(self
            .read_node_key(node_id, false)
            .await?
            .map(|(node, revision, _)| (node, revision)))
    }
    async fn acquire_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
    ) -> Result<MutationOutcome<Revision>, CoordinationError> {
        match self.read_node_key(&node.node_id, true).await? {
            None => self.put_node(node, ttl, None).await,
            Some((_, revision, expires_at)) if expires_at <= wall_time_millis() => {
                self.put_node(node, ttl, Some(&revision)).await
            }
            Some(_) => Ok(MutationOutcome::Conflict),
        }
    }
    async fn renew_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        revision: &Revision,
    ) -> Result<MutationOutcome<Revision>, CoordinationError> {
        self.put_node(node, ttl, Some(revision)).await
    }
    async fn release_node(
        &self,
        node_id: &str,
        revision: &Revision,
    ) -> Result<MutationOutcome<()>, CoordinationError> {
        match self
            .client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(self.node_key(node_id))
            .if_match(revision.as_str())
            .send()
            .await
        {
            Ok(_) => Ok(MutationOutcome::Applied(())),
            Err(error) => classify_write_error(error).map(|outcome| match outcome {
                MutationOutcome::Applied(_) => MutationOutcome::Applied(()),
                MutationOutcome::Conflict => MutationOutcome::Conflict,
                MutationOutcome::Indeterminate(error) => MutationOutcome::Indeterminate(error),
            }),
        }
    }
}

#[async_trait]
impl ActorOwnerReader for S3CoordinationStore {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        self.read_actor_key(address).await
    }
}

#[async_trait]
impl ActorOwnerStore for S3CoordinationStore {
    async fn compare_exchange_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        revision: Option<&Revision>,
    ) -> Result<MutationOutcome<Revision>, CoordinationError> {
        let body = serde_json::to_vec(&record)
            .map_err(|error| CoordinationError::new(CoordinationErrorKind::InvalidData, error))?;
        let mut request = self
            .client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(self.actor_key(address))
            .body(ByteStream::from(body));
        request = match revision {
            Some(revision) => request.if_match(revision.as_str()),
            None => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(output) => output
                .e_tag
                .map(|etag| MutationOutcome::Applied(Revision::new(etag)))
                .ok_or_else(|| CoordinationError::from_kind(CoordinationErrorKind::InvalidData)),
            Err(error) => classify_write_error(error),
        }
    }
}

fn classify_read_error<E>(error: aws_sdk_s3::error::SdkError<E>) -> CoordinationError
where
    E: Error + Send + Sync + 'static + aws_sdk_s3::error::ProvideErrorMetadata,
{
    let kind = if error
        .raw_response()
        .is_some_and(|response| matches!(response.status().as_u16(), 401 | 403))
    {
        CoordinationErrorKind::PermissionDenied
    } else {
        CoordinationErrorKind::Unavailable
    };
    CoordinationError::new(kind, error)
}

fn classify_write_error<E>(
    error: aws_sdk_s3::error::SdkError<E>,
) -> Result<MutationOutcome<Revision>, CoordinationError>
where
    E: Error + Send + Sync + 'static + aws_sdk_s3::error::ProvideErrorMetadata,
{
    if error
        .raw_response()
        .is_some_and(|response| response.status().as_u16() == 412)
    {
        Ok(MutationOutcome::Conflict)
    } else if error.as_service_error().is_some() {
        Err(classify_read_error(error))
    } else {
        Ok(MutationOutcome::Indeterminate(classify_read_error(error)))
    }
}
