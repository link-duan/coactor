use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};
use serde::{Deserialize, Serialize};

use super::wall_time_millis;
use crate::{
    ActorAddress, ActorOwnerRecord, ActorOwnerStore, AmbiguousMutation, CoordinationError,
    LeaseMutation, LeaseToken, Mutation, NodeDirectory, NodeLeaseStore, NodeRecord, NodeSessionId,
    Revision, VersionedActorOwnerRecord,
};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum CoordinationConfig {
    S3(S3CoordinationConfig),
}

impl From<S3CoordinationConfig> for CoordinationConfig {
    fn from(config: S3CoordinationConfig) -> Self {
        Self::S3(config)
    }
}

impl CoordinationConfig {
    pub(crate) fn build(self) -> crate::CoordinationStores {
        match self {
            Self::S3(config) => {
                crate::CoordinationStores::new(Arc::new(S3CoordinationStore::new(config)))
            }
        }
    }
}

#[derive(Clone)]
pub struct S3CoordinationConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub credentials_provider: SharedCredentialsProvider,
    pub request_timeout: Duration,
    #[cfg(test)]
    pub(crate) http_client: Option<aws_smithy_runtime_api::client::http::SharedHttpClient>,
}

impl fmt::Debug for S3CoordinationConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3CoordinationConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field("credentials_provider", &"[redacted]")
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl S3CoordinationConfig {
    pub fn local(
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        endpoint_url: impl Into<String>,
    ) -> Self {
        Self {
            bucket: bucket.into(),
            prefix: prefix.into(),
            region: "us-east-1".to_owned(),
            endpoint_url: Some(endpoint_url.into()),
            credentials_provider: SharedCredentialsProvider::new(Credentials::new(
                "test-access-key",
                "test-secret-key",
                None,
                None,
                "coactor-local-test",
            )),
            request_timeout: Duration::from_secs(2),
            #[cfg(test)]
            http_client: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoredNodeLease {
    #[serde(flatten)]
    node: NodeRecord,
    expires_at_unix_ms: u64,
}

pub(crate) struct S3CoordinationStore {
    client: Client,
    bucket: Arc<str>,
    prefix: Arc<str>,
}

impl S3CoordinationStore {
    pub(crate) fn new(config: S3CoordinationConfig) -> Self {
        let mut sdk = aws_sdk_s3::Config::builder()
            .behavior_version_latest()
            .region(Region::new(config.region))
            .credentials_provider(config.credentials_provider)
            .force_path_style(true)
            .retry_config(RetryConfig::disabled())
            .timeout_config(
                TimeoutConfig::builder()
                    .operation_attempt_timeout(config.request_timeout)
                    .build(),
            );
        if let Some(endpoint_url) = config.endpoint_url {
            sdk = sdk.endpoint_url(endpoint_url);
        }
        #[cfg(test)]
        if let Some(http_client) = config.http_client {
            sdk = sdk.http_client(http_client);
        }
        Self {
            client: Client::from_conf(sdk.build()),
            bucket: config.bucket.into(),
            prefix: config.prefix.trim_matches('/').to_owned().into(),
        }
    }

    fn node_key(&self, session_id: &NodeSessionId) -> String {
        format!("{}/nodes/{}.json", self.prefix, session_id.as_str())
    }

    fn actor_key(&self, address: &ActorAddress) -> String {
        format!(
            "{}/actors/{}/ownership.json",
            self.prefix,
            hex::encode(address.to_bytes())
        )
    }

    async fn read_node_key(
        &self,
        key: String,
    ) -> Result<Option<(NodeRecord, LeaseToken)>, CoordinationError> {
        let output = match self
            .client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_no_such_key()) =>
            {
                return Ok(None);
            }
            Err(_) => return Err(CoordinationError::Unavailable),
        };
        let token = output.e_tag.ok_or(CoordinationError::Failed)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|_| CoordinationError::Unavailable)?
            .into_bytes();
        let lease: StoredNodeLease =
            serde_json::from_slice(&bytes).map_err(|_| CoordinationError::Failed)?;
        if lease.expires_at_unix_ms <= wall_time_millis() {
            return Ok(None);
        }
        Ok(Some((lease.node, LeaseToken::new(token))))
    }

    async fn put_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        token: Option<&LeaseToken>,
    ) -> Result<LeaseMutation, CoordinationError> {
        let lease = StoredNodeLease {
            node,
            expires_at_unix_ms: wall_time_millis()
                .saturating_add(ttl.as_millis().try_into().unwrap_or(u64::MAX)),
        };
        let body = serde_json::to_vec(&lease).map_err(|_| CoordinationError::Failed)?;
        let mut request = self
            .client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(self.node_key(&lease.node.session_id))
            .body(ByteStream::from(body));
        request = match token {
            Some(token) => request.if_match(token.as_str()),
            None => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(output) => output
                .e_tag
                .map(|etag| LeaseMutation::Applied {
                    token: LeaseToken::new(etag),
                })
                .ok_or(CoordinationError::Failed),
            Err(error) => classify_lease_write_error(&error),
        }
    }

    async fn read_actor_key(
        &self,
        key: String,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        let output = match self
            .client
            .get_object()
            .bucket(self.bucket.as_ref())
            .key(key)
            .send()
            .await
        {
            Ok(output) => output,
            Err(error)
                if error
                    .as_service_error()
                    .is_some_and(|error| error.is_no_such_key()) =>
            {
                return Ok(None);
            }
            Err(_) => return Err(CoordinationError::Unavailable),
        };
        let revision = output.e_tag.ok_or(CoordinationError::Failed)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|_| CoordinationError::Unavailable)?
            .into_bytes();
        let record = serde_json::from_slice(&bytes).map_err(|_| CoordinationError::Failed)?;
        Ok(Some(VersionedActorOwnerRecord {
            record,
            revision: Revision::new(revision),
        }))
    }

    async fn put_actor(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        revision: Option<&Revision>,
    ) -> Result<Mutation, CoordinationError> {
        let body = serde_json::to_vec(&record).map_err(|_| CoordinationError::Failed)?;
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
                .map(|etag| Mutation::Applied {
                    revision: Revision::new(etag),
                })
                .ok_or(CoordinationError::Failed),
            Err(error) => classify_mutation_error(&error),
        }
    }
}

#[async_trait]
impl NodeDirectory for S3CoordinationStore {
    async fn read_node(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<NodeRecord>, CoordinationError> {
        Ok(self
            .read_node_key(self.node_key(session_id))
            .await?
            .map(|(node, _)| node))
    }

    async fn list_nodes(&self) -> Result<Vec<NodeRecord>, CoordinationError> {
        let prefix = format!("{}/nodes/", self.prefix);
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
            let output = request
                .send()
                .await
                .map_err(|_| CoordinationError::Unavailable)?;
            for object in output.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if let Some((node, _)) = self.read_node_key(key.to_owned()).await? {
                    nodes.push(node);
                }
            }
            if !output.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = output.next_continuation_token;
            if continuation_token.is_none() {
                return Err(CoordinationError::Failed);
            }
        }
        Ok(nodes)
    }
}

#[async_trait]
impl NodeLeaseStore for S3CoordinationStore {
    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<(NodeRecord, LeaseToken)>, CoordinationError> {
        self.read_node_key(self.node_key(session_id)).await
    }

    async fn acquire_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
    ) -> Result<LeaseMutation, CoordinationError> {
        self.put_node(node, ttl, None).await
    }

    async fn renew_node(
        &self,
        node: NodeRecord,
        ttl: Duration,
        token: &LeaseToken,
    ) -> Result<LeaseMutation, CoordinationError> {
        self.put_node(node, ttl, Some(token)).await
    }

    async fn release_node(
        &self,
        session_id: &NodeSessionId,
        token: &LeaseToken,
    ) -> Result<LeaseMutation, CoordinationError> {
        match self
            .client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(self.node_key(session_id))
            .if_match(token.as_str())
            .send()
            .await
        {
            Ok(_) => Ok(LeaseMutation::Applied {
                token: token.clone(),
            }),
            Err(error) => classify_lease_write_error(&error),
        }
    }
}

#[async_trait]
impl ActorOwnerStore for S3CoordinationStore {
    async fn read_actor_owner(
        &self,
        address: &ActorAddress,
    ) -> Result<Option<VersionedActorOwnerRecord>, CoordinationError> {
        self.read_actor_key(self.actor_key(address)).await
    }

    async fn compare_exchange_actor_owner(
        &self,
        address: &ActorAddress,
        record: ActorOwnerRecord,
        revision: Option<&Revision>,
    ) -> Result<Mutation, CoordinationError> {
        self.put_actor(address, record, revision).await
    }
}

fn ambiguous_reason<E>(
    error: &aws_sdk_s3::error::SdkError<E>,
) -> Result<AmbiguousMutation, CoordinationError>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    use aws_sdk_s3::error::SdkError;
    Ok(match error {
        SdkError::TimeoutError(_) => AmbiguousMutation::Timeout,
        SdkError::ResponseError(_) | SdkError::DispatchFailure(_) => {
            AmbiguousMutation::ResponseLost
        }
        SdkError::ConstructionFailure(_) => return Err(CoordinationError::Failed),
        SdkError::ServiceError(_) => unreachable!("service errors were handled above"),
        _ => AmbiguousMutation::DispatchUnknown,
    })
}

fn classify_lease_write_error<E>(
    error: &aws_sdk_s3::error::SdkError<E>,
) -> Result<LeaseMutation, CoordinationError>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    if error
        .raw_response()
        .is_some_and(|response| response.status().as_u16() == 412)
    {
        Ok(LeaseMutation::Conflict)
    } else if error.as_service_error().is_some() {
        Err(CoordinationError::Failed)
    } else {
        Ok(LeaseMutation::Ambiguous(ambiguous_reason(error)?))
    }
}

fn classify_mutation_error<E>(
    error: &aws_sdk_s3::error::SdkError<E>,
) -> Result<Mutation, CoordinationError>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    if error
        .raw_response()
        .is_some_and(|response| response.status().as_u16() == 412)
    {
        Ok(Mutation::Conflict)
    } else if error.as_service_error().is_some() {
        Err(CoordinationError::Failed)
    } else {
        Ok(Mutation::Ambiguous(ambiguous_reason(error)?))
    }
}
