use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::Region, primitives::ByteStream};
use aws_smithy_types::{retry::RetryConfig, timeout::TimeoutConfig};

use crate::{
    AmbiguousMutation, LeaseMutation, NodeLease, NodeLeaseStorage, NodeSessionId,
    OwnershipStorageError, VersionedNodeLease,
};

#[derive(Clone)]
pub struct S3NodeLeaseConfig {
    pub bucket: String,
    pub prefix: String,
    pub region: String,
    pub endpoint_url: Option<String>,
    pub credentials_provider: SharedCredentialsProvider,
    pub request_timeout: Duration,
}

impl fmt::Debug for S3NodeLeaseConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("S3NodeLeaseConfig")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("endpoint_url", &self.endpoint_url)
            .field("credentials_provider", &"[redacted]")
            .field("request_timeout", &self.request_timeout)
            .finish()
    }
}

impl S3NodeLeaseConfig {
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
        }
    }
}

pub struct S3NodeLeaseStorage {
    client: Client,
    bucket: Arc<str>,
    prefix: Arc<str>,
}

impl S3NodeLeaseStorage {
    pub fn new(config: S3NodeLeaseConfig) -> Self {
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
        Self {
            client: Client::from_conf(sdk.build()),
            bucket: config.bucket.into(),
            prefix: config.prefix.trim_matches('/').to_owned().into(),
        }
    }

    fn key(&self, session_id: &NodeSessionId) -> String {
        format!("{}/nodes/{}.json", self.prefix, session_id.as_str())
    }

    async fn read_key(
        &self,
        key: String,
    ) -> Result<Option<VersionedNodeLease>, OwnershipStorageError> {
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
            Err(_) => return Err(OwnershipStorageError::Unavailable),
        };
        let etag = output.e_tag.ok_or(OwnershipStorageError::Failed)?;
        let bytes = output
            .body
            .collect()
            .await
            .map_err(|_| OwnershipStorageError::Unavailable)?
            .into_bytes();
        let lease = serde_json::from_slice(&bytes).map_err(|_| OwnershipStorageError::Failed)?;
        Ok(Some(VersionedNodeLease { lease, etag }))
    }

    async fn put(
        &self,
        lease: NodeLease,
        etag: Option<&str>,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        let body = serde_json::to_vec(&lease).map_err(|_| OwnershipStorageError::Failed)?;
        let mut request = self
            .client
            .put_object()
            .bucket(self.bucket.as_ref())
            .key(self.key(&lease.session_id))
            .body(ByteStream::from(body));
        request = match etag {
            Some(etag) => request.if_match(etag),
            None => request.if_none_match("*"),
        };
        match request.send().await {
            Ok(output) => output
                .e_tag
                .map(|etag| LeaseMutation::Applied { etag })
                .ok_or(OwnershipStorageError::Failed),
            Err(error) => classify_write_error(&error),
        }
    }
}

#[async_trait]
impl NodeLeaseStorage for S3NodeLeaseStorage {
    async fn acquire_node_lease(
        &self,
        lease: NodeLease,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.put(lease, None).await
    }

    async fn read_node_lease(
        &self,
        session_id: &NodeSessionId,
    ) -> Result<Option<VersionedNodeLease>, OwnershipStorageError> {
        self.read_key(self.key(session_id)).await
    }

    async fn list_node_leases(&self) -> Result<Vec<VersionedNodeLease>, OwnershipStorageError> {
        let prefix = format!("{}/nodes/", self.prefix);
        let mut continuation_token = None;
        let mut leases = Vec::new();
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
                .map_err(|_| OwnershipStorageError::Unavailable)?;
            for object in output.contents() {
                let Some(key) = object.key() else {
                    continue;
                };
                if let Some(lease) = self.read_key(key.to_owned()).await? {
                    leases.push(lease);
                }
            }
            if !output.is_truncated.unwrap_or(false) {
                break;
            }
            continuation_token = output.next_continuation_token;
            if continuation_token.is_none() {
                return Err(OwnershipStorageError::Failed);
            }
        }
        Ok(leases)
    }

    async fn renew_node_lease(
        &self,
        lease: NodeLease,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        self.put(lease, Some(etag)).await
    }

    async fn release_node_lease(
        &self,
        session_id: &NodeSessionId,
        etag: &str,
    ) -> Result<LeaseMutation, OwnershipStorageError> {
        match self
            .client
            .delete_object()
            .bucket(self.bucket.as_ref())
            .key(self.key(session_id))
            .if_match(etag)
            .send()
            .await
        {
            Ok(_) => Ok(LeaseMutation::Applied {
                etag: etag.to_owned(),
            }),
            Err(error) => classify_write_error(&error),
        }
    }
}

fn classify_write_error<E>(
    error: &aws_sdk_s3::error::SdkError<E>,
) -> Result<LeaseMutation, OwnershipStorageError>
where
    E: aws_sdk_s3::error::ProvideErrorMetadata,
{
    if error
        .raw_response()
        .is_some_and(|response| response.status().as_u16() == 412)
    {
        Ok(LeaseMutation::ConditionalRejected)
    } else if error.as_service_error().is_some() {
        Err(OwnershipStorageError::Failed)
    } else {
        use aws_sdk_s3::error::SdkError;
        let reason = match error {
            SdkError::TimeoutError(_) => AmbiguousMutation::Timeout,
            SdkError::ResponseError(_) => AmbiguousMutation::ResponseLost,
            SdkError::DispatchFailure(_) => AmbiguousMutation::ResponseLost,
            SdkError::ConstructionFailure(_) => return Err(OwnershipStorageError::Failed),
            SdkError::ServiceError(_) => unreachable!("service errors were handled above"),
            _ => AmbiguousMutation::DispatchUnknown,
        };
        Ok(LeaseMutation::Ambiguous(reason))
    }
}
