use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
use aws_sdk_s3::{Client, config::Region};
use coactor::coordination::backend::s3::{S3CoordinationStore, S3StoreConfigError};

const ACCESS_KEY: &str = "coactor-dev";
const SECRET_KEY: &str = "coactor-dev-secret";
const ENDPOINT: &str = "http://127.0.0.1:9000";
const REGION: &str = "us-east-1";
const BUCKET: &str = "coactor";
const PREFIX: &str = "coactor/chat";

pub fn coordination_store() -> Result<S3CoordinationStore, S3StoreConfigError> {
    let config = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new(REGION))
        .credentials_provider(SharedCredentialsProvider::new(Credentials::new(
            ACCESS_KEY,
            SECRET_KEY,
            None,
            None,
            "chat-example",
        )))
        .endpoint_url(ENDPOINT)
        .force_path_style(true)
        .build();

    S3CoordinationStore::new(Client::from_conf(config), BUCKET, PREFIX)
}
