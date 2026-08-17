use coactor::{ActorAddress, Client, coordination::backend::s3::S3CoordinationStore};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bucket = std::env::var("COACTOR_S3_BUCKET")?;
    let prefix = std::env::var("COACTOR_S3_PREFIX").unwrap_or_default();
    let sdk = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let directory = S3CoordinationStore::new(aws_sdk_s3::Client::new(&sdk), bucket, prefix)?;
    let client = Client::builder(directory).build()?;

    let address = ActorAddress::new("counter", "counter-7")?;
    let mut session = client.open(&address).await?;
    session.send(b"increment".to_vec()).await?;

    match session.recv().await {
        Some(Ok(event)) => println!("{}", String::from_utf8(event)?),
        Some(Err(error)) => return Err(error.into()),
        None => eprintln!("the Session ended before an Event arrived"),
    }

    client.shutdown().await;
    Ok(())
}
