#[tokio::main]
async fn main() {
    println!("Example for download one day database near protocol ");
    let near_backup_bucket = s3handler::none_blocking::primitives::S3Pool::new(
        "s3.ca-central-1.amazonaws.com".to_string(),
    )
    .bucket("near-protocol-public");
    let mut collections = near_backup_bucket
        .prefix("backups/testnet/rpc/2023-02-06T00:00:29Z/")
        .list()
        .await
        .unwrap();

    while let Ok(Some(obj)) = collections.next_object().await {
        println!("{}", obj.key.unwrap_or_default());
    }
}
