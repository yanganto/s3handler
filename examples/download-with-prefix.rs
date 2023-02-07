#[tokio::main]
async fn main() {
    println!("Example for download one day database near protocol ");
    let mut collections = s3handler::none_blocking::primitives::S3Pool::new(
        "s3.ca-central-1.amazonaws.com".to_string(),
    )
    .bucket("near-protocol-public")
    .prefix("backups/testnet/rpc/2023-02-06T00:00:29Z/")
    .list()
    .await
    .unwrap();

    std::fs::create_dir_all("/tmp/backups/testnet/rpc/2023-02-06T00:00:29Z/")
        .expect("Could not create folder for stroage");

    let cannel = s3handler::none_blocking::primitives::S3Pool::new(
        "s3.ca-central-1.amazonaws.com".to_string(),
    )
    .bucket("near-protocol-public")
    .toward("/tmp")
    .expect("Should store object under /tmp");

    while let Ok(Some(obj)) = collections.next_object().await {
        if let Err(e) = cannel.pull_obj(obj).await {
            println!("{e:}");
        }
    }
}
