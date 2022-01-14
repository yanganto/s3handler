#[test_with::env(
    ACCESS_KEY,
    SECRET_KEY,
    S3_HOST,
    BUCKET_NAME,
    OBJECT_NAME,
    EXPECT_CONTENT
)]
#[tokio::test]
async fn test_v2_async_operation() {
    use s3handler::none_blocking::primitives::S3Pool;
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;
    use std::time::SystemTime;

    let akey = env::var("ACCESS_KEY").unwrap();
    println!("use access key: {} for async testing", akey);

    // TODO: use tmpfile crate to test on different OS
    let temp_test_file = "/tmp/async-test";
    let new_object = format!(
        "{}-async-{}",
        env::var("OBJECT_NAME").unwrap(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    );

    // List
    let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap())
        .aws_v2(akey.to_string(), env::var("SECRET_KEY").unwrap());
    let object_list = s3_pool.bucket(&env::var("BUCKET_NAME").unwrap());
    let mut list = object_list.list().await.unwrap();
    assert!(list.next_object().await.unwrap().is_some());

    // Download
    let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap())
        .aws_v2(akey.to_string(), env::var("SECRET_KEY").unwrap());
    let obj = s3_pool
        .bucket(&env::var("BUCKET_NAME").unwrap())
        .object(&env::var("OBJECT_NAME").unwrap());

    obj.download_file(temp_test_file).await.unwrap();
    let mut file = File::open(temp_test_file).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, env::var("EXPECT_CONTENT").unwrap());

    // Upload
    let obj = S3Pool::new(env::var("S3_HOST").unwrap())
        .aws_v2(akey.to_string(), env::var("SECRET_KEY").unwrap())
        .bucket(&env::var("BUCKET_NAME").unwrap())
        .object(&new_object);
    obj.upload_file(temp_test_file).await.unwrap();

    // Delete
    let obj = S3Pool::new(env::var("S3_HOST").unwrap())
        .aws_v2(akey.to_string(), env::var("SECRET_KEY").unwrap())
        .bucket(&env::var("BUCKET_NAME").unwrap())
        .object(&new_object);
    obj.remove().await.unwrap();
}

#[test_with::env(
    ACCESS_KEY,
    SECRET_KEY,
    S3_HOST,
    BUCKET_NAME,
    OBJECT_NAME,
    EXPECT_CONTENT
)]
#[test]
fn test_v2_sync_operation() {
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;
    use std::time::SystemTime;

    let akey = env::var("ACCESS_KEY").unwrap();
    println!("use access key: {} for sync testing", akey);

    // TODO: use tmpfile crate to test on different OS
    let temp_test_file = "/tmp/sync-test";
    let new_object = format!(
        "{}-sync-{}",
        env::var("OBJECT_NAME").unwrap(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    );

    // Download
    let config = s3handler::blocking::CredentialConfig {
        host: env::var("S3_HOST").unwrap(),
        access_key: env::var("ACCESS_KEY").unwrap(),
        secret_key: env::var("SECRET_KEY").unwrap(),
        user: None,
        region: None,
        s3_type: None,
        secure: None,
    };
    let mut handler = s3handler::blocking::Handler::from(&config);
    handler.change_auth_type("aws2");
    handler
        .get(
            &format!(
                "/{}/{}",
                env::var("BUCKET_NAME").unwrap(),
                env::var("OBJECT_NAME").unwrap()
            ),
            Some(&temp_test_file),
        )
        .unwrap();
    let mut file = File::open(temp_test_file).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, env::var("EXPECT_CONTENT").unwrap());

    // Upload
    handler
        .put(
            &temp_test_file,
            &format!("/{}/{}", env::var("BUCKET_NAME").unwrap(), &new_object),
        )
        .unwrap();

    // Delete
    handler
        .del(&format!(
            "/{}/{}",
            env::var("BUCKET_NAME").unwrap(),
            &new_object
        ))
        .unwrap();
}
