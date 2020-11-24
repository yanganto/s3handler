#[tokio::test]
async fn test_v4_async_operation() {
    use s3handler::none_blocking::primitives::S3Pool;
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;
    use std::time::SystemTime;

    match env::var("ACCESS_KEY") {
        Ok(akey) => {
            println!("use access key: {} for async testing", akey);

            // TODO: use tmpfile crate to test on different OS
            let temp_test_file = "/tmp/v4-async-test";
            let new_object = format!(
                "{}-v4_async-{}",
                env::var("OBJECT_NAME").unwrap(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );

            // Download
            let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap()).aws_v4(
                akey.to_string(),
                env::var("SECRET_KEY").unwrap(),
                env::var("REGION").unwrap(),
            );
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
                .aws_v4(
                    akey.to_string(),
                    env::var("SECRET_KEY").unwrap(),
                    env::var("REGION").unwrap(),
                )
                .bucket(&env::var("BUCKET_NAME").unwrap())
                .object(&new_object);
            obj.upload_file(temp_test_file).await.unwrap();

            // Delete
            let obj = S3Pool::new(env::var("S3_HOST").unwrap())
                .aws_v4(
                    akey.to_string(),
                    env::var("SECRET_KEY").unwrap(),
                    env::var("REGION").unwrap(),
                )
                .bucket(&env::var("BUCKET_NAME").unwrap())
                .object(&new_object);
            obj.remove().await.unwrap();
        }
        Err(_) => (),
    }
}

#[test]
fn test_v4_sync_operation() {
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;
    use std::time::SystemTime;

    match env::var("ACCESS_KEY") {
        Ok(akey) => {
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
                region: env::var("REGION").ok(),
                s3_type: None,
                secure: None,
            };
            let mut handler = s3handler::blocking::Handler::from(&config);
            handler.change_auth_type("aws4");
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
        Err(_) => (),
    }
}
