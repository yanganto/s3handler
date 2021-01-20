//! End to End test
//!
//! Following envvironment is need for testing
//! ```bash
//! export ACCESS_KEY=XXXXXXXXXXXXXXXXXXXX
//! export SECRET_KEY=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
//! export REGION=ap-northeast-1
//! export S3_HOST=s3.ap-northeast-1.amazonaws.com
//! export BUCKET_NAME=xxxxxxx
//! export OBJECT_NAME=test-s3handle
//! export BIG_OBJECT_NAME=test-s3handle-big
//! export PART_SIZE=3670016
//! export EXPECT_CONTENT="This is a test file"$'\n'
//! ```

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
                "{}-v4-async-{}",
                env::var("OBJECT_NAME").unwrap(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );

            // List
            let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap()).aws_v4(
                akey.to_string(),
                env::var("SECRET_KEY").unwrap(),
                env::var("REGION").unwrap(),
            );
            let mut object_list = s3_pool
                .bucket(&env::var("BUCKET_NAME").unwrap())
                .list()
                .await
                .unwrap();
            assert!(object_list.next_object().await.unwrap().is_some());

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

            // Test multipart operation
            // TODO: use tmpfile crate to test on different OS
            let temp_test_file = "/tmp/v4-async-7M";
            let new_object = format!(
                "{}-v4-async-{}",
                env::var("BIG_OBJECT_NAME").unwrap(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );

            // TODO
            // Test multipart download
            let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap())
                .aws_v4(
                    akey.to_string(),
                    env::var("SECRET_KEY").unwrap(),
                    env::var("REGION").unwrap(),
                )
                .part_size(env::var("PART_SIZE").unwrap().parse::<u64>().unwrap());
            let obj = s3_pool
                .bucket(&env::var("BUCKET_NAME").unwrap())
                .object(&env::var("OBJECT_NAME").unwrap());
            obj.download_file(temp_test_file).await.unwrap();

            // Test multipart upload
            let obj = S3Pool::new(env::var("S3_HOST").unwrap())
                .aws_v4(
                    akey.to_string(),
                    env::var("SECRET_KEY").unwrap(),
                    env::var("REGION").unwrap(),
                )
                .part_size(env::var("PART_SIZE").unwrap().parse::<u64>().unwrap())
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
        Err(_) => {
            println!("There is no access key, skip test");
        }
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
