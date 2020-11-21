#[tokio::test]
async fn test_v2_async_operation() {
    use s3handler::none_blocking::primitives::S3Pool;
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;
    use std::time::SystemTime;

    match env::var("ACCESS_KEY") {
        Ok(akey) => {
            println!("use access key: {} for testing", akey);

            // TODO: use tmpfile crate to test on different OS
            let temp_test_file = "/tmp/test";
            let new_object = format!(
                "{}-{}",
                env::var("OBJECT_NAME").unwrap(),
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );

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
        Err(_) => (),
    }
}
