#[tokio::test]
async fn test_v2_download() {
    use s3handler::none_blocking::primitives::S3Pool;
    use std::env;
    use std::fs::File;
    use std::io::prelude::*;

    match env::var("ACCESS_KEY") {
        Ok(akey) => {
            println!("use access key: {} for testing", akey);
            let s3_pool = S3Pool::new(env::var("S3_HOST").unwrap())
                .aws_v2(akey.to_string(), env::var("SECRET_KEY").unwrap());
            let obj = s3_pool
                .bucket(&env::var("BUCKET_NAME").unwrap())
                .object(&env::var("OBJECT_NAME").unwrap());

            // TODO: use tmpfile crate to test on different OS
            obj.download_file("/tmp/test").await.unwrap();
            let mut file = File::open("/tmp/test").unwrap();
            let mut contents = String::new();
            file.read_to_string(&mut contents).unwrap();
            assert_eq!(contents, env::var("EXPECT_CONTENT").unwrap());
        }
        Err(_) => (),
    }
}
