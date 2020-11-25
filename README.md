S3handler
---
[![Build Status](https://travis-ci.com/yanganto/s3handler.svg?branch=master)](https://travis-ci.com/yanganto/s3handler)


A s3 handler library for [s3rs](https://github.com/yanganto/s3rs) and [nu-shell s3 plugin](https://github.com/nushell/nushell/tree/main/crates/nu_plugin_s3)
Here is the [document](https://docs.rs/s3handler/).


### Blocking API is ready
use s3handler = { features = ["blocking"] }

```rust
let config = s3handler::CredentialConfig{
    host: "s3.us-east-1.amazonaws.com".to_string(),
    access_key: "akey".to_string(),
    secret_key: "skey".to_string(),
    user: None,
    region: None, // default will be treated as us-east-1
    s3_type: None, // default will try to config as AWS S3 handler
    secure: None, // dafault is false, because the integrity protect by HMAC
};
let mut handler = s3handler::Handler::from(&config);
let _ = handler.la();
```

### Async API
Basic CRUD is implemented, other advance features are under developing.
use s3handler = { features = ["tokio"] }

Download a file with async api
use s3handler = { features = ["tokio-async"] }
```rust
// Public resource
let s3_pool = s3handler::none_blocking::primitives::S3Pool::new("somewhere.in.the.world".to_string());
let obj = s3_pool.bucket("bucket_name").object("objcet_name");
async {
    obj.download_file("/path/to/save/a/file").await;
};

```

S3 async handler to manipulate objects and buckets.
This treat all data as pool and create a canal to bridge two pool.
It is easy to management and sync data from folder to S3, S3 to S3, event folder to folder.

>>>
       +------+
       | Pool | (UpPool)  modify by `from_*` api
       +------+
         |  ^
    Pull |  | Push
         v  |
       +------+
       | Pool | (DownPool) modify by `toward_*` api
       +------+
>>>

```rust
use s3handler::none_blocking::traits::DataPool;

// Resource with AWS version 2 auth
let s3_pool = s3handler::none_blocking::primitives::S3Pool::new("somewhere.in.the.world".to_string())
        .aws_v2("access-key".to_string(), "secrete-key".to_string());
let bucket = s3_pool.bucket("bucket_name");
// Actually the bucket is a unconnnected canal
assert!(!bucket.is_connect());
let canal = bucket.toward("/path/to/another/folder").unwrap();
// The canal bridges the two folder and ready to transfer data between bucket and folder
assert!(canal.is_connect());
canal.sync().await;

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
let obj = object_list.next_object().await.unwrap();
```
