//! Initilize S3 handler to manipulate objects and buckets
//! use s3handler = { features = ["blocking"] }
//! ```
//! let config = s3handler::CredentialConfig{
//!     host: "s3.us-east-1.amazonaws.com".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     user: None,
//!     region: None, // default is us-east-1
//!     s3_type: None, // default will try to config as AWS S3 handler
//!     secure: None, // dafault is false, because the integrity protect by HMAC
//! };
//! let mut handler = s3handler::Handler::from(&config);
//! let _ = handler.la();
//! ```
//!
//! Download a file with async api
//! ```
//! let s3_pool = s3handler::tokio::primitives::S3Pool {
//!     host: "somewhere.in.the.world".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     ..Default::default()
//! };
//! let obj = s3_pool.bucket("bucket_name").object("objcet_name");
//! // obj.to_file("/path/to/save/a/file").await;
//! ```
//!
//! S3 async handler to manipulate objects and buckets.
//! This treat all data as pool and create a canal to bridge two pool.
//! It is easy to management and sync data from folder to S3, S3 to S3, event folder to folder.
//!
//! """
//!        +------+
//!        | Pool | (UpPool)  modify by `from_*` api
//!        +------+
//!          |  ^
//!     Pull |  | Push
//!          v  |
//!        +------+
//!        | Pool | (DownPool) modify by `toward_*` api
//!        +------+
//! """
//!
//! ```
//! use s3handler::tokio::traits::DataPool;
//!
//! let s3_pool = s3handler::tokio::primitives::S3Pool {
//!     host: "somewhere.in.the.world".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     ..Default::default()
//! };
//! let bucket = s3_pool.bucket("bucket_name");
//! // Actually the bucket is a unconnnected canal
//! assert!(!bucket.is_connect());
//! let canal = bucket.toward("/path/to/another/folder").unwrap();
//! // The canal bridges the two folder and ready to transfer data between bucket and folder
//! assert!(canal.is_connect());
//! // canal.sync().awit;
//! ```

//  use s3handler = { features = ["tokio"] }
//  or
//  use s3handler = { features = ["async-std"] }
//  ```
//  let s3_pool = s3handler::DataPool {
//      host: "s3.us-east-1.amazonaws.com".to_string(),
//      access_identifier: "access key".to_string(),
//      secret: "secret key".to_string(),
//      region: None,  // default is Some("us-east-1")
//      data_type: s3handler::DataType:AWS4,
//      secure: None,  // dafault is false, because the integrity protect by HMAC
//  };

//  let mut bucket = s3_pool.from("s3://bucket_name").unwarp();

//  for obj in bucket.list().await.unwrap().into_iter() {
//      obj.down().await;
//  };

//  let mut up_pool = s3handler::DataPool {
//     host: "s3.us-east-1.amazonaws.com".to_string(),
//     access_identifier: "access key".to_string(),
//     secret: "secret key".to_string(),
//     data_type: s3handler::DataType:AWS4,
//     ..Default::default()
//  };

//  let mut down_pool: s3handler::DataPool {
//     host: "/current/path/".to_string(),
//     access_key: "akey".to_string(),
//     secret_key: "skey".to_string(),
//     region: None,  // default is us-east-1
//     data_type: s3handler::DataType:File,
//     secure: None,  // dafault is false, because the integrity protect by HMAC
//  };

//  let mut bucket = s3handler::Canal {
//      up_pool: &mut up_pool,
//      down_pool &mut down_pool,
//      upstream_index: Some("bucket_name"),
//      upstream_key: None,
//      downstream_index: Some("/current/folder/"),
//      upstream_key: None,
//      view_point: s3handler::ViewPoint::Upstream, // Upstream, Downstream
//  };

//  // upload objects
//  for obj in bucket.upstream_list().await.unwrap().into_iter() {
//      obj.up().await;
//  };

//  // `list_after` is a short cut for `upstream_list_after`, if view_point: s3handler::ViewPoint::Upstream,
//  for obj in bucket.list_after("index").unwarp().await.into_iter(){
//      obj.down().await;
//  };

//  // `from` will put the pool to the up_pool of a Canal
//  // `to` will put the pool to the down_pool of a Canal
//  let mut bucket = s3_pool.from("s3://bucket_name/").unwarp();
//  let mut object = s3_pool.from("s3://bucket_name/file").unwarp();
//  let mut object = s3_pool.toward("/a/absolute/path/to/file").unwarp();
//  let mut object = s3_pool.toward("./a/relations/path/to/file").unwarp();
//  let mut folder = s3_pool.toward("/a/absolute/path/to/folder/").unwarp();
//  let mut folder = s3_pool.toward("./a/relations/path/to/folder/").unwarp();
//  let error = s3_pool.to("file");
//  let error = s3_pool.from("file/");

//  // `list_from` is a short cut for `downstream_list`, if view_point: s3handler::ViewPoint::Downstream,
//  for obj in folder.set_upstream(s3://bucket_name).list().await.unwarp().into_iter() {
//      obj.up().await;
//  };

//  // `object` will base on the view point to call `from_object` or `toward_object`
//  // the `file`, `key` method do the same thing
//  let mut object = bucket.object("/a/absolute/path/to/file");
//  // `sync` method will check the timestamp of the object than call `up` or `down`
//  let result = object.sync().await;

//  // flowing code present to upload a local file to s3 object /bucket_name/new_file
//  let bucket = s3_pool.from("s3://bucket_name/").unwarp();
//  let mut object = bucket.toward_object("/a/path/to/file").key("new_file");
//  let result = object.up().await;

//  // These methods of Canal will consume and return a new Canal
//  // `object()`, `bucket()`, `folder()`, `key()`
//  let object = s3_pool.from("s3://bucket_name/object_name").unwarp();
//  let new_object = object.bucket("another_bucket");
//  // now `new_object` equal to `s3_pool.from("s3://bucket_name/object_name").unwarp()`
//
//  // These methods of Canal will modify Canal
//  // `toward_object()`, `toward_bucket()`, `toward_folder()`, `toward_key()`
//  // `from_object()`, `from_bucket()`, `from_folder()`, `from_key()`
//  // `toward`, `from`
//
//  let mut object = s3_pool.from("s3://bucket_name/object_name").unwarp();
//  object.from_bucket("another_bucket");
//  // now `object` equal to `s3_pool.from("s3://bucket_name/object_name").unwarp()`

//  ```
#[cfg(feature = "blocking")]
pub use blocking::{CredentialConfig, Handler};
#[cfg(feature = "blocking")]
mod blocking;

#[cfg(feature = "tokio")]
pub mod tokio;

#[cfg(feature = "async-std")]
mod async_std;

pub mod error;

pub use utils::{S3Convert, S3Object};
pub mod utils;
