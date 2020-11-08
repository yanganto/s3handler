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
