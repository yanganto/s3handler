//! Initilize S3 handler to manipulate objects and buckets
//! use s3handler = { features = ["blocking"] }
//! ```
//! let config = s3handler::blocking::CredentialConfig{
//!     host: "s3.us-east-1.amazonaws.com".to_string(),
//!     access_key: "akey".to_string(),
//!     secret_key: "skey".to_string(),
//!     user: None,
//!     region: None, // default is us-east-1
//!     s3_type: None, // default will try to config as AWS S3 handler
//!     secure: None, // dafault is false, because the integrity protect by HMAC
//! };
//! let mut handler = s3handler::blocking::Handler::from(&config);
//! let _ = handler.la();
//! ```
//!
//! Download a file with async api
//! use s3handler = { features = ["tokio-async"] }
//! ```
//! // Public resource
//! let s3_pool = s3handler::none_blocking::primitives::S3Pool::new("somewhere.in.the.world".to_string());
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
//! use s3handler::none_blocking::traits::DataPool;
//!
//! // Resource with AWS version 2 auth
//! let s3_pool = s3handler::none_blocking::primitives::S3Pool::new("somewhere.in.the.world".to_string())
//!         .aws_v2("access-key".to_string(), "secrete-key".to_string());
//! let bucket = s3_pool.bucket("bucket_name");
//! // Actually the bucket is a unconnnected canal
//! assert!(!bucket.is_connect());
//! let canal = bucket.toward("/path/to/another/folder").unwrap();
//! // The canal bridges the two folder and ready to transfer data between bucket and folder
//! assert!(canal.is_connect());
//! // canal.sync().awit;
//! ```

#[cfg(feature = "blocking")]
pub mod blocking;
#[cfg(feature = "blocking")]
pub use blocking::*;

// #[cfg(feature = "std-async")]
// pub mod async_std;

#[cfg(feature = "tokio-async")]
pub mod tokio_async;
#[cfg(feature = "tokio-async")]
pub use tokio_async as none_blocking;

pub mod error;
pub use utils::{S3Convert, S3Object};
pub mod utils;
