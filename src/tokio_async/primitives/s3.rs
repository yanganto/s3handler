use async_trait::async_trait;

use super::canal::{Canal, PoolType};
use crate::error::Error;
use crate::tokio_async::traits::DataPool;
use crate::utils::S3Object;

#[derive(Clone, Default)]
pub struct S3Pool {
    pub host: String,
    pub access_key: String,
    pub secret_key: String,
    /// The region is string not enum, such that the libiary can be easily to use for other
    /// customized S3, and the default will treat as "us-east-1"
    pub region: Option<String>,
    /// To use https or not, please note that integrity is secured by S3 protocol.
    /// If the confidentiality is not under concerned, the http is good.
    pub secure: bool,
}

#[async_trait]
impl DataPool for S3Pool {
    async fn push(&self, desc: S3Object, object: Vec<u8>) -> Result<(), Error> {
        unimplemented!()
    }
    async fn pull(&self, desc: S3Object) -> Result<Vec<u8>, Error> {
        unimplemented!()
    }
    async fn list(
        &self,
        index: Option<S3Object>,
    ) -> Result<(Vec<S3Object>, Option<S3Object>), Error> {
        unimplemented!()
    }
    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        unimplemented!()
    }
    fn check_scheme(&self, scheme: &str) -> Result<(), Error> {
        if scheme.to_lowercase() != "s3" {
            Err(Error::SchemeError())
        } else {
            Ok(())
        }
    }
}
impl S3Pool {
    pub fn bucket(self, bucket_name: &str) -> Canal {
        Canal {
            up_pool: Some(Box::new(self)),
            down_pool: None,
            upstream_object: Some(bucket_name.to_string().into()),
            downstream_object: None,
            default: PoolType::UpPool,
        }
    }
}
