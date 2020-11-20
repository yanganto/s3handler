use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::{create_dir, read, remove_dir_all, remove_file, write};

use crate::error::Error;
use crate::tokio_async::traits::DataPool;
use crate::utils::S3Object;

#[derive(Clone)]
pub struct FilePool {}
unsafe impl Send for FilePool {}
unsafe impl Sync for FilePool {}

#[async_trait]
impl DataPool for FilePool {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error> {
        if let Some(b) = desc.bucket {
            let r = if let Some(k) = desc.key {
                write(Path::new(&format!("{}{}", b, k)), object).await
            } else {
                create_dir(Path::new(&b)).await
            };
            r.or_else(|e| Err(e.into()))
        } else {
            Err(Error::ModifyEmptyBucketError())
        }
    }
    async fn pull(&self, desc: S3Object) -> Result<Bytes, Error> {
        if let S3Object {
            bucket: Some(b),
            key: Some(k),
            ..
        } = desc
        {
            return match read(Path::new(&format!("{}{}", b, k))).await {
                // TODO: figure ouput how to use Bytes in tolio
                Ok(c) => Ok(Bytes::copy_from_slice(&c)),
                Err(e) => Err(e.into()),
            };
        }
        Err(Error::PullEmptyObjectError())
    }
    async fn list(
        &self,
        index: Option<S3Object>,
    ) -> Result<(Vec<S3Object>, Option<S3Object>), Error> {
        unimplemented!()
    }
    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        if let Some(b) = desc.bucket {
            let r = if let Some(k) = desc.key {
                remove_file(Path::new(&format!("{}{}", b, k))).await
            } else {
                remove_dir_all(Path::new(&b)).await
            };
            r.or_else(|e| Err(e.into()))
        } else {
            Err(Error::ModifyEmptyBucketError())
        }
    }
    fn check_scheme(&self, scheme: &str) -> Result<(), Error> {
        if scheme.to_lowercase() != "file" {
            Err(Error::SchemeError())
        } else {
            Ok(())
        }
    }
}
