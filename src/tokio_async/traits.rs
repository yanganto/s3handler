use async_trait::async_trait;
use bytes::Bytes;
use std::fmt::Debug;
use url::Url;

use super::primitives::{Canal, PoolType};
use crate::error::Error;
use crate::utils::S3Object;

#[async_trait]
pub trait S3Folder: Debug {
    async fn next_object(&mut self) -> Result<Option<S3Object>, Error>;
}

#[async_trait]
pub trait DataPool: Send + Sync + Debug {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error>;
    async fn pull(&self, desc: S3Object) -> Result<Bytes, Error>;
    /// The index will be treated as a folder object to filter the list results
    async fn list(&self, index: Option<S3Object>) -> Result<Box<dyn S3Folder>, Error>;
    async fn remove(&self, desc: S3Object) -> Result<(), Error>;
    /// TODO: sync feature
    /// This method is for the sync feature
    async fn fetch_meta(&self, _desc: &mut S3Object) -> Result<(), Error> {
        unimplemented!()
    }
    fn check_scheme(&self, _scheme: &str) -> Result<(), Error> {
        Err(Error::SchemeError())
    }
    fn as_base_from(self, resource_location: &str) -> Result<Canal, Error>
    where
        Self: Sized + 'static,
    {
        match Url::parse(resource_location) {
            Ok(r) if self.check_scheme(r.scheme()).is_err() => Err(Error::SchemeError()),
            _ => Ok(Canal {
                up_pool: Some(Box::new(self)),
                down_pool: None,
                upstream_object: None,
                downstream_object: Some(resource_location.into()),
                default: PoolType::DownPool,
            }),
        }
    }
    fn as_target_to(self, resource_location: &str) -> Result<Canal, Error>
    where
        Self: Sized + 'static,
    {
        match Url::parse(resource_location) {
            Ok(r) if self.check_scheme(r.scheme()).is_err() => Err(Error::SchemeError()),
            _ => Ok(Canal {
                up_pool: Some(Box::new(self)),
                down_pool: None,
                upstream_object: Some(resource_location.into()),
                downstream_object: None,
                default: PoolType::UpPool,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tokio_async::primitives::FilePool;
    use crate::tokio_async::traits::DataPool;

    #[test]
    fn test_canal_connect() {
        let resource = FilePool::default();
        let folder = resource.as_base_from("/path/to/a/folder").unwrap();
        assert!(!folder.is_connect());
        let canal = folder.toward("/path/to/another/folder").unwrap();
        assert!(canal.is_connect());
    }
}
