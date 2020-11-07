use url::Url;

use super::primitives::{Canal, PoolType};
use crate::error::Error;
use crate::utils::S3Object;

pub trait DataPool {
    fn push(&self, desc: S3Object, object: Vec<u8>) -> Result<(), Error>;
    fn pull(&self, desc: S3Object) -> Result<Vec<u8>, Error>;
    fn list(&self, index: Option<S3Object>) -> Result<Vec<S3Object>, Error>;
    fn remove(&self, desc: S3Object) -> Result<(), Error>;
    /// TODO: sync feature
    /// This method is for the sync feature
    fn fetch_meta(&self, desc: &mut S3Object) {
        unimplemented!()
    }
    fn check_scheme(&self, scheme: &str) -> Result<(), Error> {
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
                downstream_object: Some(resource_location.to_string().into()),
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
                upstream_object: Some(resource_location.to_string().into()),
                downstream_object: None,
                default: PoolType::UpPool,
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_canal_connect() {
        let resource = FilePool {};
        let folder = resource.as_base_from("/path/to/a/folder").unwrap();
        assert!(!folder.is_connect());
        let canal = folder.toward("/path/to/another/folder").unwrap();
        assert!(canal.is_connect());
    }
}
