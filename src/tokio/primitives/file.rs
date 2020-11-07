use crate::error::Error;
use crate::tokio::traits::DataPool;
use crate::utils::S3Object;

#[derive(Clone)]
pub struct FilePool {}

impl DataPool for FilePool {
    fn push(&self, desc: S3Object, object: Vec<u8>) -> Result<(), Error> {
        unimplemented!();
    }
    fn pull(&self, desc: S3Object) -> Result<Vec<u8>, Error> {
        unimplemented!();
    }
    fn list(&self, index: Option<S3Object>) -> Result<Vec<S3Object>, Error> {
        unimplemented!();
    }
    fn remove(&self, desc: S3Object) -> Result<(), Error> {
        unimplemented!();
    }
    fn check_scheme(&self, scheme: &str) -> Result<(), Error> {
        if scheme.to_lowercase() != "file" {
            Err(Error::SchemeError())
        } else {
            Ok(())
        }
    }
}
