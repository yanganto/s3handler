use std::path::Path;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::{create_dir, read, read_dir, remove_dir_all, remove_file, write, ReadDir};
use url::Url;

use crate::error::Error;
use crate::tokio_async::traits::{DataPool, Filter, S3Folder};
use crate::utils::S3Object;

#[async_trait]
impl S3Folder for ReadDir {
    async fn next_object(&mut self) -> Result<Option<S3Object>, Error> {
        Ok(self.next_entry().await?.map(|e| S3Object {
            key: e.path().to_str().map(|s| s.to_string()),
            ..Default::default()
        }))
    }
}

#[derive(Clone, Debug)]
pub struct FilePool {
    /// use "/" for *nix, "C://" for windows (not tested)
    pub drive: String,
}
impl Default for FilePool {
    fn default() -> Self {
        Self { drive: "/".into() }
    }
}

impl FilePool {
    pub fn new(path: &str) -> Result<Self, Error> {
        let mut fp = FilePool::default();
        if path.starts_with('/') {
            fp.drive = path.to_string();
        } else if let Ok(r) = Url::parse(path) {
            if ["s3", "S3"].contains(&r.scheme()) {
                return Err(Error::SchemeError());
            }
        }
        Ok(fp)
    }
}

unsafe impl Send for FilePool {}
unsafe impl Sync for FilePool {}

#[async_trait]
impl DataPool for FilePool {
    async fn push(&self, desc: S3Object, object: Bytes) -> Result<(), Error> {
        if let Some(b) = desc.bucket {
            let r = if let Some(k) = desc.key {
                let path = if k.starts_with("/") {
                    format!("{}{}{}", self.drive, b, k)
                } else {
                    format!("{}/{}{}", self.drive, b, k)
                };
                write(Path::new(&path), object).await
            } else {
                create_dir(Path::new(&b)).await
            };
            r.map_err(|e| e.into())
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
            let path = if k.starts_with("/") {
                format!("{}{}{}", self.drive, b, k)
            } else {
                format!("{}/{}{}", self.drive, b, k)
            };
            return match read(Path::new(&path)).await {
                // TODO: figure ouput how to use Bytes in tokio
                Ok(c) => Ok(Bytes::copy_from_slice(&c)),
                Err(e) => Err(e.into()),
            };
        }
        Err(Error::PullEmptyObjectError())
    }

    async fn list(
        &self,
        index: Option<S3Object>,
        filter: &Option<Filter>,
    ) -> Result<Box<dyn S3Folder>, Error> {
        if filter.is_some() {
            unimplemented!("filter for file system is not implemented")
        }
        match index {
            Some(S3Object {
                bucket: Some(b),
                key: None,
                ..
            }) => Ok(Box::new(
                read_dir(Path::new(&format!("{}{}", self.drive, b))).await?,
            )),
            Some(S3Object {
                bucket: Some(b),
                key: Some(k),
                ..
            }) => Ok(Box::new(
                read_dir(Path::new(&format!("{}{}{}", self.drive, b, k))).await?,
            )),
            Some(S3Object { bucket: None, .. }) | None => Ok(Box::new(
                read_dir(Path::new(&self.drive.to_string())).await?,
            )),
        }
    }

    async fn remove(&self, desc: S3Object) -> Result<(), Error> {
        if let Some(b) = desc.bucket {
            let r = if let Some(k) = desc.key {
                remove_file(Path::new(&format!("{}{}{}", self.drive, b, k))).await
            } else {
                remove_dir_all(Path::new(&b)).await
            };
            r.map_err(|e| e.into())
        } else {
            Err(Error::ModifyEmptyBucketError())
        }
    }

    fn check_scheme(&self, _scheme: &str) -> Result<(), Error> {
        panic!("file pool use new to create a valid, without this function")
    }
}
