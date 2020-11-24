use std::{env, path::Path};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::fs::{create_dir, read, read_dir, remove_dir_all, remove_file, write, ReadDir};
use url::Url;

use crate::error::Error;
use crate::tokio_async::traits::{DataPool, S3Folder};
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

#[derive(Clone)]
pub struct FilePool {
    /// use "/" for *nix, "C://" for windows (not tested)
    pub drive: String,
}
impl Default for FilePool {
    fn default() -> Self {
        Self {
            drive: env::current_dir()
                .expect("current dir is undetected")
                .to_str()
                .expect("current dir is not valid string")
                .into(),
        }
    }
}

impl FilePool {
    pub fn new(path: &str) -> Result<Self, Error> {
        let mut fp = FilePool::default();
        if path.starts_with("/") {
            fp.drive = "/".to_string();
        } else {
            match Url::parse(path) {
                Ok(r) => {
                    if ["s3", "S3"].contains(&r.scheme()) {
                        return Err(Error::SchemeError());
                    }
                }
                _ => {}
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
                write(Path::new(&format!("{}{}{}", self.drive, b, k)), object).await
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
            return match read(Path::new(&format!("{}{}{}", self.drive, b, k))).await {
                // TODO: figure ouput how to use Bytes in tokio
                Ok(c) => Ok(Bytes::copy_from_slice(&c)),
                Err(e) => Err(e.into()),
            };
        }
        Err(Error::PullEmptyObjectError())
    }

    async fn list(&self, index: Option<S3Object>) -> Result<Box<dyn S3Folder>, Error> {
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
                read_dir(Path::new(&format!("{}", self.drive))).await?,
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
            r.or_else(|e| Err(e.into()))
        } else {
            Err(Error::ModifyEmptyBucketError())
        }
    }

    fn check_scheme(&self, _scheme: &str) -> Result<(), Error> {
        panic!("file pool use new to create a valid, without this function")
    }
}
