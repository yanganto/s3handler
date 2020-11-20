use std::fmt;

use super::file::FilePool;
use crate::error::Error;
use crate::tokio_async::traits::DataPool;
use crate::utils::S3Object;
use url::Url;

#[derive(Debug)]
pub enum PoolType {
    UpPool,
    DownPool,
}

#[derive(Debug)]
pub struct Canal {
    pub up_pool: Option<Box<dyn DataPool>>,
    pub down_pool: Option<Box<dyn DataPool>>,
    pub upstream_object: Option<S3Object>,
    pub downstream_object: Option<S3Object>,
    pub(crate) default: PoolType,
    // TODO: feature: data transformer
    // it may do encrypt, or format transformation here
    // upstream_obj_lambda:
    // downstream_obj_lambda:

    // TODO: folder/bucket upload feature:
    // index & key of S3Object transformer
    // upstream_obj_desc_lambda:
    // downstream_obj_desc_lambda:
}
impl fmt::Debug for dyn DataPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Dynamic DataPool").finish()
    }
}

impl Canal {
    pub fn is_connect(&self) -> bool {
        self.up_pool.is_some() && self.down_pool.is_some()
    }

    // Begin of short cut api to file pool
    pub fn toward(mut self, resource_location: &str) -> Result<Self, Error> {
        let fp = FilePool::default();
        match Url::parse(resource_location) {
            Ok(r) if fp.check_scheme(r.scheme()).is_err() => Err(Error::SchemeError()),
            _ => {
                self.toward_pool(Box::new(fp));
                self.upstream_object = Some(resource_location.to_string().into());
                Ok(self)
            }
        }
    }
    pub fn from(mut self, resource_location: &str) -> Result<Self, Error> {
        let fp = FilePool::default();
        match Url::parse(resource_location) {
            Ok(r) if fp.check_scheme(r.scheme()).is_err() => Err(Error::SchemeError()),
            _ => {
                self.from_pool(Box::new(fp));
                self.downstream_object = Some(resource_location.to_string().into());
                Ok(self)
            }
        }
    }

    pub async fn download_file(mut self, resource_location: &str) -> Result<(), Error> {
        let fp = FilePool::default();
        match Url::parse(resource_location) {
            Ok(r) if fp.check_scheme(r.scheme()).is_err() => Err(Error::SchemeError()),
            _ => {
                self.toward_pool(Box::new(fp));
                self.downstream_object = Some(resource_location.to_string().into());
                Ok(self.pull().await?)
            }
        }
    }

    pub fn upload_file(mut self, resource_location: &str) -> Result<(), Error> {
        unimplemented!()
    }
    // End of short cut api to file pool

    // Begin of setting api
    pub fn from_pool(&mut self, pool: Box<dyn DataPool>) {
        self.up_pool = Some(pool);
    }
    pub fn toward_pool(&mut self, pool: Box<dyn DataPool>) {
        self.down_pool = Some(pool);
    }

    #[inline]
    pub fn _object(mut self, object_name: &str) -> Self {
        let mut o = match self.default {
            PoolType::UpPool => self.upstream_object.take(),
            PoolType::DownPool => self.downstream_object.take(),
        }
        .unwrap_or_default();
        o.key = Some(object_name.to_string());
        match self.default {
            PoolType::UpPool => self.upstream_object = Some(o),
            PoolType::DownPool => self.downstream_object = Some(o),
        };
        self
    }

    #[inline]
    pub fn _bucket(mut self, bucket_name: &str) -> Self {
        let mut o = match self.default {
            PoolType::UpPool => self.upstream_object.take(),
            PoolType::DownPool => self.downstream_object.take(),
        }
        .unwrap_or_default();
        o.bucket = Some(bucket_name.to_string());
        match self.default {
            PoolType::UpPool => self.upstream_object = Some(o),
            PoolType::DownPool => self.downstream_object = Some(o),
        };
        self
    }

    pub fn object(self, object_name: &str) -> Self {
        self._object(object_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn key(self, key_name: &str) -> Self {
        self._object(key_name)
    }

    pub fn bucket(self, bucket_name: &str) -> Self {
        self._bucket(bucket_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn folder(self, folder_name: &str) -> Self {
        self._bucket(folder_name)
    }

    #[inline]
    pub fn _toward_object(&mut self, object_name: &str) {
        let mut o = self.downstream_object.take().unwrap_or_default();
        o.key = Some(object_name.to_string());
        self.downstream_object = Some(o);
    }

    pub fn toward_object(&mut self, object_name: &str) {
        self._toward_object(object_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn toward_key(&mut self, object_name: &str) {
        self._toward_object(object_name)
    }

    #[inline]
    pub fn _toward_bucket(&mut self, bucket_name: &str) {
        let mut o = self.downstream_object.take().unwrap_or_default();
        o.bucket = Some(bucket_name.to_string());
        self.downstream_object = Some(o);
    }

    pub fn toward_bucket(&mut self, bucket_name: &str) {
        self._toward_bucket(bucket_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn toward_folder(&mut self, folder_name: &str) {
        self._toward_bucket(folder_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn toward_path(&mut self, path: &str) {
        self.downstream_object = Some(path.to_string().into());
    }

    #[inline]
    pub fn _from_object(&mut self, object_name: &str) {
        let mut o = self.upstream_object.take().unwrap_or_default();
        o.key = Some(object_name.to_string());
        self.upstream_object = Some(o);
    }

    pub fn from_object(&mut self, object_name: &str) {
        self._from_object(object_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn from_key(&mut self, object_name: &str) {
        self._from_object(object_name)
    }

    #[inline]
    pub fn _from_bucket(&mut self, bucket_name: &str) {
        let mut o = self.upstream_object.take().unwrap_or_default();
        o.bucket = Some(bucket_name.to_string());
        self.upstream_object = Some(o);
    }

    pub fn from_bucket(&mut self, bucket_name: &str) {
        self._from_bucket(bucket_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn from_folder(&mut self, folder_name: &str) {
        self._from_bucket(folder_name)
    }

    #[cfg(not(feature = "slim"))]
    pub fn from_path(&mut self, path: &str) {
        self.upstream_object = Some(path.to_string().into());
    }
    // End of setting api

    // Begin of IO api
    // pub async fn push(self)
    pub async fn pull(self) -> Result<(), Error> {
        match (self.up_pool, self.down_pool) {
            (Some(up_pool), Some(down_pool)) => {
                let b = up_pool
                    .pull(self.upstream_object.expect("should be upstream object"))
                    .await?;
                down_pool
                    .push(
                        self.downstream_object.expect("should be downstream object"),
                        b,
                    )
                    .await?;
                Ok(())
            }
            _ => Err(Error::PoolUninitializeError()),
        }
    }
    //
    // pub async fn upstream_list(self)
    // pub async fn downstream_list(self)
    // pub async fn list(self)
    //
    // pub async fn upstream_remove(self)
    // pub async fn downstream_remove(self)
    // pub async fn remove(self)
    //
    // pub async fn sync(self)
    //
    // End of IO api
}
