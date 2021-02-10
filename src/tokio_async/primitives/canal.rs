use super::file::FilePool;
use crate::error::Error;
use crate::tokio_async::traits::{DataPool, S3Folder};
use crate::utils::S3Object;

#[derive(Debug)]
pub enum PoolType {
    UpPool,
    DownPool,
}

#[derive(Debug)]
pub struct Canal {
    pub up_pool: Option<Box<dyn DataPool>>,
    pub upstream_object: Option<S3Object>,
    pub down_pool: Option<Box<dyn DataPool>>,
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

/// A canal presets a object link for two object from resource pool to pool.
/// If everything is set, the async api can pull/push the objects.
///
/// The `download_file`, and `upload_file` api will setup the up pool as s3 pool,
/// and set up the down pool as file pool for the most usage case.
///
/// The terms `object`, `key`, `bucket`, `folder`, `path` may be easiler for readness in coding,
/// so there are several similar methods help you to setup things.
/// If you down want these duplicate functions, you can enable the `slim` feature.
impl Canal {
    /// Check the two pools are set or not
    pub fn is_connect(&self) -> bool {
        self.up_pool.is_some() && self.down_pool.is_some()
    }

    /// Set downd pool as file pool, and toward to the `resource_location`
    pub fn toward(mut self, resource_location: &str) -> Result<Self, Error> {
        self.toward_pool(Box::new(FilePool::new(resource_location)?));
        self.upstream_object = Some(resource_location.into());
        Ok(self)
    }

    /// Set up pool as file pool, and from to the `resource_location`
    pub fn from(mut self, resource_location: &str) -> Result<Self, Error> {
        self.from_pool(Box::new(FilePool::new(resource_location)?));
        self.downstream_object = Some(resource_location.into());
        Ok(self)
    }

    /// Download object from s3 pool to file pool
    /// This function set file pool as down pool and s3 pool as up pool
    /// then toward to the `resource_location`,
    /// pull the object from uppool into down pool.
    pub async fn download_file(mut self, resource_location: &str) -> Result<(), Error> {
        self.toward_pool(Box::new(FilePool::new(resource_location)?));
        self.downstream_object = Some(resource_location.into());
        match self.downstream_object.take() {
            Some(S3Object { bucket, key, .. }) if key.is_none() => {
                self.downstream_object = Some(S3Object {
                    bucket,
                    key: self.upstream_object.clone().unwrap().key,
                    ..Default::default()
                });
            }
            Some(obj) => {
                self.downstream_object = Some(obj);
            }
            None => {
                panic!("never be here")
            }
        }
        Ok(self.pull().await?)
    }

    /// Upload object from file pool to s3 pool
    /// This function set file pool as down pool and s3 pool as up pool
    /// then toward to the `resource_location`,
    /// push the object from uppool into down pool.
    pub async fn upload_file(mut self, resource_location: &str) -> Result<(), Error> {
        self.toward_pool(Box::new(FilePool::new(resource_location)?));
        self.downstream_object = Some(resource_location.into());
        match self.downstream_object.take() {
            Some(S3Object { bucket, key, .. }) if key.is_none() => {
                self.downstream_object = Some(S3Object {
                    bucket: Some(std::env::current_dir()?.to_string_lossy()[1..].into()),
                    key: Some(format!("/{}", bucket.unwrap_or_default())),
                    ..Default::default()
                });
            }
            Some(obj) => {
                self.downstream_object = Some(obj);
            }
            None => {
                panic!("never be here")
            }
        }
        Ok(self.push().await?)
    }
    // End of short cut api to file pool

    // Begin of setting api
    /// Setup the up pool
    pub fn from_pool(&mut self, pool: Box<dyn DataPool>) {
        self.up_pool = Some(pool);
    }

    /// Setup the down pool
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
        o.key = if object_name.starts_with('/') {
            Some(object_name.to_string())
        } else {
            Some(format!("/{}", object_name))
        };
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

    /// Setup the object for the first pool connected by canal,
    /// This api can be used without fully setting up two pools,
    /// and just set up the object as you what you think.
    pub fn object(self, object_name: &str) -> Self {
        self._object(object_name)
    }

    /// The same as `object()`
    #[cfg(not(feature = "slim"))]
    pub fn key(self, key_name: &str) -> Self {
        self._object(key_name)
    }

    /// Setup the bucket for the first pool connected by canal,
    /// This api can be used without fully setting up two pools,
    /// and just set up the object as you what you think.
    pub fn bucket(self, bucket_name: &str) -> Self {
        self._bucket(bucket_name)
    }

    #[cfg(not(feature = "slim"))]
    /// The same as `bucket()`
    pub fn folder(self, folder_name: &str) -> Self {
        self._bucket(folder_name)
    }

    #[inline]
    pub fn _toward_object(&mut self, object_name: &str) {
        let mut o = self.downstream_object.take().unwrap_or_default();
        o.key = if object_name.starts_with('/') {
            Some(object_name.to_string())
        } else {
            Some(format!("/{}", object_name))
        };
        self.downstream_object = Some(o);
    }

    /// Setup the object in the down pool
    pub fn toward_object(&mut self, object_name: &str) {
        self._toward_object(object_name)
    }

    /// The same as `toward_object()`
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

    /// Setup the bucket in the down pool
    pub fn toward_bucket(&mut self, bucket_name: &str) {
        self._toward_bucket(bucket_name)
    }

    /// The same as `toward_bucket()`
    #[cfg(not(feature = "slim"))]
    pub fn toward_folder(&mut self, folder_name: &str) {
        self._toward_bucket(folder_name)
    }

    /// Setup the path in the down pool
    #[cfg(not(feature = "slim"))]
    pub fn toward_path(&mut self, path: &str) {
        self.downstream_object = Some(path.into());
    }

    #[inline]
    pub fn _from_object(&mut self, object_name: &str) {
        let mut o = self.upstream_object.take().unwrap_or_default();
        o.key = if object_name.starts_with('/') {
            Some(object_name.to_string())
        } else {
            Some(format!("/{}", object_name))
        };
        self.upstream_object = Some(o);
    }

    /// Setup the object in the up pool
    pub fn from_object(&mut self, object_name: &str) {
        self._from_object(object_name)
    }

    /// The same as `from_object()`
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

    /// Setup the bucket in the up pool
    pub fn from_bucket(&mut self, bucket_name: &str) {
        self._from_bucket(bucket_name)
    }

    /// The same as `from_bucket()`
    #[cfg(not(feature = "slim"))]
    pub fn from_folder(&mut self, folder_name: &str) {
        self._from_bucket(folder_name)
    }

    /// Setup the path in the up pool
    #[cfg(not(feature = "slim"))]
    pub fn from_path(&mut self, path: &str) {
        self.upstream_object = Some(path.into());
    }
    // End of setting api

    // Begin of IO api
    /// Push the object from down pool to up pool.
    /// It will raise error if the canal is not will setup.
    pub async fn push(self) -> Result<(), Error> {
        match (self.up_pool, self.down_pool) {
            (Some(up_pool), Some(down_pool)) => {
                let b = down_pool
                    .pull(self.downstream_object.expect("should be upstream object"))
                    .await?;
                // TODO: make a default for target if unset
                up_pool
                    .push(
                        self.upstream_object.expect("should be downstream object"),
                        b,
                    )
                    .await?;
                Ok(())
            }
            _ => Err(Error::PoolUninitializeError()),
        }
    }

    /// Pull the object from up pool to down pool.
    /// It will raise error if the canal is not will setup.
    pub async fn pull(self) -> Result<(), Error> {
        match (self.up_pool, self.down_pool) {
            (Some(up_pool), Some(down_pool)) => {
                let b = up_pool
                    .pull(self.upstream_object.expect("should be upstream object"))
                    .await?;
                // TODO: make a default for target if unset
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

    /// Remove the object in the up pool.
    pub async fn upstream_remove(self) -> Result<(), Error> {
        if let Some(upstream_object) = self.upstream_object {
            Ok(self
                .up_pool
                .expect("upstream pool should exist") // TODO customize Error
                .remove(upstream_object)
                .await?)
        } else {
            return Err(Error::ResourceUrlError(
                "can not remove on an object withouput setup".to_string(),
            ));
        }
    }

    /// Remove the object in the down pool.
    pub async fn downstream_remove(self) -> Result<(), Error> {
        if let Some(downstream_object) = self.downstream_object {
            Ok(self
                .down_pool
                .expect("downstream pool should exist") // TODO customize Error
                .remove(downstream_object)
                .await?)
        } else {
            return Err(Error::ResourceUrlError(
                "can not remove on an object withouput setup".to_string(),
            ));
        }
    }

    /// Remove the object depence on the first pool connected by the canal
    /// This api can be used without fully setting up two pools,
    /// and remove object as you what you think.
    pub async fn remove(self) -> Result<(), Error> {
        match self.default {
            PoolType::UpPool => self.upstream_remove().await,
            PoolType::DownPool => self.downstream_remove().await,
        }
    }

    /// List the objects in the up pool.
    pub async fn upstream_list(self) -> Result<Box<dyn S3Folder>, Error> {
        Ok(self
            .up_pool
            .expect("upstream pool should exist")
            .list(self.upstream_object)
            .await?)
    }

    /// List the objects in the down pool.
    pub async fn downstream_list(self) -> Result<Box<dyn S3Folder>, Error> {
        Ok(self
            .down_pool
            .expect("downstream pool should exist")
            .list(self.downstream_object)
            .await?)
    }

    /// List the objects depence on the first pool connected by the canal
    /// This api can be used without fully setting up two pools,
    /// and list objects as you what you think.
    pub async fn list(self) -> Result<Box<dyn S3Folder>, Error> {
        match self.default {
            PoolType::UpPool => self.upstream_list().await,
            PoolType::DownPool => self.downstream_list().await,
        }
    }

    // pub async fn sync(self)
    // End of IO api
}
