use super::traits::DataPool;
use crate::error::Error;

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

impl DataPool for S3Pool {
    fn push(&self, desc: ObjectDescription, object: Vec<u8>) -> Result<(), Error> {
        unimplemented!();
    }
    fn pull(&self, desc: ObjectDescription) -> Result<Vec<u8>, Error> {
        unimplemented!();
    }
    fn list(&self, index: Option<ObjectDescription>) -> Result<Vec<ObjectDescription>, Error> {
        unimplemented!();
    }
    fn remove(&self, desc: ObjectDescription) -> Result<(), Error> {
        unimplemented!();
    }
    fn as_base_from(&self, resource_location: &str) -> Result<Canal, Error> {
        if resource_location.to_lowercase().starts_with("s3://") {
            Ok(self._as_base_from(ObjectDescription::default()))
        } else {
            Err(Error::DataPoolLocationError(
                "S3",
                resource_location.to_string(),
            ))
        }
    }
    fn as_target_to(
        &self,
        resource_location: &str,
        upstream_object: ObjectDescription,
    ) -> Result<Canal, Error> {
        if resource_location.to_lowercase().starts_with("s3://") {
            Ok(self._as_target_to(ObjectDescription::default()))
        } else {
            Err(Error::DataPoolLocationError(
                "S3",
                resource_location.to_string(),
            ))
        }
    }
}

#[derive(Clone)]
pub struct FilePool {}

impl DataPool for FilePool {
    fn push(&self, desc: ObjectDescription, object: Vec<u8>) -> Result<(), Error> {
        unimplemented!();
    }
    fn pull(&self, desc: ObjectDescription) -> Result<Vec<u8>, Error> {
        unimplemented!();
    }
    fn list(&self, index: Option<ObjectDescription>) -> Result<Vec<ObjectDescription>, Error> {
        unimplemented!();
    }
    fn remove(&self, desc: ObjectDescription) -> Result<(), Error> {
        unimplemented!();
    }
    fn as_base_from(&self, resource_location: &str) -> Result<Canal, Error> {
        if resource_location.starts_with("/") {
            Ok(self._as_base_from(ObjectDescription::default()))
        } else {
            Err(Error::DataPoolLocationError(
                "file",
                resource_location.to_string(),
            ))
        }
    }
    fn as_target_to(
        &self,
        resource_location: &str,
        upstream_object: ObjectDescription,
    ) -> Result<Canal, Error> {
        if resource_location.starts_with("/") {
            Ok(self._as_target_to(ObjectDescription::default()))
        } else {
            Err(Error::DataPoolLocationError(
                "file",
                resource_location.to_string(),
            ))
        }
    }
}
pub(crate) enum PoolType {
    UpPool,
    DownPool,
}

// TODO: reuse S3Object
#[derive(Default)]
pub struct ObjectDescription {
    index: Option<String>,
    key: Option<String>,
    // TODO: sync feature:
    // meta:
}
pub struct Canal {
    pub up_pool: Option<Box<dyn DataPool>>,
    pub down_pool: Option<Box<dyn DataPool>>,
    pub upstream_object: ObjectDescription,
    pub downstream_object: ObjectDescription,
    pub(crate) default: PoolType,
    // TODO: feature: data transformer
    // it may do encrypt, or format transformation here
    // upstream_obj_lambda:
    // downstream_obj_lambda:

    // TODO: folder/bucket upload feature:
    // index & key of ObjectDescription transformer
    // upstream_obj_desc_lambda:
    // downstream_obj_desc_lambda:
}

impl Canal {
    pub fn is_connect(&self) -> bool {
        self.up_pool.is_some() && self.down_pool.is_some()
    }
    pub fn toward(mut self, resource_location: &str) -> Result<Self, ()> {
        if resource_location.starts_with("/") {
            self.toward_pool(Box::new(FilePool {}));
            // self._set_resource(PoolType::UpPool, resource_location);
            Ok(self)
        } else {
            Err(())
        }
    }
    pub fn from(mut self, resource_location: &str) -> Result<Self, ()> {
        if resource_location.starts_with("/") {
            self.from_pool(Box::new(FilePool {}));
            // self._set_resource(PoolType::DownPool, resource_location);
            Ok(self)
        } else {
            Err(())
        }
    }
    pub fn from_pool(&mut self, pool: Box<dyn DataPool>) -> Result<(), ()> {
        self.up_pool = Some(pool);
        Ok(())
    }
    pub fn toward_pool(&mut self, pool: Box<dyn DataPool>) -> Result<(), ()> {
        self.down_pool = Some(pool);
        Ok(())
    }
    // TODO: Setting Modify Part
    // bucket()
    // folder()
    // key()
    // object()
    // toward_bucket()
    // toward_folder()
    // toward_object()
    // toward_key()
    // _set_resource(self, pool_type: PoolType, resource: &str)
    //
    // TODO: IO Part
    // fn _list_by(self, pool_type: PoolType)
    // pub async fn upstream_list(self)
    // pub async fn downstream_list(self)
    // pub async fn list(self)
    // pub async fn upstream_remove(self)
    // pub async fn downstream_remove(self)
    // pub async fn remove(self)
    // pub async fn upstream_put(self)
    // pub async fn downstream_put(self)
    // pub async fn put(self)
    // pub async fn sync(self)
}
