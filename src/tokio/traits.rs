use super::primitives::{Canal, FilePool, ObjectDescription, PoolType};
use crate::error::Error;

pub trait DataPool {
    fn push(&self, desc: ObjectDescription, object: Vec<u8>) -> Result<(), Error>;
    fn pull(&self, desc: ObjectDescription) -> Result<Vec<u8>, Error>;
    fn list(&self, index: Option<ObjectDescription>) -> Result<Vec<ObjectDescription>, Error>;
    fn remove(&self, desc: ObjectDescription) -> Result<(), Error>;
    /// TODO: sync feature
    /// This method is for the sync feature
    fn fetch_meta(&self, desc: &mut ObjectDescription) {
        unimplemented!()
    }
    fn _as_base_from(&self, downstream_object: ObjectDescription) -> Canal
    where
        Self: Sized + Clone + 'static,
    {
        Canal {
            up_pool: Some(Box::new(self.clone())),
            down_pool: None,
            upstream_object: ObjectDescription::default(),
            downstream_object,
            default: PoolType::UpPool,
        }
    }
    fn as_base_from(&self, resource_location: &str) -> Result<Canal, Error>;
    fn _as_target_to(&self, upstream_object: ObjectDescription) -> Canal
    where
        Self: Sized + Clone + 'static,
    {
        Canal {
            up_pool: Some(Box::new(self.clone())),
            down_pool: None,
            upstream_object,
            downstream_object: ObjectDescription::default(),
            default: PoolType::DownPool,
        }
    }
    fn as_target_to(
        &self,
        resource_location: &str,
        upstream_object: ObjectDescription,
    ) -> Result<Canal, Error>;
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
