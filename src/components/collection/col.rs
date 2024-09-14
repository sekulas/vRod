use super::index::types::DEFAULT_BRANCHING_FACTOR;
use super::{index::tree::BPTree, storage::Storage, types::OperationMode, Result};
use crate::components::wal::Wal;
use crate::types::{Dim, Offset};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct Collection {
    path: PathBuf,
    storage: Storage,
    wal: Wal,
    index: BPTree,
}

impl Collection {
    pub fn create(path: &Path, name: &str) -> Result<()> {
        let collection_path = path.join(name);

        fs::create_dir(&collection_path)?;
        Wal::create(&collection_path)?;
        Storage::create(&collection_path)?;
        BPTree::create(&collection_path, DEFAULT_BRANCHING_FACTOR)?;

        println!("Collection created at: {:?}", collection_path);

        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let collection_path = path.to_path_buf();

        let storage = Storage::load(&collection_path.join(STORAGE_FILE))?;
        let index = BPTree::load(&collection_path.join(INDEX_FILE))?;

        Ok(Self {
            path: collection_path,
            storage,
            index,
        })
    }

    pub fn insert(&mut self, vector: &[Dim], payload: &str) -> Result<Offset> {
        let offset = self
            .storage
            .insert(vector, payload, &OperationMode::RawOperation)?;

        Ok(offset)
    }
}

#[cfg(test)]
mod tests {
    use crate::types::WAL_FILE;

    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn create_should_create_col() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";

        //Act
        Collection::create(path, collection_name)?;

        //Assert
        assert!(path.join(collection_name).exists());
        assert!(path.join(collection_name).join(WAL_FILE).exists());
        assert!(path.join(collection_name).join(STORAGE_FILE).exists());
        assert!(path.join(collection_name).join(INDEX_FILE).exists());

        Ok(())
    }

    #[test]
    fn load_should_load_col() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;

        //Act
        let col = Collection::load(&path.join(collection_name))?;

        //Assert
        assert_eq!(col.path, path.join(collection_name));

        Ok(())
    }

    #[test]
    fn insert_should_store_record() -> Result<()> {
        Ok(())
    }

    #[test]
    fn insert_twice_should_store_two_records() -> Result<()> {
        Ok(())
    }

    #[test]
    fn delete_should_delete_record() -> Result<()> {
        Ok(())
    }

    #[test]
    fn update_should_delete_old_record_and_store_new() -> Result<()> {
        Ok(())
    }

    #[test]
    fn search_should_return_record() -> Result<()> {
        Ok(())
    }

    #[test]
    fn search_for_deleted_record_should_return_nothing() -> Result<()> {
        Ok(())
    }

    #[test]
    fn rollback_insert_should_remove_record() -> Result<()> {
        Ok(())
    }

    #[test]
    fn rollback_delete_should_restore_record() -> Result<()> {
        Ok(())
    }

    #[test]
    fn rollback_update_should_restore_old_record() -> Result<()> {
        Ok(())
    }
}
