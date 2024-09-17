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
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";

        //Act
        let offset = col.insert(&vector, payload)?;

        //Assert
        let stored_vector = col.storage.search(offset)?;
        assert_eq!(stored_vector.vector, vector);
        assert_eq!(stored_vector.payload, payload);
        assert_eq!(stored_vector.record_header.lsn, 1);
        assert!(!stored_vector.record_header.deleted);

        Ok(())
    }

    #[test]
    fn insert_twice_should_store_two_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";

        //Act
        let offset1 = col.insert(&vector, payload)?;
        let offset2 = col.insert(&vector, payload)?;

        //Assert
        let stored_vector1 = col.storage.search(offset1)?;
        assert_eq!(stored_vector1.vector, vector);
        assert_eq!(stored_vector1.payload, payload);
        assert_eq!(stored_vector1.record_header.lsn, 1);
        assert!(!stored_vector1.record_header.deleted);

        let stored_vector2 = col.storage.search(offset2)?;
        assert_eq!(stored_vector2.vector, vector);
        assert_eq!(stored_vector2.payload, payload);
        assert_eq!(stored_vector2.record_header.lsn, 2);
        assert!(!stored_vector2.record_header.deleted);

        Ok(())
    }

    #[test]
    fn search_should_return_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let _ = col.insert(&vector, payload)?;

        //Act
        let record = col.search(1)?;

        //Assert
        match record {
            CollectionSearchResult::NotFound => panic!("Record not found"),
            CollectionSearchResult::Found(record) => {
                assert_eq!(record.vector, vector);
                assert_eq!(record.payload, payload);
                assert_eq!(record.record_header.lsn, 1);
                assert!(!record.record_header.deleted);
        Ok(())
            }
        }
    }

    #[test]
    fn search_for_deleted_record_should_return_nothing() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let _ = col.insert(&vector, payload)?;

        //Act
        col.delete(1)?;

        //Assert
        match col.search(1)? {
            CollectionSearchResult::Found(_) => panic!("Record found"),
            CollectionSearchResult::NotFound => Ok(()),
        }
    }

    #[test]
    fn search_for_non_existing_record_should_return_nothing() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;

        //Act
        let record = col.search(1)?;

        //Assert
        match record {
            CollectionSearchResult::NotFound => Ok(()),
            CollectionSearchResult::Found(_) => panic!("Record found"),
        }
    }

    #[test]
    fn update_should_delete_old_record_and_store_new() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let _ = col.insert(&vector, payload)?;

        //Act
        let new_vector = vec![4.0, 5.0, 6.0];
        let new_payload = "new_test";
        col.update(1, Some(&new_vector), Some(new_payload))?;

        //Assert
        let record = col.search(1)?;

        match record {
            CollectionSearchResult::NotFound => panic!("Record not found"),
            CollectionSearchResult::Found(record) => {
                assert_eq!(record.vector, new_vector);
                assert_eq!(record.payload, new_payload);
                assert_eq!(record.record_header.lsn, 2);
                assert!(!record.record_header.deleted);
        Ok(())
    }
        }
    }

    #[test]
    fn delete_should_delete_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let _ = col.insert(&vector, payload)?;

        //Act
        col.delete(1)?;

        //Assert
        let record = col.search(1)?;

        match record {
            CollectionSearchResult::NotFound => Ok(()),
            CollectionSearchResult::Found(_) => panic!("Record found"),
        }
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
