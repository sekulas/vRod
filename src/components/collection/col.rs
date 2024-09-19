use super::index::types::{
    Index, IndexCommand, IndexCommandResult, IndexQuery, IndexQueryResult, DEFAULT_BRANCHING_FACTOR,
};

use super::storage::types::{
    StorageCommand, StorageCommandResult, StorageInterface, StorageQuery, StorageQueryResult,
};
use super::types::{CollectionDeleteResult, CollectionSearchResult, CollectionUpdateResult};
use super::Error;
use super::{index::tree::BPTree, storage::Storage, Result};
use crate::components::wal::Wal;
use crate::types::{Dim, Lsn, RecordId, INDEX_FILE, STORAGE_FILE};
use std::{fs, path::Path};

pub struct Collection {
    storage: Storage,
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

        Ok(Self { storage, index })
    }

    pub fn insert(&mut self, vector: &[Dim], payload: &str, lsn: Lsn) -> Result<()> {
        let storage_insert_result = self
            .storage
            .perform_command(StorageCommand::Insert { vector, payload }, lsn)?;

        match storage_insert_result {
            StorageCommandResult::Inserted { offset } => {
                self.index
                    .perform_command(IndexCommand::Insert(offset), lsn)?;
                Ok(())
            }
            _ => Err(Error::Unexpected(
                "Collection: Insert returned unexpected result.",
            )),
        }
    }

    pub fn bulk_insert(&mut self, vectors_and_payloads: &[(&[Dim], &str)], lsn: Lsn) -> Result<()> {
        let bulk_insert_result = self.storage.perform_command(
            StorageCommand::BulkInsert {
                vectors_and_payloads,
            },
            lsn,
        )?;

        match bulk_insert_result {
            StorageCommandResult::BulkInserted { offsets } => {
                self.index
                    .perform_command(IndexCommand::BulkInsert(offsets), lsn)?;
                Ok(())
            }
            _ => Err(Error::Unexpected(
                "Collection: Bulk insert returned unexpected result.",
            )),
        }
    }

    pub fn search(&mut self, record_id: RecordId) -> Result<CollectionSearchResult> {
        let search_result = self.index.perform_query(IndexQuery::Search(record_id))?;

        match search_result {
            IndexQueryResult::FoundValue(offset) => {
                let record = self
                    .storage
                    .perform_query(StorageQuery::Search { offset })?;

                match record {
                    StorageQueryResult::FoundRecord { record } => {
                        Ok(CollectionSearchResult::FoundRecord(record))
                    }
                    StorageQueryResult::NotFound => Ok(CollectionSearchResult::NotFound),
                }
            }
            IndexQueryResult::NotFound => Ok(CollectionSearchResult::NotFound),
            _ => Err(Error::Unexpected(
                "Collection: Search returned unexpected result.",
            )),
        }
    }

    pub fn update(
        &mut self,
        record_id: RecordId,
        vector: Option<&[Dim]>,
        payload: Option<&str>,
        lsn: Lsn,
    ) -> Result<CollectionUpdateResult> {
        let search_result = self.index.perform_query(IndexQuery::Search(record_id))?;

        match search_result {
            IndexQueryResult::FoundValue(offset) => {
                let update_result = self.storage.perform_command(
                    StorageCommand::Update {
                        offset,
                        vector,
                        payload,
                    },
                    lsn,
                )?;

                match update_result {
                    StorageCommandResult::Updated { new_offset } => {
                        let update_result = self
                            .index
                            .perform_command(IndexCommand::Update(record_id, new_offset), lsn)?;
                        match update_result {
                            IndexCommandResult::Updated => Ok(CollectionUpdateResult::Updated),
                            _ => Err(Error::Unexpected(
                                "Collection: Update - post index update returned unexpected result.",
                            )),
                        }
                    }
                    _ => Err(Error::Unexpected(
                        "Collection: update - post storage update returned unexpected result.",
                    )),
                }
            }
            IndexQueryResult::NotFound => Ok(CollectionUpdateResult::NotFound),
            _ => Err(Error::Unexpected(
                "Collection: Update - post search update returned unexpected result.",
            )),
        }
    }

    pub fn delete(&mut self, record_id: RecordId, lsn: Lsn) -> Result<CollectionDeleteResult> {
        let search_result = self.index.perform_query(IndexQuery::Search(record_id))?;

        match search_result {
            IndexQueryResult::FoundValue(offset) => match self
                .storage
                .perform_command(StorageCommand::Delete { offset }, lsn)?
            {
                StorageCommandResult::Deleted => Ok(CollectionDeleteResult::Deleted),
                _ => Err(Error::Unexpected(
                    "Collection: Delete - post storage delete returned unexpected result.",
                )),
            },
            IndexQueryResult::NotFound => Ok(CollectionDeleteResult::NotFound),
            _ => Err(Error::Unexpected(
                "Collection: Delete - post search delete returned unexpected result.",
            )),
        }
    }

    // pub fn rollback_insert(&mut self) -> Result<()> {
    //     let record_id = self.index.get_highest_id_in_tree()?;
    //     let query_result = self.index.perform_query(IndexQuery::Search(record_id))?;

    //     match query_result {
    //         IndexQueryResult::SearchResult(offset) => {
    //             self.storage.delete(offset, &OperationMode::RawOperation)?;
    //             Ok(())
    //         }
    //         IndexQueryResult::NotFound => {
    //             println!("Collection: Cannot rollback insert. No records to rollback.");
    //             Ok(())
    //         }
    //         _ => Err(Error::UnexpectedError(
    //             "Collection: Rollback insert returned unexpected result.",
    //         )),
    //     }
    // }
}

#[cfg(test)]
mod tests {
    use crate::{components::collection::storage::strg::Record, types::WAL_FILE};

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
        let result = Collection::load(&path.join(collection_name));

        //Assert
        assert!(result.is_ok());
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

        let expected_record = Record::new(1, &vector, payload);

        //Act
        col.insert(&vector, payload, 1)?;

        //Assert
        let stored_vector = col.search(1)?;
        assert_eq!(
            stored_vector,
            CollectionSearchResult::FoundRecord(expected_record)
        );
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

        let expected_record = Record::new(1, &vector, payload);
        let expected_record2 = Record::new(2, &vector, payload);

        //Act
        col.insert(&vector, payload, 1)?;
        col.insert(&vector, payload, 2)?;

        //Assert
        let stored_vector = col.search(1)?;
        assert_eq!(
            stored_vector,
            CollectionSearchResult::FoundRecord(expected_record)
        );

        let stored_vector2 = col.search(2)?;
        assert_eq!(
            stored_vector2,
            CollectionSearchResult::FoundRecord(expected_record2)
        );
        Ok(())
    }

    #[test]
    fn bulk_insert_two_records_should_store_two_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test1";
        let vector2 = vec![4.0, 5.0, 6.0];
        let payload2 = "test2";

        let expected_record = Record::new(1, &vector, payload);
        let expected_record2 = Record::new(1, &vector2, payload2);

        //Act
        col.bulk_insert(&[(&vector, payload), (&vector2, payload2)], 1)?;

        //Assert
        let stored_vector = col.search(1)?;
        assert_eq!(
            stored_vector,
            CollectionSearchResult::FoundRecord(expected_record)
        );

        let stored_vector2 = col.search(2)?;
        assert_eq!(
            stored_vector2,
            CollectionSearchResult::FoundRecord(expected_record2)
        );
        Ok(())
    }

    #[test]
    fn bulk_insert_no_records_should_not_store_any_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;

        //Act
        col.bulk_insert(&[], 1)?;

        //Assert
        assert_eq!(col.search(1)?, CollectionSearchResult::NotFound);
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
        col.insert(&vector, payload, 1)?;

        let expected_record = Record::new(1, &vector, payload);

        //Act
        let record = col.search(1)?;

        //Assert
        assert_eq!(record, CollectionSearchResult::FoundRecord(expected_record));
        Ok(())
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
        col.insert(&vector, payload, 1)?;

        //Act
        col.delete(1, 2)?;

        //Assert
        assert_eq!(col.search(1)?, CollectionSearchResult::NotFound);
        Ok(())
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
        assert_eq!(record, CollectionSearchResult::NotFound);
        Ok(())
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
        col.insert(&vector, payload, 1)?;

        //Act
        let new_vector = vec![4.0, 5.0, 6.0];
        let new_payload = "new_test";
        col.update(1, Some(&new_vector), Some(new_payload), 2)?;

        //Assert
        let expected_record = Record::new(2, &new_vector, new_payload);
        let record = col.search(1)?;

        assert_eq!(record, CollectionSearchResult::FoundRecord(expected_record));
        Ok(())
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
        col.insert(&vector, payload, 1)?;

        //Act
        col.delete(1, 2)?;

        //Assert
        let record = col.search(1)?;

        assert_eq!(record, CollectionSearchResult::NotFound);
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
