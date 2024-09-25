use super::index::types::{Index, IndexCommand, IndexCommandResult, IndexQuery, IndexQueryResult};

use super::storage::strg::Record;
use super::storage::types::{
    StorageCommand, StorageCommandResult, StorageInterface, StorageQuery, StorageQueryResult,
};
use super::types::{
    CollectionDeleteResult, CollectionInsertResult, CollectionSearchResult, CollectionUpdateResult,
};
use super::Error;
use super::{
    index::tree::BPTree,
    storage::{Error as StorageError, Storage},
    Result,
};
use crate::components::wal::Wal;
use crate::types::{Dim, Lsn, RecordId, INDEX_FILE, STORAGE_FILE};
use std::path::PathBuf;
use std::{fs, path::Path};

pub struct Collection {
    collection_path: PathBuf,
    storage: Storage,
    index: BPTree,
}

impl Collection {
    pub fn create(path: &Path, name: &str) -> Result<()> {
        let collection_path = path.join(name);

        fs::create_dir(&collection_path)?;
        Wal::create(&collection_path)?;
        Storage::create(&collection_path, None)?;
        BPTree::create(&collection_path, None)?;

        println!("Collection created at: {:?}", collection_path);

        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        let collection_path = path.to_path_buf();

        let storage = Storage::load(&collection_path.join(STORAGE_FILE))?;
        let index = BPTree::load(&collection_path.join(INDEX_FILE))?;

        Ok(Self {
            collection_path,
            storage,
            index,
        })
    }

    pub fn insert(
        &mut self,
        vector: &[Dim],
        payload: &str,
        lsn: Lsn,
    ) -> Result<CollectionInsertResult> {
        let storage_insert_result = self
            .storage
            .perform_command(StorageCommand::Insert { vector, payload }, lsn);

        if let Err(StorageError::InvalidVectorDim {
            expected,
            actual,
            vector,
        }) = storage_insert_result
        {
            return Ok(CollectionInsertResult::NotInserted {
                description: StorageError::InvalidVectorDim {
                    expected,
                    actual,
                    vector,
                }
                .to_string(),
            });
        }

        match storage_insert_result? {
            StorageCommandResult::Inserted { offset } => {
                self.index
                    .perform_command(IndexCommand::Insert(offset), lsn)?;
                Ok(CollectionInsertResult::Inserted)
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

    pub fn search_all(&mut self) -> Result<Vec<(RecordId, Record)>> {
        let query_result = self.index.perform_query(IndexQuery::SearchAll)?;

        match query_result {
            IndexQueryResult::FoundKeysAndValues(keys_and_values) => {
                let mut records = Vec::with_capacity(keys_and_values.len());

                for (record_id, offset) in keys_and_values {
                    let record = self
                        .storage
                        .perform_query(StorageQuery::Search { offset })?;

                    if let StorageQueryResult::FoundRecord { record } = record {
                        records.push((record_id, record));
                    }
                }

                Ok(records)
            }
            _ => Err(Error::Unexpected(
                "Collection: Search all returned unexpected result.",
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
                );

                if let Err(StorageError::InvalidVectorDim {
                    expected,
                    actual,
                    vector,
                }) = update_result
                {
                    return Ok(CollectionUpdateResult::NotUpdated {
                        description: StorageError::InvalidVectorDim {
                            expected,
                            actual,
                            vector,
                        }
                        .to_string(),
                    });
                }

                match update_result? {
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
                    StorageCommandResult::NotFound => Ok(CollectionUpdateResult::NotFound),
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
                StorageCommandResult::NotFound => Ok(CollectionDeleteResult::NotFound),
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

    pub fn rollback_insertion_like_command(&mut self, lsn: Lsn) -> Result<()> {
        self.index.perform_rollback(lsn)?;
        Ok(())
    }

    pub fn rollback_update_command(&mut self, lsn: Lsn) -> Result<()> {
        self.storage.perform_rollback(lsn)?;
        self.index.perform_rollback(lsn)?;
        Ok(())
    }

    pub fn rollback_delete_command(&mut self, lsn: Lsn) -> Result<()> {
        self.storage.perform_rollback(lsn)?;
        Ok(())
    }

    pub fn reindex(&mut self, lsn: Lsn) -> Result<()> {
        let records = self.search_all()?;

        let (mut new_storage, mut new_index) = self.prepare_new_storage_and_index(lsn)?;

        let vecs_and_payloads_refs: Vec<(&[Dim], &str)> = records
            .iter()
            .map(|(_, record)| (record.vector.as_slice(), record.payload.as_str()))
            .collect();

        let strg_result = new_storage.perform_command(
            StorageCommand::BulkInsert {
                vectors_and_payloads: &vecs_and_payloads_refs,
            },
            lsn,
        )?;

        let new_offsets =
            match strg_result {
                StorageCommandResult::BulkInserted { offsets } => offsets,
                _ => return Err(Error::Unexpected(
                    "Collection: Reindex - insertion to new storage returned unexpected result.",
                )),
            };

        let idx_result = new_index.perform_command(IndexCommand::BulkInsert(new_offsets), lsn)?;

        match idx_result {
            IndexCommandResult::BulkInserted => {
                self.swap_storage_and_index(new_storage, new_index)?;
            }
            _ => {
                return Err(Error::Unexpected(
                    "Collection: Reindex - insertion to new index returned unexpected result.",
                ))
            }
        }

        Ok(())
    }

    fn prepare_new_storage_and_index(&self, lsn: Lsn) -> Result<(Storage, BPTree)> {
        let storage_name = format!("{STORAGE_FILE}_{}", lsn);
        let mut storage_settings = self.storage.get_creation_settings();
        storage_settings.name = storage_name;
        let new_storage: Storage = Storage::create(&self.collection_path, Some(storage_settings))?;

        let index_name = format!("{INDEX_FILE}_{}", lsn);
        let mut index_settings = self.index.get_creation_settings();
        index_settings.name = index_name;
        let new_index = BPTree::create(&self.collection_path, Some(index_settings))?;

        Ok((new_storage, new_index))
    }

    fn swap_storage_and_index(&mut self, new_storage: Storage, new_index: BPTree) -> Result<()> {
        let cur_strg_path = self.collection_path.join(STORAGE_FILE);
        let cur_idx_path = self.collection_path.join(INDEX_FILE);

        let bak_strg_path = self.collection_path.join(format!("{STORAGE_FILE}.bak"));
        let bak_idx_path = self.collection_path.join(format!("{INDEX_FILE}.bak"));

        let new_strg_path = self.collection_path.join(&new_storage.file_name);
        let new_idx_path = self.collection_path.join(&new_index.file.file_name);

        fs::rename(&cur_strg_path, &bak_strg_path)?;
        fs::rename(&cur_idx_path, &bak_idx_path)?;

        fs::rename(new_strg_path, &cur_strg_path)?;
        fs::rename(new_idx_path, &cur_idx_path)?;

        fs::remove_file(&bak_strg_path)?;
        fs::remove_file(&bak_idx_path)?;

        self.storage = new_storage;
        self.index = new_index;

        Ok(())
    }
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
        col.rollback_insertion_like_command(2)?;

        //Assert
        let record = col.search(1)?;

        assert_eq!(record, CollectionSearchResult::NotFound);
        Ok(())
    }

    #[test]
    fn rollback_bulk_insert_should_remove_records() -> Result<()> {
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
        col.bulk_insert(&[(&vector, payload), (&vector2, payload2)], 1)?;

        //Act
        col.rollback_insertion_like_command(2)?;

        //Assert
        assert_eq!(col.search(1)?, CollectionSearchResult::NotFound);
        assert_eq!(col.search(2)?, CollectionSearchResult::NotFound);
        Ok(())
    }

    #[test]
    fn rollback_update_should_restore_old_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        col.insert(&vector, payload, 1)?;
        let new_vector = vec![4.0, 5.0, 6.0];
        let new_payload = "new_test";
        col.update(1, Some(&new_vector), Some(new_payload), 2)?;

        //Act
        col.rollback_update_command(3)?; //TODO: Delete rollbacking needed? In this form.

        //Assert
        let expected_record = Record::new(1, &vector, payload);
        let record = col.search(1)?;

        assert_eq!(record, CollectionSearchResult::FoundRecord(expected_record));
        Ok(())
    }

    #[test]
    fn rollback_delete_should_restore_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        col.insert(&vector, payload, 1)?;
        col.delete(1, 2)?;

        //Act
        col.rollback_delete_command(3)?;

        //Assert
        let expected_record = Record::new(1, &vector, payload);
        let record = col.search(1)?;

        assert_eq!(record, CollectionSearchResult::FoundRecord(expected_record));
        Ok(())
    }

    #[test]
    fn reindex_should_make_idx_empty_if_all_records_deleted() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        col.insert(&vector, payload, 1)?;
        col.delete(1, 2)?;

        //Act
        col.reindex(3)?;

        //Assert
        let records = col.search_all()?;
        assert_eq!(records.len(), 0);
        Ok(())
    }

    #[test]
    fn reindex_should_reindex_all_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        col.insert(&vector, payload, 1)?;
        let vector2 = vec![4.0, 5.0, 6.0];
        let payload2 = "test2";
        col.insert(&vector2, payload2, 2)?;

        //Act
        col.reindex(3)?;

        //Assert
        let records = col.search_all()?;
        assert_eq!(records.len(), 2);
        assert!(records
            .iter()
            .any(|(_, record)| record.vector == vector && record.payload == payload));
        assert!(records
            .iter()
            .any(|(_, record)| record.vector == vector2 && record.payload == payload2));
        Ok(())
    }

    #[test]
    fn reindex_should_leave_only_not_deleted_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let collection_name = "test";
        Collection::create(path, collection_name)?;
        let mut col = Collection::load(&path.join(collection_name))?;
        let vector = vec![1.0, 2.0, 3.0];
        let payload = "test";
        col.insert(&vector, payload, 1)?;
        let vector2 = vec![4.0, 5.0, 6.0];
        let payload2 = "test2";
        col.insert(&vector2, payload2, 2)?;
        col.delete(1, 3)?;

        //Act
        col.reindex(4)?;

        //Assert
        let records = col.search_all()?;
        assert_eq!(records.len(), 1);
        assert!(records
            .iter()
            .any(|(_, record)| record.vector == vector2 && record.payload == payload2));
        Ok(())
    }
}
