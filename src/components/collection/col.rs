use super::{
    id_offset_storage::IdOffsetStorage, index::Index, storage::Storage, types::OperationMode,
    Result,
};
use crate::types::Dim;
use crate::{
    components::wal::Wal,
    types::{ID_OFFSET_STORAGE_FILE, INDEX_FILE, STORAGE_FILE},
};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct Collection {
    path: PathBuf,
    storage: Storage,
    wal: Wal,
    id_offset_storage: IdOffsetStorage,
    index: Index,
}

impl Collection {
    pub fn create(path: &Path, name: &str) -> Result<()> {
        let collection_path = path.join(name);

        fs::create_dir(&collection_path)?;
        Wal::create(&collection_path)?;
        fs::File::create(collection_path.join(STORAGE_FILE))?;
        fs::File::create(collection_path.join(ID_OFFSET_STORAGE_FILE))?;
        fs::File::create(collection_path.join(INDEX_FILE))?;

        println!("Collection created at: {:?}", collection_path);

        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self> {
        todo!("Not implemented.");
    }

    pub fn insert(&mut self, vector: &[Dim], payload: &str) -> Result<()> {
        let offset = self
            .storage
            .insert(vector, payload, &OperationMode::RawOperation)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn create_should_create_col() -> Result<()> {
        todo!();
    }

    #[test]
    fn load_should_load_col() -> Result<()> {
        todo!();
    }

    #[test]
    fn insert_should_store_record() -> Result<()> {
        todo!();
    }

    #[test]
    fn insert_twice_should_store_two_records() -> Result<()> {
        todo!();
    }

    #[test]
    fn delete_should_delete_record() -> Result<()> {
        todo!();
    }

    #[test]
    fn update_should_delete_old_record_and_store_new() -> Result<()> {
        todo!();
    }

    #[test]
    fn search_should_return_record() -> Result<()> {
        todo!();
    }

    #[test]
    fn search_for_deleted_record_should_return_nothing() -> Result<()> {
        todo!();
    }

    #[test]
    fn rollback_insert_should_remove_record() -> Result<()> {
        todo!();
    }

    #[test]
    fn rollback_delete_should_restore_record() -> Result<()> {
        todo!();
    }

    #[test]
    fn rollback_update_should_restore_old_record() -> Result<()> {
        todo!();
    }
}
