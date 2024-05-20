use super::{id_offset_storage::IdOffsetStorage, index::Index, storage::Storage, Result};
use crate::{
    types::{ID_OFFSET_STORAGE_FILE, INDEX_FILE, STORAGE_FILE},
    wal::Wal,
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
        todo!();
    }

    //TODO: HERE VECTOR VALIDATION
}
