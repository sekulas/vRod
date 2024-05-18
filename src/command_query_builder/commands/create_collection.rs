use super::Result;
use crate::{
    command_query_builder::{CQAction, Command},
    database::DbConfig,
    types::{DB_CONFIG, ID_OFFSET_STORAGE_FILE, INDEX_FILE, STORAGE_FILE, WAL_FILE},
    wal::Wal,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct CreateCollectionCommand {
    path: PathBuf,
    collection_name: String,
}

impl CreateCollectionCommand {
    pub fn new(path: &Path, collection_name: String) -> Self {
        CreateCollectionCommand {
            path: path.to_owned(),
            collection_name,
        }
    }
}

impl Command for CreateCollectionCommand {
    fn execute(&self) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);
        let mut db_config: DbConfig = DbConfig::load(&self.path.join(DB_CONFIG))?;

        if collection_path.exists() {
            fs::remove_dir_all(&collection_path)?;
        }

        setup_collection(&collection_path)?;

        db_config.add_collection(&self.collection_name)?;

        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir_all(collection_path)?;
        }

        Ok(())
    }
}

fn setup_collection(collection_path: &Path) -> Result<()> {
    fs::create_dir(collection_path)?;
    Wal::create(collection_path)?;
    fs::File::create(collection_path.join(STORAGE_FILE))?;
    fs::File::create(collection_path.join(ID_OFFSET_STORAGE_FILE))?;
    fs::File::create(collection_path.join(INDEX_FILE))?;

    Ok(())
}

impl CQAction for CreateCollectionCommand {
    fn to_string(&self) -> String {
        format!("CREATE {}", self.collection_name)
    }
}
