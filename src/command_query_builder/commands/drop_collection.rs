use super::Result;
use crate::{
    command_query_builder::{CQAction, Command},
    database::DbConfig,
    types::DB_CONFIG,
};
use std::{
    fs,
    path::{Path, PathBuf},
};

pub struct DropCollectionCommand {
    pub path: PathBuf,
    pub collection_name: String,
}

impl DropCollectionCommand {
    pub fn new(path: &Path, collection_name: String) -> Self {
        DropCollectionCommand {
            path: path.to_owned(),
            collection_name,
        }
    }
}

impl Command for DropCollectionCommand {
    fn execute(&self) -> Result<()> {
        let mut db_config: DbConfig = DbConfig::load(&self.path.join(DB_CONFIG))?;
        db_config.remove_collection(&self.collection_name)?;

        let collection_path = self.path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir_all(&collection_path)?;
        }

        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }
}

impl CQAction for DropCollectionCommand {
    fn to_string(&self) -> String {
        format!("DROP {}", self.collection_name)
    }
}