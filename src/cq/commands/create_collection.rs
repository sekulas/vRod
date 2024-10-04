use super::Result;
use crate::{
    components::collection::Collection,
    cq::{CQAction, Command},
    database::DbConfig,
    types::{Lsn, DB_CONFIG},
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
    fn execute(&mut self, _: Lsn) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);
        let mut db_config: DbConfig = DbConfig::load(&self.path.join(DB_CONFIG))?;

        if collection_path.exists() {
            fs::remove_dir_all(&collection_path)?;
        }

        Collection::create(&self.path, &self.collection_name)?;

        db_config.add_collection(&self.collection_name)?;

        Ok(())
    }

    fn rollback(&mut self, _: Lsn) -> Result<()> {
        let mut db_config = DbConfig::load(&self.path.join(DB_CONFIG))?;

        if db_config.collection_exists(&self.collection_name) {
            db_config.remove_collection(&self.collection_name)?;
        }

        let collection_path = self.path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir_all(collection_path)?;
        }

        Ok(())
    }
}

impl CQAction for CreateCollectionCommand {
    fn to_string(&self) -> String {
        format!("CREATE {}", self.collection_name)
    }
}
