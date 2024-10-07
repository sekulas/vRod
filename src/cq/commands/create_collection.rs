use super::{Error, Result};
use crate::{
    components::{collection::Collection, wal::Wal},
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
    database::DbConfig,
    types::DB_CONFIG,
};
use std::fs;

pub struct CreateCollectionCommand {
    database: CQTarget,
    collection_name: String,
}

impl CreateCollectionCommand {
    pub fn new(database: CQTarget, collection_name: String) -> Self {
        CreateCollectionCommand {
            database,
            collection_name,
        }
    }
}

impl Command for CreateCollectionCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.database);

        let path = self.database.get_target_path();
        let collection_path = path.join(&self.collection_name);
        let mut db_config: DbConfig = DbConfig::load(&path.join(DB_CONFIG))?;

        if db_config.collection_exists(&self.collection_name) {
            return Err(Error::CollectionAlreadyExists {
                collection_name: self.collection_name.clone(),
            });
        }

        wal.append(self.to_string())?;

        if collection_path.exists() {
            fs::remove_dir_all(&collection_path)?;
        }

        Collection::create(&path, &self.collection_name)?;

        db_config.add_collection(&self.collection_name)?;

        wal.commit()?;
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.database);
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        let path = self.database.get_target_path();
        let mut db_config = DbConfig::load(&path.join(DB_CONFIG))?;

        if db_config.collection_exists(&self.collection_name) {
            db_config.remove_collection(&self.collection_name)?;
        }

        // TODO: ##### Is that needed? If the collection was created, it will be removed in the execute method.
        let collection_path = &path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir_all(collection_path)?;
        }

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for CreateCollectionCommand {
    fn to_string(&self) -> String {
        format!("CREATE {}", self.collection_name)
    }
}
