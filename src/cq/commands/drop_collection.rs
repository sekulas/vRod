use super::{Error, Result};
use crate::{
    components::wal::Wal,
    cq::{types::DROP_C_STR, CQAction, CQTarget, CQValidator, Command, Validator},
    database::DbConfig,
    types::DB_CONFIG,
};
use std::fs;

pub struct DropCollectionCommand {
    database: CQTarget,
    collection_name: String,
}

impl DropCollectionCommand {
    pub fn new(database: CQTarget, collection_name: String) -> Self {
        DropCollectionCommand {
            database,
            collection_name,
        }
    }
}

impl Command for DropCollectionCommand {
    fn execute(&self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.database);

        let path = self.database.get_target_path();
        let mut db_config: DbConfig = DbConfig::load(&path.join(DB_CONFIG))?;

        if !db_config.collection_exists(&self.collection_name) {
            return Err(Error::CollectionDoesNotExist {
                collection_name: self.collection_name.clone(),
            });
        }

        wal.append(self.to_string())?;

        db_config.remove_collection(&self.collection_name)?;

        let collection_path = path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir_all(&collection_path)?;
        }

        println!("Collection dropped succesfully.");

        wal.commit()?;
        Ok(())
    }

    fn rollback(&self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.database);
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        println!("No ROLLBACK for DROP command provided.");

        Err(Error::RollbackFailed {
            command: self.to_string(),
        })
    }
}

impl CQAction for DropCollectionCommand {
    fn to_string(&self) -> String {
        format!("{} {}", DROP_C_STR, self.collection_name)
    }
}
