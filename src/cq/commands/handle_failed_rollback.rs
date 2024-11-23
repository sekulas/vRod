use std::path::Path;

use crate::{
    components::wal::Wal,
    cq::{
        commands::Result, types::HANDLE_FAILED_ROLLBACK_C_STR, CQAction, CQTarget, CQValidator,
        Command, Validator,
    },
    database::DbConfig,
    types::DB_CONFIG,
};

const FAILED_TO_LOAD_DB_CONFIG_ERR_MSG: &str =
    "failed to load database configuration file during HANDLE_FAILED_ROLLBACK command execution,
     this may mean that the database will not proceed with any operations";

pub struct HandleFailedRollbackCommand {
    target: CQTarget,
}

impl HandleFailedRollbackCommand {
    pub fn new(target: CQTarget) -> Self {
        HandleFailedRollbackCommand { target }
    }
}

impl Command for HandleFailedRollbackCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.target);

        wal.append(self.to_string())?;

        Self::mark_target_as_readonly(&self.target)?;

        wal.commit()?;

        println!("Target marked as readonly.");

        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.target);
        self.execute(wal)
    }
}

impl CQAction for HandleFailedRollbackCommand {
    fn to_string(&self) -> String {
        HANDLE_FAILED_ROLLBACK_C_STR.to_string()
    }
}

impl HandleFailedRollbackCommand {
    fn mark_target_as_readonly(target: &CQTarget) -> Result<()> {
        match target {
            CQTarget::Database { database_path } => {
                Self::mark_db_as_readonly(database_path)?;
            }
            CQTarget::Collection {
                database_path,
                collection_name,
            } => {
                Self::mark_collection_as_readonly(database_path, collection_name)?;
            }
        }

        Ok(())
    }

    fn mark_db_as_readonly(database_path: &Path) -> Result<()> {
        let mut db_config = Self::load_db_config(database_path);
        db_config.set_db_as_readonly()?;
        Ok(())
    }

    fn mark_collection_as_readonly(database_path: &Path, collection_name: &str) -> Result<()> {
        let mut db_config = Self::load_db_config(database_path);
        db_config.set_collection_as_readonly(collection_name)?;
        Ok(())
    }

    fn load_db_config(database_path: &Path) -> DbConfig {
        DbConfig::load(&database_path.join(DB_CONFIG))
            .unwrap_or_else(|e| panic!("{}, error: {}", FAILED_TO_LOAD_DB_CONFIG_ERR_MSG, e))
    }
}
