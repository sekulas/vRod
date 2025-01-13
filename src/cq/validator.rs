use std::path::Path;

use crate::{database::DbConfig, types::DB_CONFIG};

use super::CQTarget;

pub struct CQValidator;

const FAILED_TO_LOAD_DB_CONFIG_ERR_MSG: &str =
    "failed to load database configuration file during command/query validation";

pub trait Validator {
    fn target_exists(target: &CQTarget);
}

impl Validator for CQValidator {
    fn target_exists(target: &CQTarget) {
        match target {
            CQTarget::Database { database_path } => {
                CQValidator::load_db_config(database_path);
            }
            CQTarget::Collection {
                database_path,
                collection_name,
            } => {
                let db_config = CQValidator::load_db_config(database_path);
                if !CQValidator::collection_exists(&db_config, collection_name) {
                    panic!(
                        "collection '{}' does not exist in the database in path: '{:?}'",
                        collection_name, database_path
                    );
                }
            }
        }
    }
}

impl CQValidator {
    fn load_db_config(database_path: &Path) -> DbConfig {
        DbConfig::load(&database_path.join(DB_CONFIG))
            .unwrap_or_else(|e| panic!("{}, error: {}", FAILED_TO_LOAD_DB_CONFIG_ERR_MSG, e))
    }

    fn collection_exists(db_config: &DbConfig, collection_name: &str) -> bool {
        db_config.collection_exists(collection_name)
    }
}
