use crate::{database::DbConfig, types::DB_CONFIG};

use super::CQTarget;

//Has to be used directly in Command Queries

pub struct CQValidator;

pub trait Validator {
    fn target_exists(target: &CQTarget) -> bool;
}

impl Validator for CQValidator {
    fn target_exists(target: &CQTarget) -> bool {
        match target {
            CQTarget::Database { database_path } => {
                let _ = DbConfig::load(&database_path.join(DB_CONFIG)).unwrap_or_else(|_| {
                    panic!("failed to load database configuration file during database existance checking")
                });
                true
            }
            CQTarget::Collection {
                database_path,
                collection_name,
            } => {
                let db_config = DbConfig::load(&database_path.join(DB_CONFIG)).unwrap_or_else(|_| {
                    panic!("failed to load database configuration file during collection existance checking")
                });
                db_config.collection_exists(collection_name)
            }
        }
    }
}
