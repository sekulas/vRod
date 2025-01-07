use super::commands::Result as CommandResult;
use super::queries::Result as QueryResult;
use crate::components::wal::Wal;
use std::path::PathBuf;
pub const ROLLBACK_C_STR: &str = "ROLLBACK";
pub const HANDLE_FAILED_ROLLBACK_C_STR: &str = "HANDLE_FAILED_ROLLBACK";

pub const CREATE_C_STR: &str = "CREATE";
pub const DROP_C_STR: &str = "DROP";
pub const LIST_COLLECTIONS_Q_STR: &str = "LIST_COLLECTIONS";
pub const TRUNCATE_WAL_C_STR: &str = "TRUNCATE_WAL";
pub const INSERT_C_STR: &str = "INSERT";
pub const SEARCH_Q_STR: &str = "SEARCH";
pub const SEARCH_ALL_Q_STR: &str = "SEARCH_ALL";
pub const UPDATE_C_STR: &str = "UPDATE";
pub const DELETE_C_STR: &str = "DELETE";
pub const BULK_INSERT_C_STR: &str = "BULK_INSERT";
pub const REINDEX_C_STR: &str = "REINDEX";
pub const SEARCH_SIMILAR_Q_STR: &str = "SEARCH_SIMILAR";
pub const CREATE_VECTOR_INDEX_C_STR: &str = "CREATE_VECTOR_INDEX";

#[derive(Clone)]
pub enum CQTarget {
    Database {
        database_path: PathBuf,
    },
    Collection {
        database_path: PathBuf,
        collection_name: String,
    },
}

impl CQTarget {
    pub fn get_target_path(&self) -> PathBuf {
        match self {
            CQTarget::Database { database_path } => database_path.to_owned(),
            CQTarget::Collection {
                database_path,
                collection_name,
            } => database_path.join(collection_name),
        }
    }
}

pub enum CQType {
    Command(Box<dyn Command>),
    Query(Box<dyn Query>),
}

pub trait CQAction {
    fn to_string(&self) -> String;
}

pub trait Command: CQAction {
    fn execute(&self, wal: &mut Wal) -> CommandResult<()>;
    fn rollback(&self, wal: &mut Wal) -> CommandResult<()>;
}

pub trait Query: CQAction {
    fn execute(&self) -> QueryResult<()>;
}
