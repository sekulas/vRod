use super::commands::Result as CommandResult;
use super::queries::Result as QueryResult;
use crate::types::Lsn;
use std::path::PathBuf;

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
        let path = match self {
            CQTarget::Database { database_path } => database_path.to_owned(),
            CQTarget::Collection {
                database_path,
                collection_name,
            } => database_path.join(collection_name),
        };
        path
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
    fn execute(&mut self, lsn: Lsn) -> CommandResult<()>;
    fn rollback(&mut self, lsn: Lsn) -> CommandResult<()>;
}

pub trait Query: CQAction {
    fn execute(&mut self) -> QueryResult<()>;
}
