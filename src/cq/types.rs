use super::commands::Result as CommandResult;
use super::queries::Result as QueryResult;
use crate::components::wal::Wal;
use std::path::PathBuf;

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
    fn execute(&mut self, wal: &mut Wal) -> CommandResult<()>;
    fn rollback(&mut self, wal: &mut Wal) -> CommandResult<()>;
}

pub trait Query: CQAction {
    fn execute(&mut self) -> QueryResult<()>;
}
