use crate::types::LSN;

use super::CommandResult;
use super::QueryResult;

pub enum CQType {
    Command(Box<dyn Command>),
    Query(Box<dyn Query>),
}

pub trait CQAction {
    fn to_string(&self) -> String;
}

pub trait Command: CQAction {
    fn execute(&self, lsn: LSN) -> CommandResult<()>;
    fn rollback(&self, lsn: LSN) -> CommandResult<()>;
}

pub trait Query: CQAction {
    fn execute(&self) -> QueryResult<()>;
}
