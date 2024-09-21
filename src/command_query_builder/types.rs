use crate::types::Lsn;

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
    fn execute(&mut self, lsn: Lsn) -> CommandResult<()>;
    fn rollback(&mut self, lsn: Lsn) -> CommandResult<()>;
}

pub trait Query: CQAction {
    fn execute(&self) -> QueryResult<()>;
}
