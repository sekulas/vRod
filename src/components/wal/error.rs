use crate::command_query_builder;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Error while parsing wal entry to command and arg: {0}")]
    ParsingEntry(String),

    #[error(transparent)]
    CommandBuilder(#[from] command_query_builder::Error),

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
