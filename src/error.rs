use crate::{command_query_builder, components::wal, database, utils};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseName,

    #[error("Missing argument '-e' - 'command to execute'.")]
    MissingCommand,

    #[error("Database does not exist in path: {0}.")]
    DatabaseDoesNotExist(String),

    #[error("Collection does not exist in database: {0}.")]
    CollectionDoesNotExist(String),

    #[error("Cannot perform operation on readonly target.")]
    TargetIsReadonly, //TODO: Possibly not needed if verification not needed.

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utils(#[from] utils::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Database(#[from] database::Error),

    #[error(transparent)]
    CommandBuilder(#[from] command_query_builder::Error),

    #[error(transparent)]
    Command(#[from] command_query_builder::CommandError),

    #[error(transparent)]
    Query(#[from] command_query_builder::QueryError),

    #[error("Unexpected error: {0}")]
    Unexpected(&'static str),
}
