use crate::{components::wal, cq, database, utils};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseName,

    #[error("Missing argument '-e' - 'command to execute'.")]
    MissingCommand,

    #[error("Database does not exist in path: {0}.")]
    DatabaseDoesNotExist(String),

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
    CQ(#[from] cq::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(&'static str),
}
