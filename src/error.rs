use crate::{components::wal, cq, database, utils};
pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseName,

    #[error("missing argument '-e' - 'command to execute'.")]
    MissingCommand,

    #[error("database does not exist in path: '{0}'.")]
    DatabaseDoesNotExist(String),

    #[error("collection with name {0} does not exist in selected database.")]
    CollectionDoesNotExist(String),

    #[error("cannot perform operation on readonly target.")]
    TargetIsReadonly, //TODO: Possibly not needed if verification not needed.

    #[error("missing file path argument: {description}")]
    MissingFilePathArgument { description: String },

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
}
