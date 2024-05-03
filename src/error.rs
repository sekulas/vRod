use crate::{command, database, utils};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseName,

    #[error("Missing argument '-e' - 'command to execute'.")]
    MissingCommand,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Utils(#[from] utils::Error),

    #[error(transparent)]
    Database(#[from] database::Error),

    #[error(transparent)]
    Command(#[from] command::Error),
}
