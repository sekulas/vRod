use crate::{database, utils};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseNameFlag,

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Utils error: {0}")]
    Utils(#[from] utils::Error),

    #[error("Database error: {0}")]
    Database(#[from] database::Error),
}
