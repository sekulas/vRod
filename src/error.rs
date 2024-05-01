use crate::utils;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseNameFlag,

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Utils error: {0}")]
    UtilsError(#[from] utils::error::Error),
}