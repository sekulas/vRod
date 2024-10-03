use std::path::PathBuf;

use crate::components::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Directory '{0}' already exists.")]
    DirectoryExists(PathBuf),

    #[error("[CODE:Incorrect checksum for 'DbConfig'.")]
    IncorrectChecksum,

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(&'static str),
}
