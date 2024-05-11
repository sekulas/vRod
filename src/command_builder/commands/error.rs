use std::path::PathBuf;

use crate::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot recognize collection name in path: {0}")]
    CollectionPathProblem(PathBuf),

    #[error("Cannot convert collection name to string: {0}")]
    CollectionNameToStrProblem(PathBuf),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),
}
