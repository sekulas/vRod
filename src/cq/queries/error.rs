use crate::components::{collection, wal};
use std::{io, path::PathBuf};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("cannot proceed with simillarity search as the vector index does not exist for the given path: '{path}'")]
    VectorIndexDoesNotExist { path: PathBuf },

    #[error(transparent)]
    Collection(#[from] collection::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    Hnsw(#[from] hnsw::Error),
}
