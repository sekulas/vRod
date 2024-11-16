use crate::components::{collection, wal};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("collection '{collection_name}' already exists.")]
    CollectionAlreadyExists { collection_name: String },

    #[error("collection '{collection_name}' does not exist.")]
    CollectionDoesNotExist { collection_name: String },

    #[error(transparent)]
    Collection(#[from] collection::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Hnsw(#[from] hnsw::Error),
}
