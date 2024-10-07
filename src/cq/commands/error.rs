use crate::components::{collection, wal};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collection '{collection_name}' already exists.")]
    CollectionAlreadyExists { collection_name: String },

    #[error("Collection '{collection_name}' does not exist.")]
    CollectionDoesNotExist { collection_name: String },

    #[error(transparent)]
    Collection(#[from] collection::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
