use crate::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unrecognized command '{0}'.")]
    UnrecognizedCommand(String),

    #[error("No name for the collection has been provided.")]
    MissingCollectionName,

    #[error("Collection with given name '{0}' already exists.")]
    CollectionExists(String),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),
}
