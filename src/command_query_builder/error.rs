pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collection '{collection_name}' already exists.")]
    CollectionAlreadyExists { collection_name: String },

    #[error("Collection '{collection_name}' does not exist.")]
    CollectionDoesNotExist { collection_name: String },

    #[error("Unrecognized command '{0}'.")]
    UnrecognizedCommand(String),

    #[error("No name for the collection has been provided.")]
    MissingCollectionName,

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
