use std::path::PathBuf;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot determine the path to the collection.")]
    CannotDetermineCollectionPath {
        database_path: Option<PathBuf>,
        collection_name: Option<String>,
    },

    #[error("Collection '{collection_name}' already exists.")]
    CollectionAlreadyExists { collection_name: String },

    #[error("Collection '{collection_name}' does not exist.")]
    CollectionDoesNotExist { collection_name: String },

    #[error("Unrecognized command '{0}'.")]
    UnrecognizedCommand(String),

    #[error("No name for the collection has been provided.")]
    MissingCollectionName,

    #[error("Missing argument for the given command.")]
    MissingArgument,

    #[error("Cannot parse: {data}\nVector must be in the format: vector;payload\\n.")]
    InvalidDataFormat { data: String },

    #[error("No vector provided in: {data}\nVector must be in the format: vector;payload\\n.")]
    NoVector { data: String },

    #[error("No payload provided in: {data}\nVector must be in the format: vector;payload\\n.")]
    NoPayload { data: String },

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("{description}")]
    Collection { description: String },

    #[error(transparent)]
    ParseIntError(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
