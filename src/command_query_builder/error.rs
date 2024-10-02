pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Collection '{collection_name}' already exists.")]
    CollectionAlreadyExists { collection_name: String },

    #[error("Collection '{collection_name}' does not exist.")]
    CollectionDoesNotExist { collection_name: String },

    #[error("Unrecognized command or query '{0}'.")]
    UnrecognizedCommandOrQuery(String),

    #[error("No name for the collection has been provided.")]
    MissingCollectionName,

    #[error("Missing argument for the given command. {description}")]
    MissingArgument { description: String },

    #[error("Invalid data format: {description}")]
    InvalidDataFormat { description: String },

    #[error("No data in the source.")]
    NoDataInSource,

    #[error(transparent)]
    ParseFloat(#[from] std::num::ParseFloatError),

    #[error("{description}")]
    Collection { description: String },

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
