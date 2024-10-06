use crate::components::wal;

use super::commands::Error as CommandError;
use super::queries::Error as QueryError;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
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

    #[error(transparent)]
    Command(#[from] CommandError),

    #[error(transparent)]
    Query(#[from] QueryError),

    #[error(transparent)]
    ParseInt(#[from] std::num::ParseIntError),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
