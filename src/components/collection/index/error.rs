use crate::types::Offset;
use crate::types::INDEX_FILE;
pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("checksum incorrect for file '{INDEX_FILE}' header.")]
    IncorrectHeaderChecksum,
    #[error("cannot deserialize file header for the '{INDEX_FILE}'. {description}")]
    CannotDeserializeFileHeader { description: String },

    #[error("incorrect checksum for B+Tree node under given offset: '{offset}'")]
    IncorrectChecksum { offset: Offset },

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("unexpected error: {0}")]
    Unexpected(&'static str),
}
