use crate::types::{Offset, RecordId};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Key not found: '{key}'")]
    KeyNotFound { key: RecordId },

    #[error("Incorrect checksum for B+Tree node under given offset: '{offset}'")]
    IncorrectChecksum { offset: Offset },

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    UnexpectedError(&'static str),
}
