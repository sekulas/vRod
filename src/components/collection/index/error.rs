use crate::types::Offset;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("NodeIsFull")]
    NodeIsFull,

    #[error("Incorrect checksum for B+Tree node under given offset: '{offset}'")]
    IncorrectChecksum { offset: Offset },

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    UnexpectedError(&'static str),
}
