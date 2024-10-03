use crate::types::Offset;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[CODE:600] Checksum incorrect for 'Index' header.")]
    IncorrectHeaderChecksum,
    #[error("[CODE:601] Cannot deserialize file header for the 'Index'. {description}")]
    CannotDeserializeFileHeader { description: String },

    #[error("Incorrect checksum for B+Tree node under given offset: '{offset}'")]
    IncorrectChecksum { offset: Offset },

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(&'static str),
}
