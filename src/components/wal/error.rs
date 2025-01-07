use crate::types::Lsn;
use crate::types::WAL_FILE;
pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("checksum incorrect for '{WAL_FILE}' header.")]
    IncorrectHeaderChecksum,

    #[error("cannot deserialize file header for the '{WAL_FILE}'. {description}")]
    CannotDeserializeFileHeader { description: String },

    #[error("incorrect entry checksum for entry with LSN: {entry_lsn}. Entry: {entry}")]
    IncorrectEntryChecksum { entry_lsn: Lsn, entry: String },

    #[error("error while parsing wal entry to command and arg: {0}")]
    ParsingEntry(String),

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("unexpected error: {description}")]
    Unexpected { description: &'static str },
}
