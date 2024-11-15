use crate::types::Lsn;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[CODE:200] checksum incorrect for 'WAL' header.")]
    IncorrectHeaderChecksum,

    #[error("[CODE:201] cannot deserialize file header for the 'WAL'. {description}")]
    CannotDeserializeFileHeader { description: String },

    #[error("[CODE:202] incorrect entry checksum for entry with LSN: {entry_lsn}. Entry: {entry}")]
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
