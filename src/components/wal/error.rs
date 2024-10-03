use crate::command_query_builder;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("[CODE:200] Checksum incorrect for 'WAL' header.")]
    IncorrectHeaderChecksum,

    #[error("[CODE:201] Cannot deserialize file header for the 'WAL'. {description}")]
    CannotDeserializeFileHeader { description: String },

    #[error("Error while parsing wal entry to command and arg: {0}")]
    ParsingEntry(String),

    #[error(transparent)]
    CommandBuilder(#[from] command_query_builder::Error),

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {description}")]
    Unexpected { description: &'static str },
}
