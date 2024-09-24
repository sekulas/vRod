use crate::{components::wal, types::Dim};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot deserialize record with the given offset: '{offset}'. Source: '{source}")]
    CannotDeserializeRecord {
        offset: u64,
        #[source]
        source: bincode::Error,
    },

    #[error(
        "Provided vector has different dimension. Expected: '{expected}', Actural: '{actual}'.\
    Vector: '{vector:?}'"
    )]
    InvalidVectorDim {
        expected: u16,
        actual: u16,
        vector: Vec<Dim>,
    },

    #[error("Incorrect checksum. Expected: '{expected}', Actual: '{actual}'")]
    IncorrectChecksum { expected: u64, actual: u64 },

    #[error("Record not found for rollback. Offset: '{offset}'")]
    RecordNotFoundForRollback { offset: u64 },

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error("Unexpected error: {0}")]
    Unexpected(&'static str),
}
