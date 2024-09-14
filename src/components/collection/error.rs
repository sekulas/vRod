use crate::{components::wal, types::Dim};

use super::index;

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
        "Provided vector has incorrect dimension. Expected: '{expected}', Actural: '{actual}'.\
    Vector: '{vector:?}'"
    )]
    InvalidVectorDim {
        expected: u16,
        actual: u16,
        vector: Vec<Dim>,
    },

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    BPTree(#[from] index::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
