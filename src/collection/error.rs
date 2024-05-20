use crate::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot deserialize record with the given offset '{offset}'. Source: '{source}")]
    CannotDeserializeRecord {
        offset: u64,
        #[source]
        source: bincode::Error,
    },

    #[error(transparent)]
    Serialization(#[from] bincode::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
