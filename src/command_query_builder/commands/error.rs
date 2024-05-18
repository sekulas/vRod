use crate::collection;
use crate::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Collection(#[from] collection::Error),

    #[error(transparent)]
    Wal(#[from] wal::Error),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
