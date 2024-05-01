use crate::wal;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Directory with the name '{0}' already exists in '{1}'")]
    DirectoryExists(String, String),

    #[error("WAL error: {0}")]
    Wal(#[from] wal::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
}
