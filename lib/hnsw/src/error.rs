use std::path::PathBuf;

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("hnsw graph file has not been found in path: {path}")]
    GraphFileHasNotBeenFound { path: PathBuf },

    #[error("hnsw config file has not been found in path: {path}")]
    ConfigFileHasNotBeenFound { path: PathBuf },

    #[error("file storage error: {message}")]
    FileStorageError { message: String },

    #[error(transparent)]
    Bincode(#[from] bincode::Error),

    #[error(transparent)]
    SerdeJson(#[from] serde_json::Error),

    #[error(transparent)]
    ThreadPoolBuilder(#[from] rayon::ThreadPoolBuildError),

    #[error(transparent)]
    Io(#[from] std::io::Error),
}
