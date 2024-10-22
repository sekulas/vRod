pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("File storage error: {message}")]
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
