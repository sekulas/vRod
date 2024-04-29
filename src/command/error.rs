pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Unrecognized command: {0}")]
    UnrecognizedCommand(String),
}
