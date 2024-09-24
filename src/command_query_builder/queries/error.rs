use crate::components::collection;
use std::{io, path::PathBuf};

pub type Result<T> = core::result::Result<T, Error>;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Cannot recognize collection name in path: {0}")]
    CollectionPathProblem(PathBuf),

    #[error("Cannot convert collection name to string: {0}")]
    CollectionNameToStrProblem(PathBuf),

    #[error(transparent)]
    Collection(#[from] collection::Error),

    #[error(transparent)]
    Io(#[from] io::Error),
}
