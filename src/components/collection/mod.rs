mod col;
use std::path::Path;

pub use col::*;
mod error;
pub use error::{Error, Result};
mod index;
mod storage;
pub mod types;

pub fn get_file_name_from_path(path: &Path) -> std::io::Result<String> {
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "Failed to get valid file name from path. Invalid file name",
            )
        })?
        .to_owned();
    Ok(file_name)
}
