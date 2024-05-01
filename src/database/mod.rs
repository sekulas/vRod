mod error;
mod file_ops;
mod types;

use crate::wal::WAL;
pub use error::{Error, Result};
use std::path::PathBuf;
use types::WAL_FILE;

pub struct Database {
    path: PathBuf,
    wal: WAL,
    collections: Vec<String>,
}

impl Database {
    pub fn create(path: PathBuf, name: String) -> Result<()> {
        file_ops::create_database(&path, &name)?;
        Ok(())
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let wal = WAL::load(&path.join(WAL_FILE))?;
        let collections = file_ops::get_collection_list(&path)?;
        Ok(Database {
            path,
            wal,
            collections,
        })
    }
}
