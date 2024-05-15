use super::{Error, Result};
use crate::wal::Wal;
use crate::WAL_FILE;
use std::{fs, path::Path};

pub struct Database;

impl Database {
    pub fn create(path: &Path, name: String) -> Result<()> {
        let database_dir = path.join(name);

        if database_dir.exists() {
            return Err(Error::DirectoryExists(database_dir));
        }

        fs::create_dir(&database_dir)?;

        Wal::create(&database_dir.join(WAL_FILE))?;

        println!("Database created at: {:?}", database_dir);

        Ok(())
    }
}
