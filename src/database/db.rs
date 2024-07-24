use super::{Error, Result};
use crate::components::wal::Wal;
use crate::database::DbConfig;
use std::{fs, path::Path};

pub struct Database;

impl Database {
    pub fn create(path: &Path, name: String) -> Result<()> {
        let database_dir = path.join(name);

        if database_dir.exists() {
            return Err(Error::DirectoryExists(database_dir));
        }

        fs::create_dir(&database_dir)?;

        DbConfig::create(&database_dir)?;
        Wal::create(&database_dir)?;

        println!("Database created at: {:?}", database_dir);

        Ok(())
    }
}
