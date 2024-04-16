mod setup;
mod types;
use crate::wal::WAL;

use std::io;
use std::path::PathBuf;

pub struct Database {
    path: PathBuf,
    wal: WAL,
    //TODO collections: todo!("Implement collections"),
    //TODO wal: Wal
}

impl Database {
    pub fn new(path: PathBuf, name: String) -> Result<(), io::Error> {
        self::setup::create_database(&path, &name)?;
        Ok(())
    }

    pub fn load(path: PathBuf) -> Database {
        todo!("Load the database from the path")
    }
}
