mod setup;

use std::io;
use std::path::PathBuf;

pub struct Database {
    path: PathBuf,
    //TODO collections: todo!("Implement collections"),
    //TODO wal: Wal
}

impl Database {
    pub fn new(path: PathBuf, name: String) -> Result<Self, io::Error> {
        self::setup::create_database_directory(&path, &name)?;

        Ok(Self { path })
    }

    pub fn load(path: PathBuf) -> Database {
        todo!("Load the database from the path")
    }
}
