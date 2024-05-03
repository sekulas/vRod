mod error;
pub mod types;

use crate::wal::WAL;
pub use error::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};
use types::WAL_FILE;

pub struct Database {
    path: PathBuf,
    wal: WAL,
    collections: Rc<Vec<String>>,
}

impl Database {
    pub fn create(path: PathBuf, name: String) -> Result<()> {
        let database_dir = path.join(name);

        if database_dir.exists() {
            return Err(Error::DirectoryExists(database_dir));
        }

        fs::create_dir(&database_dir)?;

        WAL::create(&database_dir.join(WAL_FILE));

        Ok(())
    }

    pub fn load(path: PathBuf) -> Result<Self> {
        let wal = WAL::load(&path.join(WAL_FILE))?;
        let collections = Rc::new(Database::get_collections(&path)?);
        Ok(Database {
            path,
            wal,
            collections,
        })
    }

    pub fn get_collection_list(&self) -> Rc<Vec<String>> {
        Rc::clone(&self.collections)
    }

    pub fn get_database_path(&self) -> PathBuf {
        self.path.clone()
    }

    fn get_collections(path: &Path) -> Result<Vec<String>> {
        let entries = fs::read_dir(path)?;
        let mut dir_names = Vec::new();

        for entry in entries {
            match entry {
                Ok(entry) => {
                    if entry.file_type()?.is_dir() {
                        if let Some(dir_name) = entry.file_name().to_str() {
                            dir_names.push(dir_name.to_owned());
                        }
                    }
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }

        Ok(dir_names)
    }
}
