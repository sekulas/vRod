mod error;
pub mod types;

use crate::wal::WAL;
pub use error::{Error, Result};
use std::{
    cell::RefCell,
    fs,
    path::{Path, PathBuf},
    rc::Rc,
};
use types::WAL_FILE;

use crate::wal::utils::wal_to_txt;

pub struct Database {
    path: PathBuf,
    wal: Rc<RefCell<WAL>>,
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
        let wal = Rc::new(RefCell::new(WAL::load(&path.join(WAL_FILE))?));
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

    pub fn get_wal(&self) -> Rc<RefCell<WAL>> {
        Rc::clone(&self.wal)
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

//TODO To remove / for developmnet only
impl Drop for Database {
    fn drop(&mut self) {
        wal_to_txt(self.path.clone().join(WAL_FILE)).unwrap_or_else(|error| {
            eprintln!(
                "Error occurred while converting WAL to text.\nWAL Path: {:?}\n{:?}",
                self.path.join(WAL_FILE),
                error
            );
        });
    }
}
