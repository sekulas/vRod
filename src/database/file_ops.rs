use std::{fs, path::Path};

use super::types::{CONFIG_FILE, WAL_FILE};
use crate::database::{Error, Result};

pub fn create_database(path: &Path, name: &str) -> Result<()> {
    let database_dir = path.join(name);

    if database_dir.exists() {
        return Err(Error::DirectoryExists(
            name.to_owned(),
            String::from(path.to_string_lossy()),
        ));
    }

    fs::create_dir(&database_dir)?;

    let config_file = database_dir.join(CONFIG_FILE);
    fs::File::create(config_file)?;

    let wal_file = database_dir.join(WAL_FILE);
    fs::File::create(wal_file)?;

    Ok(())
}

pub fn get_collection_list(path: &Path) -> Result<Vec<String>> {
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
