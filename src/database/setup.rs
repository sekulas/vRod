use std::{fs, io, path::Path};

use super::types::{CONFIG_FILE, WAL_FILE};

pub fn create_database(path: &Path, name: &str) -> Result<(), io::Error> {
    let database_dir = path.join(name);

    if database_dir.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!(
                "Directory with the name '{}' already exists in '{}'",
                name,
                path.display()
            ),
        ));
    }

    fs::create_dir(&database_dir)?;

    let config_file = database_dir.join(CONFIG_FILE);
    fs::File::create(config_file)?;

    let wal_file = database_dir.join(WAL_FILE);
    fs::File::create(wal_file)?;

    Ok(())
}
