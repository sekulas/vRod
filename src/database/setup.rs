use std::{fs, io, path::Path};

pub fn create_database_directory(path: &Path, name: &str) -> Result<(), io::Error> {
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

    let config_file = database_dir.join("vr_config");
    fs::File::create(config_file)?;

    let wal_file = database_dir.join("vr_wal");
    fs::File::create(wal_file)?;

    Ok(())
}
