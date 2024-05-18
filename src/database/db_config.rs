use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::types::DB_CONFIG;
use std::io::Write;

#[derive(Serialize, Deserialize)]
pub struct DbConfig {
    path: PathBuf,
    collections: Vec<String>,
}

impl DbConfig {
    pub fn new(path: PathBuf) -> Self {
        DbConfig {
            path,
            collections: Vec::new(),
        }
    }

    pub fn create(path: &Path) -> Result<(), io::Error> {
        let file_path = path.join(DB_CONFIG);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        serde_json::to_writer(file, &DbConfig::new(file_path))?;

        Ok(())
    }

    pub fn load(path: &Path) -> Result<Self, io::Error> {
        let file = OpenOptions::new().read(true).open(path)?;

        let config: DbConfig = serde_json::from_reader(file)?;

        Ok(config)
    }

    pub fn add_collection(&mut self, collection_name: &str) -> Result<(), io::Error> {
        if self.collections.contains(&collection_name.to_owned()) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Collection '{collection_name}' already exists in vr_config.json - cannot add it again"),
            ));
        }

        self.collections.push(collection_name.to_owned());

        let temp_path = self.path.with_extension("tmp");

        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&temp_path)?;

        let json = serde_json::to_string_pretty(&self)?;
        write!(file, "{}", json)?;

        fs::rename(&temp_path, &self.path)?;

        Ok(())
    }

    pub fn remove_collection(&mut self, collection_name: &str) -> Result<(), io::Error> {
        if !self.collections.contains(&collection_name.to_owned()) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Collection '{collection_name}' does not exist in vr_config.json - cannot remove it"),
            ));
        }

        self.collections.retain(|c| c != collection_name);

        let temp_path = self.path.with_extension("tmp");

        let mut file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&temp_path)?;

        let json = serde_json::to_string_pretty(&self)?;
        write!(file, "{}", json)?;

        fs::rename(&temp_path, &self.path)?;

        Ok(())
    }

    pub fn collection_exists(&self, collection_name: &str) -> bool {
        self.collections.contains(&collection_name.to_owned())
    }
}
