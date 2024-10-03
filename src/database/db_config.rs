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
    pub db_readonly: bool,
    path: PathBuf,
    collections: Vec<CollectionMetadata>,
}

#[derive(Serialize, Deserialize)]
pub struct CollectionMetadata {
    name: String,
    is_readonly: bool,
}

impl CollectionMetadata {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
            is_readonly: false,
        }
    }
}

impl DbConfig {
    pub fn new(path: PathBuf) -> Self {
        DbConfig {
            db_readonly: false,
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
        if self.collection_exists(collection_name) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("Collection '{collection_name}' already exists in vr_config.json - cannot add it again"),
            ));
        }

        self.collections
            .push(CollectionMetadata::new(collection_name));

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
        if !self.collection_exists(collection_name) {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("Collection '{collection_name}' does not exist in vr_config.json - cannot remove it"),
            ));
        }

        self.collections.retain(|c| c.name != collection_name);

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

    pub fn set_db_as_readonly(&mut self) -> Result<(), io::Error> {
        self.db_readonly = true;

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

    pub fn set_collection_as_readonly(&mut self, collection_name: &str) -> Result<(), io::Error> {
        let collection = self.get_collection_mut(collection_name).ok_or(
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("Collection '{collection_name}' does not exist in vr_config.json - cannot set it as readonly"),
            ))?;

        collection.is_readonly = true;

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
        self.collections.iter().any(|c| c.name == collection_name)
    }

    pub fn get_collection_mut(&mut self, collection_name: &str) -> Option<&mut CollectionMetadata> {
        self.collections
            .iter_mut()
            .find(|c| c.name == collection_name)
    }

    pub fn get_collections(&self) -> Vec<String> {
        self.collections
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<String>>()
    }

    pub fn is_collection_readonly(&self, collection_name: &str) -> bool {
        self.collections
            .iter()
            .find(|c| c.name == collection_name)
            .map(|c| c.is_readonly)
            .unwrap_or(false)
    }
}
