use super::types::Command;
use super::{Error, Result};
use crate::types::{ID_OFFSET_STORAGE_FILE, INDEX_FILE, STORAGE_FILE, WAL_FILE};
use crate::wal::Wal;
use std::fs;
use std::path::{Path, PathBuf};

//TODO Provide rollback functionality for the commands

pub struct CreateCollectionCommand {
    path: PathBuf,
    collection_name: String,
}

impl CreateCollectionCommand {
    pub fn new(path: &Path, collection_name: String) -> Self {
        CreateCollectionCommand {
            path: path.to_owned(),
            collection_name,
        }
    }
}

impl Command for CreateCollectionCommand {
    fn execute(&self) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);

        if collection_path.exists() {
            return Err(Error::CollectionExists(self.collection_name.to_owned()));
        }

        fs::create_dir(&collection_path)?;
        Wal::create(&collection_path.join(WAL_FILE))?;
        fs::File::create(collection_path.join(STORAGE_FILE))?;
        fs::File::create(collection_path.join(ID_OFFSET_STORAGE_FILE))?;
        fs::File::create(collection_path.join(INDEX_FILE))?;

        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);

        if collection_path.exists() {
            fs::remove_dir(collection_path)?;
        }

        Ok(())
    }

    fn to_string(&self) -> String {
        format!("CREATE {}", self.collection_name)
    }
}

pub struct DropCollectionCommand {
    pub collection_name: Option<String>,
}

impl Command for DropCollectionCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct ListCollectionsCommand {}

impl Command for ListCollectionsCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct TruncateWalCommand {
    pub target: Option<String>,
}

impl Command for TruncateWalCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct InsertCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for InsertCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct BulkInsertCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for BulkInsertCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct UpdateCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for UpdateCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct DeleteCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for DeleteCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct SearchCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for SearchCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct SearchSimilarCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for SearchSimilarCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct ReindexCommand {}

impl Command for ReindexCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}

pub struct UnrecognizedCommand {
    pub command: String,
}

impl Command for UnrecognizedCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn to_string(&self) -> String {
        todo!("Not implemented.")
    }
}
