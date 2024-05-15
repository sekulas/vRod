use super::Result;
use crate::command_query_builder::{CQAction, Command};
use crate::types::{ID_OFFSET_STORAGE_FILE, INDEX_FILE, STORAGE_FILE, WAL_FILE};
use crate::wal::Wal;
use std::fs;
use std::path::{Path, PathBuf};

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
            fs::remove_dir_all(collection_path)?;
        }

        Ok(())
    }
}

impl CQAction for CreateCollectionCommand {
    fn to_string(&self) -> String {
        format!("CREATE {}", self.collection_name)
    }
}

pub struct DropCollectionCommand {
    pub path: PathBuf,
    pub collection_name: String,
}

impl DropCollectionCommand {
    pub fn new(path: &Path, collection_name: String) -> Self {
        DropCollectionCommand {
            path: path.to_owned(),
            collection_name,
        }
    }
}

impl Command for DropCollectionCommand {
    fn execute(&self) -> Result<()> {
        let collection_path = self.path.join(&self.collection_name);

        if !collection_path.exists() {
            println!("Collection {:?} has not been found.", collection_path);
            return Ok(());
        }

        fs::remove_dir_all(collection_path)?;

        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }
}

impl CQAction for DropCollectionCommand {
    fn to_string(&self) -> String {
        format!("DROP {}", self.collection_name)
    }
}

pub struct TruncateWalCommand {
    pub target_path: PathBuf,
}

impl TruncateWalCommand {
    pub fn new(target_path: &Path) -> Self {
        TruncateWalCommand {
            target_path: target_path.to_owned(),
        }
    }
}

impl Command for TruncateWalCommand {
    fn execute(&self) -> Result<()> {
        let wal_path = self.target_path.join(WAL_FILE);

        fs::remove_file(&wal_path)?;

        Wal::create(&wal_path)?;

        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        Ok(())
    }
}

impl CQAction for TruncateWalCommand {
    fn to_string(&self) -> String {
        "TRUNCATEWAL".to_string()
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
}

impl CQAction for InsertCommand {
    fn to_string(&self) -> String {
        todo!();
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
}

impl CQAction for BulkInsertCommand {
    fn to_string(&self) -> String {
        todo!();
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
}

impl CQAction for UpdateCommand {
    fn to_string(&self) -> String {
        todo!();
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
}

impl CQAction for DeleteCommand {
    fn to_string(&self) -> String {
        todo!();
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
}

impl CQAction for ReindexCommand {
    fn to_string(&self) -> String {
        todo!();
    }
}
