use crate::command::{Error, Result};
use crate::database::types::*;
use crate::database::Database;
use crate::wal::WAL;
use core::fmt;
use std::cell::RefCell;
use std::fs;
use std::path::PathBuf;

use std::rc::Rc;

//TODO Provide rollback functionality for the commands

pub trait Command {
    fn execute(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
    fn to_string(&self) -> String;
}

pub struct CreateCollectionCommand {
    path: PathBuf,
    collection_name: String,
}

impl CreateCollectionCommand {
    pub fn new(path: PathBuf, collection_name: String) -> Self {
        CreateCollectionCommand {
            path,
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
        WAL::create(&collection_path.join(WAL_FILE))?;
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
    pub db: Rc<RefCell<Database>>,
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

pub struct ListCollectionsCommand {
    pub db: Rc<RefCell<Database>>,
}

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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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
    pub db: Rc<RefCell<Database>>,
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

pub struct ReindexCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
}

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
