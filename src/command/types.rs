use crate::command::Result;
use crate::database::types::*;
use crate::database::Database;
use crate::wal::WAL;
use core::fmt;
use std::cell::RefCell;
use std::fs;
use std::rc::Rc;

//TODO Provide rollback functionality for the commands

pub trait Command {
    fn execute(&self) -> Result<()>;
}

pub struct CreateCollectionCommand {
    db: Rc<RefCell<Database>>,
    collection_name: String,
}

impl CreateCollectionCommand {
    pub fn new(db: Rc<RefCell<Database>>, collection_name: String) -> Self {
        CreateCollectionCommand {
            db,
            collection_name,
        }
    }
}

impl Command for CreateCollectionCommand {
    fn execute(&self) -> Result<()> {
        let db = self.db.borrow();
        let collection_path = db.get_database_path().join(&self.collection_name);
        let wal = db.get_wal();
        let mut wal = wal.borrow_mut();
        wal.append(self.to_string())?;

        fs::create_dir(&collection_path)?;
        WAL::create(&collection_path.join(WAL_FILE))?;
        fs::File::create(collection_path.join(STORAGE_FILE))?;
        fs::File::create(collection_path.join(ID_OFFSET_STORAGE_FILE))?;
        fs::File::create(collection_path.join(INDEX_FILE))?;

        wal.commit()?;
        println!("Success! Collection {} created.", &self.collection_name);
        Ok(())
    }
}

impl fmt::Display for CreateCollectionCommand {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "CREATE {}", self.collection_name)
    }
}

pub struct DropCollectionCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
}

impl Command for DropCollectionCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for DropCollectionCommand
        Ok(())
    }
}

pub struct ListCollectionsCommand {
    pub db: Rc<RefCell<Database>>,
}

impl Command for ListCollectionsCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for ListCollectionsCommand
        Ok(())
    }
}

pub struct TruncateWalCommand {
    pub db: Rc<RefCell<Database>>,
    pub target: Option<String>,
}

impl Command for TruncateWalCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for TruncateWalCommand
        Ok(())
    }
}

pub struct InsertCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for InsertCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for InsertCommand
        Ok(())
    }
}

pub struct BulkInsertCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for BulkInsertCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for BulkInsertCommand
        Ok(())
    }
}

pub struct UpdateCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for UpdateCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for UpdateCommand
        Ok(())
    }
}

pub struct DeleteCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for DeleteCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for DeleteCommand
        Ok(())
    }
}

pub struct SearchCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for SearchCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for SearchCommand
        Ok(())
    }
}

pub struct SearchSimilarCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for SearchSimilarCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for SearchSimilarCommand
        Ok(())
    }
}

pub struct ReindexCommand {
    pub db: Rc<RefCell<Database>>,
    pub collection_name: Option<String>,
}

impl Command for ReindexCommand {
    fn execute(&self) -> Result<()> {
        let mut db = self.db.borrow_mut();
        // Implementation for ReindexCommand
        Ok(())
    }
}

pub struct UnrecognizedCommand {
    pub command: String,
}

impl Command for UnrecognizedCommand {
    fn execute(&self) -> Result<()> {
        // Implementation for UnrecognizedCommand
        Ok(())
    }
}
