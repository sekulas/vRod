use std::{cell::RefCell, rc::Rc};

use super::types::*;
use crate::command::{Error, Result};
use crate::database::Database;

struct CommandBuilder {
    db: Rc<RefCell<Database>>,
}

trait Builder {
    fn new(db: Rc<RefCell<Database>>) -> Self;

    fn build(
        &mut self,
        collection: Option<String>,
        command: String,
        arg: Option<String>,
    ) -> Result<Box<dyn Command>>;
}

impl Builder for CommandBuilder {
    fn new(db: Rc<RefCell<Database>>) -> Self {
        Self { db }
    }

    fn build(
        &mut self,
        collection: Option<String>,
        command: String,
        arg: Option<String>,
    ) -> Result<Box<dyn Command>> {
        let db = Rc::clone(&self.db);
        match command.to_uppercase().as_str() {
            "CREATE" => Ok(Box::new(CreateCollectionCommand {
                db,
                collection_name: arg,
            })),
            "DROP" => Ok(Box::new(DropCollectionCommand {
                db,
                collection_name: arg,
            })),
            "LISTCOLLECTIONS" => Ok(Box::new(ListCollectionsCommand { db })),
            "TRUNCATEWAL" => Ok(Box::new(TruncateWalCommand {
                db,
                target: collection, // If the target is not provided, truncate the databases WAL
            })),
            "INSERT" => Ok(Box::new(InsertCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "BULKINSERT" => Ok(Box::new(BulkInsertCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "UPDATE" => Ok(Box::new(UpdateCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "DELETE" => Ok(Box::new(DeleteCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "SEARCH" => Ok(Box::new(SearchCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "SEARCHSIMILAR" => Ok(Box::new(SearchSimilarCommand {
                db,
                collection_name: collection,
                arg,
            })),
            "REINDEX" => Ok(Box::new(ReindexCommand {
                db,
                collection_name: collection,
            })),
            _ => Err(Error::UnrecognizedCommand(command.to_string())),
        }
    }
}
