use std::{cell::RefCell, rc::Rc};

use super::types::*;
use crate::command::{Error, Result};
use crate::database::Database;

pub struct CommandBuilder {
    db: Rc<RefCell<Database>>,
}

pub trait Builder {
    fn new(db: Rc<RefCell<Database>>) -> Self;

    fn build(
        &self,
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
        &self,
        collection: Option<String>,
        command: String,
        arg: Option<String>,
    ) -> Result<Box<dyn Command>> {
        let db = Rc::clone(&self.db);
        match command.to_uppercase().as_str() {
            "CREATE" => build_create_collection_command(db, arg),
            "DROP" => todo!(),
            /* Ok(Box::new(DropCollectionCommand {
                db,
                collection_name: arg,
            })) */
            "LISTCOLLECTIONS" => todo!(),
            /* Ok(Box::new(ListCollectionsCommand { db })) */
            "TRUNCATEWAL" => todo!(),
            /* Ok(Box::new(TruncateWalCommand {
                db,
                target: collection, // If the target is not provided, truncate the databases WAL
            })) */
            "INSERT" => todo!(),
            /* Ok(Box::new(InsertCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "BULKINSERT" => todo!(),
            /* Ok(Box::new(BulkInsertCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "UPDATE" => todo!(),
            /* Ok(Box::new(UpdateCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "DELETE" => todo!(),
            /* Ok(Box::new(DeleteCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "SEARCH" => todo!(),
            /* Ok(Box::new(SearchCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "SEARCHSIMILAR" => todo!(),
            /* Ok(Box::new(SearchSimilarCommand {
                db,
                collection_name: collection,
                arg,
            })) */
            "REINDEX" => todo!(),
            /* Ok(Box::new(ReindexCommand {
                db,
                collection_name: collection,
            })) */
            _ => Err(Error::UnrecognizedCommand(command.to_string())),
        }
    }
}

fn build_create_collection_command(
    db: Rc<RefCell<Database>>,
    collection_name: Option<String>,
) -> Result<Box<dyn Command>> {
    match collection_name {
        Some(name) => {
            let collection_list = db.borrow().get_collection_list();
            let exists = collection_list.iter().find(|&x| x == &name);

            match exists {
                Some(_) => Err(Error::CollectionExists(name)),
                None => Ok(Box::new(CreateCollectionCommand::new(db, name))),
            }
        }

        None => Err(Error::MissingCollectionName),
    }
}
