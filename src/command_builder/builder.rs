use std::path::Path;

use crate::command_builder::commands::*;
use crate::command_builder::{Error, Result};

use super::commands::Command;

pub struct CommandBuilder;

pub trait Builder {
    fn build(target_path: &Path, command: String, arg: Option<String>) -> Result<Box<dyn Command>>;
}

impl Builder for CommandBuilder {
    fn build(target_path: &Path, command: String, arg: Option<String>) -> Result<Box<dyn Command>> {
        match command.to_uppercase().as_str() {
            "CREATE" => build_create_collection_command(target_path, arg),
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
    target_path: &Path,
    collection_name: Option<String>,
) -> Result<Box<dyn Command>> {
    match collection_name {
        Some(name) => Ok(Box::new(CreateCollectionCommand::new(target_path, name))),
        None => Err(Error::MissingCollectionName),
    }
}
