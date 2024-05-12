use std::path::Path;

use super::commands::*;
use super::queries::*;
use super::CQType;
use crate::command_query_builder::{Error, Result};
pub struct CQBuilder;

pub trait Builder {
    fn build(target_path: &Path, cq_action: String, arg: Option<String>) -> Result<CQType>;
}

impl Builder for CQBuilder {
    fn build(target_path: &Path, cq_action: String, arg: Option<String>) -> Result<CQType> {
        match cq_action.to_uppercase().as_str() {
            "CREATE" => build_create_collection_command(target_path, arg),
            "DROP" => build_drop_collection_command(target_path, arg),
            "LISTCOLLECTIONS" => build_list_collections_query(target_path),
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
            _ => Err(Error::UnrecognizedCommand(cq_action.to_string())),
        }
    }
}

fn build_create_collection_command(
    target_path: &Path,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => Ok(CQType::Command(Box::new(CreateCollectionCommand::new(
            target_path,
            name,
        )))),
        None => Err(Error::MissingCollectionName),
    }
}

fn build_drop_collection_command(
    target_path: &Path,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => Ok(CQType::Command(Box::new(DropCollectionCommand::new(
            target_path,
            name,
        )))),
        None => Err(Error::MissingCollectionName),
    }
}

fn build_list_collections_query(target_path: &Path) -> Result<CQType> {
    Ok(CQType::Query(Box::new(ListCollectionsQuery::new(
        target_path,
    ))))
}