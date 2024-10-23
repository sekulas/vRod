use std::path::PathBuf;

use super::commands::*;
use super::parsing_ops::parse_distance_and_vecs;
use super::parsing_ops::parse_id_and_optional_vec_payload;
use super::parsing_ops::parse_vec_n_payload;
use super::parsing_ops::parse_vecs_and_payloads_from_file;
use super::parsing_ops::parse_vecs_and_payloads_from_string;
use super::queries::*;
use super::CQTarget;
use super::CQType;
use crate::cq::{Error, Result};
pub struct CQBuilder;

pub trait Builder {
    fn build(
        target: &CQTarget,
        cq_action: String,
        arg: Option<String>,
        file_path: Option<PathBuf>,
    ) -> Result<CQType>;
}

impl Builder for CQBuilder {
    fn build(
        target: &CQTarget,
        cq_action: String,
        arg: Option<String>,
        file_path: Option<PathBuf>,
    ) -> Result<CQType> {
        let target = (*target).clone();

        match cq_action.to_uppercase().as_str() {
            "CREATE" => build_create_collection_command(target, arg),
            "DROP" => build_drop_collection_command(target, arg),
            "LISTCOLLECTIONS" => build_list_collections_query(target),
            "TRUNCATEWAL" => build_truncate_wal_command(),
            "INSERT" => build_insert_command(target, arg),
            "SEARCH" => build_search_query(target, arg),
            "SEARCHALL" => build_search_all_query(target),
            "UPDATE" => build_update_command(target, arg),
            "DELETE" => build_delete_command(target, arg),
            "BULKINSERT" => build_bulk_insert_command(target, arg, file_path),
            "REINDEX" => build_reindex_command(target),
            "SEARCHSIMILAR" => build_search_simmilar_query(target, arg), //TODO: ### What if last command was ROLLBACK and it's uncommited? Readonly State?
            _ => Err(Error::UnrecognizedCommandOrQuery(cq_action.to_string())),
        }
    }
}

fn build_create_collection_command(
    database: CQTarget,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => Ok(CQType::Command(Box::new(CreateCollectionCommand::new(
            database, name,
        )))),
        None => Err(Error::MissingCollectionName),
    }
}

fn build_drop_collection_command(
    database: CQTarget,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => Ok(CQType::Command(Box::new(DropCollectionCommand::new(
            database, name,
        )))),
        None => Err(Error::MissingCollectionName),
    }
}

fn build_list_collections_query(database: CQTarget) -> Result<CQType> {
    Ok(CQType::Query(Box::new(ListCollectionsQuery::new(database))))
}

fn build_truncate_wal_command() -> Result<CQType> {
    Ok(CQType::Command(Box::new(TruncateWalCommand::new())))
}

fn build_insert_command(collection: CQTarget, vec_n_payload: Option<String>) -> Result<CQType> {
    match vec_n_payload {
        Some(data) => {
            let (vector, payload) = parse_vec_n_payload(&data)?;
            let insert_command = InsertCommand::new(collection, vector, payload);
            Ok(CQType::Command(Box::new(insert_command)))
        }
        None => Err(Error::MissingArgument { description: "INSERT command requires to pass vector and payload in following format '[vector];[payload]'".to_string() }),
    }
}

fn build_bulk_insert_command(
    collection: CQTarget,
    arg: Option<String>,
    file_path: Option<PathBuf>,
) -> Result<CQType> {
    if let (Some(_), Some(_)) = (&arg, &file_path) {
        println!("Provided both file_path and arg as the source. Using file path.");
    }

    match file_path {
        Some(file_path) => {
            let vecs_and_payloads = parse_vecs_and_payloads_from_file(&file_path)?;
            let bulk_insert_command = BulkInsertCommand::new(collection, vecs_and_payloads);
            Ok(CQType::Command(Box::new(bulk_insert_command)))
        }
        None => match arg {
            Some(arg) => {
                let vecs_and_payloads = parse_vecs_and_payloads_from_string(&arg)?;
                let bulk_insert_command = BulkInsertCommand::new(collection, vecs_and_payloads);
                Ok(CQType::Command(Box::new(bulk_insert_command)))
            }
            None => Err(Error::MissingArgument {
                description:
                    "BULKINSERT command requires to pass either file path or vectors and payloads."
                        .to_string(),
            }),
        },
    }
}

fn build_search_query(collection: CQTarget, record_id_str: Option<String>) -> Result<CQType> {
    match record_id_str {
        Some(record_id_str) => {
            let record_id = record_id_str.parse()?;
            let search_command = SearchQuery::new(collection, record_id);
            Ok(CQType::Query(Box::new(search_command)))
        }
        None => Err(Error::MissingArgument {
            description: "SEARCH command requires to pass record id.".to_string(),
        }),
    }
}

fn build_search_all_query(collection: CQTarget) -> Result<CQType> {
    Ok(CQType::Query(Box::new(SearchAllQuery::new(collection))))
}

fn build_update_command(collection: CQTarget, id_vec_payload: Option<String>) -> Result<CQType> {
    match id_vec_payload {
        Some(data) => {
            let (record_id, vector, payload) = parse_id_and_optional_vec_payload(&data)?;
            let update_command = UpdateCommand::new(collection, record_id, vector, payload);
            Ok(CQType::Command(Box::new(update_command)))
        }
        None => Err(Error::MissingArgument {
            description: "UPDATE command requires to pass id, embedding and payload.".to_string(),
        }),
    }
}

fn build_delete_command(collection: CQTarget, record_id_str: Option<String>) -> Result<CQType> {
    match record_id_str {
        Some(record_id_str) => {
            let record_id = record_id_str.parse()?;
            let delete_command = DeleteCommand::new(collection, record_id);
            Ok(CQType::Command(Box::new(delete_command)))
        }
        None => Err(Error::MissingArgument {
            description: "DELETE command requires to pass record id.".to_string(),
        }),
    }
}

fn build_reindex_command(collection: CQTarget) -> Result<CQType> {
    Ok(CQType::Command(Box::new(ReindexCommand::new(collection))))
}

fn build_search_simmilar_query(collection: CQTarget, args: Option<String>) -> Result<CQType> {
    match args {
        Some(args) => {
            let (distance, query_vecs) = parse_distance_and_vecs(&args)?;
            Ok(CQType::Query(Box::new(SearchSimilarQuery::new(
                collection, distance, query_vecs,
            ))))
        }
        None => Err(Error::MissingArgument {
            description: "SEARCHSIMILAR command requires to pass query vector.".to_string(),
        }),
    }
}
