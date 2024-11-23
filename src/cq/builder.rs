use std::path::PathBuf;

use super::commands::*;
use super::parsing_ops::parse_distance;
use super::parsing_ops::parse_distance_and_vecs;
use super::parsing_ops::parse_id_and_optional_vec_payload;
use super::parsing_ops::parse_vec_n_payload;
use super::parsing_ops::parse_vecs_and_payloads_from_string;
use super::queries::*;
use super::types::CREATE_VECTOR_INDEX_C_STR;
use super::types::HANDLE_FAILED_ROLLBACK_C_STR;
use super::types::ROLLBACK_C_STR;
use super::CQTarget;
use super::CQType;
use crate::cq::parsing_ops::parse_vecs_and_payloads_from_file;
use crate::cq::types::{
    BULK_INSERT_C_STR, CREATE_C_STR, DELETE_C_STR, DROP_C_STR, INSERT_C_STR,
    LIST_COLLECTIONS_Q_STR, REINDEX_C_STR, SEARCH_ALL_Q_STR, SEARCH_Q_STR, SEARCH_SIMILAR_Q_STR,
    TRUNCATE_WAL_C_STR, UPDATE_C_STR,
};
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
            CREATE_C_STR => build_create_collection_command(target, arg),
            DROP_C_STR => build_drop_collection_command(target, arg),
            LIST_COLLECTIONS_Q_STR => build_list_collections_query(target),
            TRUNCATE_WAL_C_STR => build_truncate_wal_command(),
            INSERT_C_STR => build_insert_command(target, arg),
            SEARCH_Q_STR => build_search_query(target, arg),
            SEARCH_ALL_Q_STR => build_search_all_query(target),
            UPDATE_C_STR => build_update_command(target, arg),
            DELETE_C_STR => build_delete_command(target, arg),
            BULK_INSERT_C_STR => build_bulk_insert_command(target, arg, file_path),
            REINDEX_C_STR => build_reindex_command(target),
            ROLLBACK_C_STR => build_handle_failed_rollback_command(target),
            HANDLE_FAILED_ROLLBACK_C_STR => build_handle_failed_rollback_command(target),
            SEARCH_SIMILAR_Q_STR => build_search_simmilar_query(target, arg),
            CREATE_VECTOR_INDEX_C_STR => build_create_vector_index_command(target, arg),
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
            let (vector, payload) = parse_vec_n_payload(&data, None)?;
            let insert_command = InsertCommand::new(collection, vector, payload.to_string());
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
            println!("Parsing vectors and payloads from file...");
            let time = std::time::Instant::now();
            let vecs_and_payloads = parse_vecs_and_payloads_from_file(&file_path)?;
            println!(
                "Vectors and payloads parsed in {}s.",
                time.elapsed().as_secs_f32()
            );
            let bulk_insert_command = BulkInsertCommand::new(collection, vecs_and_payloads);
            Ok(CQType::Command(Box::new(bulk_insert_command)))
        }
        None => match arg {
            Some(arg) => {
                let vecs_and_payloads = parse_vecs_and_payloads_from_string(&arg)?;
                let bulk_insert_command = BulkInsertCommand::new(collection, vecs_and_payloads);
                Ok(CQType::Command(Box::new(bulk_insert_command)))
            } // TODO None => Err(Error::MissingArgument {
            //     description:
            //         "BULKINSERT command requires to pass either file path or vectors and payloads."
            //             .to_string(),
            // }),
            None => {
                let vecs_and_payloads = Vec::new();
                let bulk_insert_command = BulkInsertCommand::new(collection, vecs_and_payloads);
                Ok(CQType::Command(Box::new(bulk_insert_command)))
            }
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

fn build_create_vector_index_command(
    collection: CQTarget,
    distance: Option<String>,
) -> Result<CQType> {
    match distance {
        Some(distance) =>  {
            let distance = parse_distance(&distance)?;
            Ok(CQType::Command(Box::new(CreateVectorIndexCommand::new(
                collection, distance,
            ))))
        }
        None => Err(Error::MissingArgument {
            description: format!(
                "{CREATE_VECTOR_INDEX_C_STR} command requires to pass the distance for index creation.",
            ),
        }),
    }
}

fn build_handle_failed_rollback_command(target: CQTarget) -> Result<CQType> {
    Ok(CQType::Command(Box::new(HandleFailedRollbackCommand::new(
        target,
    ))))
}
