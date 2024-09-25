use std::path::Path;
use std::path::PathBuf;

use super::commands::*;
use super::parsing_ops::parse_id_and_optional_vec_payload;
use super::parsing_ops::parse_vec_n_payload;
use super::parsing_ops::parse_vecs_and_payloads_from_file;
use super::parsing_ops::parse_vecs_and_payloads_from_string;
use super::queries::*;
use super::CQType;
use crate::command_query_builder::{Error, Result};
use crate::components::collection::*;
use crate::database::DbConfig;
use crate::types::DB_CONFIG;
pub struct CQBuilder;

pub trait Builder {
    fn build(target_path: &Path, cq_action: String, arg: Option<String>, file_path: Option<PathBuf>) -> Result<CQType>;
}

impl Builder for CQBuilder {
    fn build(target_path: &Path, cq_action: String, arg: Option<String>, file_path: Option<PathBuf>) -> Result<CQType> {
        match cq_action.to_uppercase().as_str() {
            "CREATE" => build_create_collection_command(target_path, arg),
            "DROP" => build_drop_collection_command(target_path, arg),
            "LISTCOLLECTIONS" => build_list_collections_query(target_path),
            "TRUNCATEWAL" => build_truncate_wal_command(target_path),
            "INSERT" => build_insert_command(target_path, arg),
            "SEARCH" => build_search_query(target_path, arg),
            "SEARCHALL" => build_search_all_query(target_path),
            "UPDATE" => build_update_command(target_path, arg),
            "DELETE" => build_delete_command(target_path, arg),
            "BULKINSERT" => build_bulk_insert_command(target_path, arg, file_path),
            "REINDEX" => build_reindex_command(target_path),
            "SEARCHSIMILAR" => todo!("NOT IMPLEMENTED search similar"),
            _ => Err(Error::UnrecognizedCommandOrQuery(cq_action.to_string())),
        }
    }
}

fn collection_exists(database_path: &Path, collection_name: &str) -> Result<bool> {
    let db_config = DbConfig::load(&database_path.join(DB_CONFIG))?;
    Ok(db_config.collection_exists(collection_name))
}

fn build_create_collection_command(
    database_path: &Path,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => match collection_exists(database_path, &name)? {
            true => Err(Error::CollectionAlreadyExists {
                collection_name: name,
            }),
            false => Ok(CQType::Command(Box::new(CreateCollectionCommand::new(
                database_path,
                name,
            )))),
        },
        None => Err(Error::MissingCollectionName),
    }
}

fn build_drop_collection_command(
    database_path: &Path,
    collection_name: Option<String>,
) -> Result<CQType> {
    match collection_name {
        Some(name) => match collection_exists(database_path, &name)? {
            true => Ok(CQType::Command(Box::new(DropCollectionCommand::new(
                database_path,
                name,
            )))),
            false => Err(Error::CollectionDoesNotExist {
                collection_name: name,
            }),
        },
        None => Err(Error::MissingCollectionName),
    }
}

fn build_list_collections_query(target_path: &Path) -> Result<CQType> {
    Ok(CQType::Query(Box::new(ListCollectionsQuery::new(
        target_path,
    ))))
}

fn build_truncate_wal_command(target_path: &Path) -> Result<CQType> {
    Ok(CQType::Command(Box::new(TruncateWalCommand::new(
        target_path,
    ))))
}

fn build_insert_command(target_path: &Path, vec_n_payload: Option<String>) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

    match vec_n_payload {
        Some(data) => {
            let (vector, payload) = parse_vec_n_payload(&data)?;
            let insert_command = InsertCommand::new(collection, vector, payload);
            Ok(CQType::Command(Box::new(insert_command)))
        }
        None => Err(Error::MissingArgument { description: "INSERT command requires to pass vector and payload in following format '[vector];[payload]'".to_string() }),
    }
}

fn build_bulk_insert_command(target_path: &Path, arg: Option<String>, file_path: Option<PathBuf>) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

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
                description: "BULKINSERT command requires to pass either file path or vectors and payloads.".to_string(),
            }),
        },
    }
}

fn build_search_query(target_path: &Path, record_id_str: Option<String>) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

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

fn build_search_all_query(target_path: &Path) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

    Ok(CQType::Query(Box::new(SearchAllQuery::new(collection))))
}

fn build_update_command(target_path: &Path, id_vec_payload: Option<String>) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name }); //TODO: Is that trully needed?
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

    match id_vec_payload {
        Some(data) => {
            let (record_id, vector, payload) = parse_id_and_optional_vec_payload(&data)?;
            let update_command = UpdateCommand::new(collection, record_id, vector, payload);
            Ok(CQType::Command(Box::new(update_command)))
        }
        None => Err(Error::MissingArgument{ 
            description: "UPDATE command requires to pass id, embedding and payload in following format '<record_id>;[vector];[payload].".to_string()}),
    }
}

fn build_delete_command(target_path: &Path, record_id_str: Option<String>) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

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

fn build_reindex_command(target_path: &Path) -> Result<CQType> {
    let collection_name = target_path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned());
    let database_path = target_path.parent().map(|path| path.to_path_buf());

    if database_path.is_none() || collection_name.is_none() {
        return Err(Error::CannotDetermineCollectionPath {
            database_path,
            collection_name,
        });
    }

    let database_path = database_path.unwrap();
    let collection_name = collection_name.unwrap();

    if !collection_exists(&database_path, &collection_name)? {
        return Err(Error::CollectionDoesNotExist { collection_name });
    }

    let collection = Collection::load(target_path).map_err(|e| Error::Collection {
        description: e.to_string(),
    })?;

    Ok(CQType::Command(Box::new(ReindexCommand::new(collection))))
}