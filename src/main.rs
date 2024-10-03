mod command_query_builder;
mod components;
mod database;
mod error;
mod types;
mod utils;

use crate::error::{Error, Result};
use crate::types::WAL_FILE;
use clap::Parser;
use command_query_builder::{Builder, CQBuilder, CQType, Command};
use components::wal::{utils::wal_to_txt, Wal, WalType};
use database::{Database, DbConfig};
use std::path::{Path, PathBuf};
use types::{CQTarget, DB_CONFIG};
use utils::embeddings::process_embeddings;

#[derive(Parser)]
#[command(arg_required_else_help(true))]
struct Args {
    #[arg(short, long, value_name = "PATH")]
    init_database: Option<PathBuf>,

    #[arg(short = 'n', long, value_name = "NAME")]
    init_database_name: Option<String>,

    #[arg(short, long, value_name = "PATH")]
    database: Option<PathBuf>,

    #[arg(short, long, value_name = "COLLECTION_NAME")]
    collection: Option<String>,

    #[arg(short, long, value_name = "COMMAND")]
    execute: Option<String>,

    #[arg(short = 'a', long, value_name = "COMMAND_ARG")]
    command_arg: Option<String>,

    #[arg(short = 'f', long, value_name = "PATH")]
    file_path: Option<PathBuf>,

    //TODO To remove / for developmnet only
    #[arg(short, long, value_name = "AMOUNT")]
    generate_embeddings: Option<usize>,

    #[arg(short, long, value_name = "PATH")]
    wal_path: Option<PathBuf>,
}

fn main() {
    match run() {
        Ok(_) => {}
        Err(e) => {
            eprintln!("ERROR: {:?}: {}", e, e);
            std::process::exit(1);
        }
    }
}

fn run() -> Result<()> {
    let args = Args::parse();

    //TODO To remove / for developmnet only
    if let Some(amount) = args.generate_embeddings {
        process_embeddings(amount)?;
        return Ok(());
    }

    //TODO To remove / for developmnet only
    if let Some(wal_path) = args.wal_path {
        wal_to_txt(&wal_path).unwrap_or_else(|error| {
            eprintln!(
                "Error occurred while converting WAL to text.\nWAL Path: {:?}\n{:?}",
                wal_path, error
            );
        });
        return Ok(());
    }

    match (args.init_database, args.init_database_name) {
        (Some(database_path), Some(database_name)) => {
            return Ok(Database::create(&database_path, database_name)?);
        }
        (Some(_), None) => return Err(Error::MissingInitDatabaseName),
        _ => {}
    }

    let command_text = args.execute.ok_or(Error::MissingCommand)?;
    let (target, is_readonly) = specify_target(args.database, args.collection)?;
    let target_path = get_target_path(&target);

    let result: Result<()> = (|| {
        let cq_action =
            CQBuilder::build(&target_path, command_text, args.command_arg, args.file_path)?;
        verify_if_command_not_run_on_readonly_target(&cq_action, is_readonly)?; //TODO: ### Is that needed - deserialize header error during build

        let wal_type = Wal::load(&target_path.join(WAL_FILE))?;

        match wal_type {
            WalType::Consistent(wal) => {
                execute_cq_action(cq_action, wal)?;
                Ok(())
            }
            WalType::Uncommited {
                mut wal,
                uncommited_command,
                arg,
            } => {
                redo_last_command(&target_path, &mut wal, uncommited_command, arg, None)?;
                execute_cq_action(cq_action, wal)?;
                Ok(())
            }
        }
    })();

    match result {
        Ok(_) => Ok(()),
        Err(e) => handle_db_error(e, &target),
    }
}

fn execute_cq_action(cq_action: CQType, mut wal: Wal) -> Result<()> {
    match cq_action {
        CQType::Command(command) => {
            println!("Executing command: {:?}", command.to_string());
            execute_command(&mut wal, command)?
        }
        CQType::Query(mut query) => {
            println!("Executing query: {:?}", query.to_string());
            query.execute()?
        }
    };
    Ok(())
}

fn redo_last_command(
    target_path: &Path,
    wal: &mut Wal,
    command: String,
    arg: Option<String>,
    file_path: Option<PathBuf>,
) -> Result<()> {
    if let CQType::Command(mut last_command) =
        CQBuilder::build(target_path, command, arg, file_path)?
    {
        let stringified_last_command = last_command.to_string();
        println!("Redoing last command: {:?}", stringified_last_command);

        let lsn = wal.append(format!("ROLLBACK {stringified_last_command}"))?;
        last_command.rollback(lsn)?;
        wal.commit()?;

        //TODO: ### Isn't REDO too much dangerous? Won't it be better to rollback and give the information
        //about not performed command?
        execute_command(wal, last_command)?;
    }
    Ok(())
}

fn execute_command(wal: &mut Wal, mut command: Box<dyn Command>) -> Result<()> {
    let lsn = wal.append(command.to_string())?;
    command.execute(lsn)?;
    wal.commit()?;
    Ok(())
}

fn specify_target(
    database_path: Option<PathBuf>,
    collection_name: Option<String>,
) -> Result<(CQTarget, bool)> {
    let database_path = get_database_path(database_path)?;
    let db_config = DbConfig::load(&database_path.join(DB_CONFIG))?;

    let (target_path, is_readonly) = match collection_name {
        Some(collection_name) => {
            verify_if_collection_exists(&db_config, &collection_name)?;
            let is_readonly = db_config.is_collection_readonly(&collection_name);
            (
                CQTarget::Collection {
                    database_path,
                    collection_name,
                },
                is_readonly,
            )
        }

        None => (CQTarget::Database { database_path }, db_config.db_readonly),
    };

    Ok((target_path, is_readonly))
}

fn get_target_path(target: &CQTarget) -> PathBuf {
    match target {
        CQTarget::Database { database_path } => database_path.clone(),
        CQTarget::Collection {
            database_path,
            collection_name,
        } => database_path.join(collection_name),
    }
}

fn get_database_path(path: Option<PathBuf>) -> Result<PathBuf> {
    let path = match path {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    match path.join(DB_CONFIG).exists() {
        true => Ok(path),
        false => Err(Error::DatabaseDoesNotExist(
            path.to_string_lossy().to_string(),
        )),
    }
}

fn verify_if_collection_exists(db_config: &DbConfig, collection_name: &str) -> Result<()> {
    match db_config.collection_exists(collection_name) {
        true => Ok(()),
        false => Err(Error::CollectionDoesNotExist(collection_name.to_string())),
    }
}

fn verify_if_command_not_run_on_readonly_target(
    cq_action: &CQType,
    is_readonly: bool,
) -> Result<()> {
    if let CQType::Command(_) = cq_action {
        if is_readonly {
            return Err(Error::TargetIsReadonly);
        }
    }

    Ok(())
}

fn handle_db_error(e: Error, target: &CQTarget) -> Result<()> {
    let err_str = e.to_string();

    if let Some(error_code) = parse_error_code(&err_str) {
        set_target_as_readonly_if_needed(error_code, target)?;
    }

    Err(e)
}

fn set_target_as_readonly_if_needed(error_code: u16, target: &CQTarget) -> Result<()> {
    match target {
        CQTarget::Collection {
            database_path,
            collection_name,
        } => {
            if [200, 201, 202, 500, 501, 600, 601].contains(&error_code) {
                let mut db_config = DbConfig::load(&database_path.join(DB_CONFIG))?;
                db_config.set_collection_as_readonly(collection_name)?;
                eprintln!(
                    "Collection: '{}' set as readonly due to error.",
                    collection_name
                );
            }
            Ok(())
        }

        CQTarget::Database { database_path } => {
            if [200, 201, 202].contains(&error_code) {
                let mut db_config = DbConfig::load(&database_path.join(DB_CONFIG))?;
                db_config.set_db_as_readonly()?;
                eprintln!("Database set as readonly due to error.");
            }
            Ok(())
        }
    }
}

fn parse_error_code(err_str: &str) -> Option<u16> {
    if let Some(code_part) = err_str.split("[CODE:").nth(1) {
        if let Some(code_str) = code_part.split(']').next() {
            if let Ok(code) = code_str.parse::<u16>() {
                return Some(code);
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::{assert::Assert, Command};
    use command_query_builder::parsing_ops::{
        parse_vec_n_payload, EXPECTED_2_ARG_FORMAT_ERR_M, EXPECTED_3_ARG_FORMAT_ERR_M,
        NO_RECORD_ID_PROVIDED_ERR_M,
    };
    use predicates::prelude::PredicateBooleanExt;
    use types::{INDEX_FILE, STORAGE_FILE};
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;
    const BINARY: &str = "vrod";

    fn is_wal_consistent(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        col_name: Option<&str>,
    ) -> Result<bool> {
        let mut wal_path = temp_dir.path().join(db_name);
        if let Some(col) = col_name {
            wal_path = wal_path.join(col);
        }
        let wal = Wal::load(&wal_path.join(WAL_FILE))?;
        if let WalType::Uncommited { .. } = wal {
            return Ok(false);
        }
        Ok(true)
    }

    fn init_database(temp_dir: &tempfile::TempDir, db_name: &str) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--init-database")
            .arg(temp_dir.path())
            .arg("--init-database-name")
            .arg(db_name)
            .assert();
        Ok(result)
    }

    fn create_collection(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("CREATE")
            .arg("--command-arg")
            .arg(collection_name)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .assert();
        Ok(result)
    }

    fn drop_collection(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("DROP")
            .arg("--command-arg")
            .arg(collection_name)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .assert();
        Ok(result)
    }

    fn list_collections(temp_dir: &tempfile::TempDir, db_name: &str) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("LISTCOLLECTIONS")
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .assert();
        Ok(result)
    }

    fn insert(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        data: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("INSERT")
            .arg("--command-arg")
            .arg(data)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn bulk_insert_arg(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        data: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("BULKINSERT")
            .arg("--command-arg")
            .arg(data)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn bulk_insert_file(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        file_path: PathBuf,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("BULKINSERT")
            .arg("--file-path")
            .arg(file_path)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn search(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        data: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("SEARCH")
            .arg("--command-arg")
            .arg(data)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn search_all(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("SEARCHALL")
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn update(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        data: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("UPDATE")
            .arg("--command-arg")
            .arg(data)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn delete(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
        data: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("DELETE")
            .arg("--command-arg")
            .arg(data)
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn reindex(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: &str,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("REINDEX")
            .arg("--database")
            .arg(temp_dir.path().join(db_name))
            .arg("--collection")
            .arg(collection_name)
            .assert();
        Ok(result)
    }

    fn truncatewal(
        temp_dir: &tempfile::TempDir,
        db_name: &str,
        collection_name: Option<&str>,
    ) -> Result<Assert> {
        let mut cmd = Command::cargo_bin(BINARY)?;
        let result = cmd
            .arg("--execute")
            .arg("TRUNCATEWAL")
            .arg("--database")
            .arg(temp_dir.path().join(db_name));

        if let Some(collection_name) = collection_name {
            result.arg("--collection").arg(collection_name);
        }

        Ok(result.assert())
    }

    #[test]
    fn init_database_success() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";

        //Act
        let result = init_database(&temp_dir, db_name)?;

        //Assert
        result.success();

        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists());
        assert!(db_path.join(DB_CONFIG).exists());
        Ok(())
    }

    #[test]
    fn init_database_fail_when_db_already_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        init_database(&temp_dir, db_name)?;

        //Act
        let result = init_database(&temp_dir, db_name)?;

        //Assert
        let specified_path_str = temp_dir.path().join(db_name);

        result.failure().stderr(predicates::str::contains(
            database::Error::DirectoryExists(specified_path_str).to_string(),
        ));

        Ok(())
    }

    #[test]
    fn init_database_missing_name() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut cmd = Command::cargo_bin(BINARY)?;

        //Act
        let result = cmd.arg("--init-database").arg(temp_dir.path()).assert();

        //Assert
        result.failure().stderr(predicates::str::contains(
            Error::MissingInitDatabaseName.to_string(),
        ));

        Ok(())
    }

    #[test]
    fn create_should_create_collection() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;

        //Act
        let result = create_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.success();
        let db_path = temp_dir.path().join(db_name);
        let collection_path = db_path.join(collection_name);

        assert!(collection_path.exists());
        assert!(collection_path.join(WAL_FILE).exists());
        assert!(collection_path.join(STORAGE_FILE).exists());
        assert!(collection_path.join(INDEX_FILE).exists());

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn create_should_fail_when_collection_already_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = create_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            command_query_builder::Error::CollectionAlreadyExists {
                collection_name: collection_name.to_owned(),
            }
            .to_string(),
        ));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn create_should_fail_when_database_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "non_existent_db";
        let collection_name = "test_col";

        //Act
        let result = create_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        let specified_path_str = temp_dir
            .path()
            .join("non_existent_db")
            .to_string_lossy()
            .to_string();

        result.failure().stderr(predicates::str::contains(
            Error::DatabaseDoesNotExist(specified_path_str).to_string(),
        ));

        Ok(())
    }

    #[test]
    fn drop_should_remove_collection() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = drop_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.success();
        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists());

        let collection_path = db_path.join(collection_name);
        assert!(!collection_path.exists());

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn drop_should_fail_when_collection_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;

        //Act
        let result = drop_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            command_query_builder::Error::CollectionDoesNotExist {
                collection_name: collection_name.to_owned(),
            }
            .to_string(),
        ));

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn drop_does_not_drop_col_twice() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        drop_collection(&temp_dir, db_name, collection_name)?;
        let result = drop_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            command_query_builder::Error::CollectionDoesNotExist {
                collection_name: collection_name.to_owned(),
            }
            .to_string(),
        ));

        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists());

        let collection_path = db_path.join(collection_name);
        assert!(!collection_path.exists());

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn create_should_create_collection_after_dropping() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;

        //Act
        create_collection(&temp_dir, db_name, collection_name)?;
        drop_collection(&temp_dir, db_name, collection_name)?;
        let result = create_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result.success();

        let db_path = temp_dir.path().join(db_name);
        assert!(db_path.exists());

        let collection_path = db_path.join(collection_name);
        assert!(collection_path.exists());

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn list_collections_should_return_empty_when_no_collections_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        init_database(&temp_dir, db_name)?;

        //Act
        let result = list_collections(&temp_dir, db_name)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("No collections found."));

        Ok(())
    }

    #[test]
    fn list_collections_should_return_all_collections() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let collection_name2 = "test_col2";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        create_collection(&temp_dir, db_name, collection_name2)?;

        //Act
        let result = list_collections(&temp_dir, db_name)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains(collection_name))
            .stdout(predicates::str::contains(collection_name2));

        Ok(())
    }

    #[test]
    fn list_collections_should_not_return_deleted_collections() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col_to_delete";
        let collection_name2 = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        create_collection(&temp_dir, db_name, collection_name2)?;
        drop_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = list_collections(&temp_dir, db_name)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains(collection_name2))
            .stdout(predicates::str::contains(collection_name).not());

        Ok(())
    }

    #[test]
    fn insert_should_store_embedding_in_collection() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn insert_should_not_insert_vec_with_different_dimensions() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";
        let incorrect_data = "1.0,2.0,3.0,4.0;test_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = insert(&temp_dir, db_name, collection_name, incorrect_data)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("different dimension"));

        Ok(())
    }

    #[test]
    fn bulk_insert_should_insert_multiple_embeddings_from_file() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let file_path = "test_data.txt";
        let inserted_data = "1.0,2.0,3.0;test_payload";
        let inserted_data_2 = "4.0,5.0,6.0;test_payload_2";
        let file_content = format!("{}\n{}\n", inserted_data, inserted_data_2);

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        let file_path = temp_dir.path().join(file_path);
        std::fs::write(&file_path, file_content)?;

        //Act
        let result = bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let result = search(&temp_dir, db_name, collection_name, "1")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("1.0, 2.0, 3.0"))
            .stdout(predicates::str::contains("test_payload"));

        let result = search(&temp_dir, db_name, collection_name, "2")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("4.0, 5.0, 6.0"))
            .stdout(predicates::str::contains("test_payload_2"));

        Ok(())
    }

    #[test]
    fn bulk_insert_should_insert_multiple_embeddings_from_arg() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";
        let inserted_data_2 = "4.0,5.0,6.0;test_payload_2";
        let data = format!("{} {}", inserted_data, inserted_data_2);

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = bulk_insert_arg(&temp_dir, db_name, collection_name, &data)?;

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let result = search(&temp_dir, db_name, collection_name, "1")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("1.0, 2.0, 3.0"))
            .stdout(predicates::str::contains("test_payload"));

        let result = search(&temp_dir, db_name, collection_name, "2")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("4.0, 5.0, 6.0"))
            .stdout(predicates::str::contains("test_payload_2"));

        Ok(())
    }

    #[test]
    fn bulk_insert_should_fail_when_incorrect_data_format() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let incorrect_data = "1.0,2.0,3.0;test_payload;extra_data";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = bulk_insert_arg(&temp_dir, db_name, collection_name, incorrect_data)?;

        //Assert
        result
            .failure()
            .stderr(predicates::str::contains(EXPECTED_2_ARG_FORMAT_ERR_M));

        Ok(())
    }

    #[test]
    fn search_should_return_embedding_when_it_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";
        let (expected_vector, expected_payload) = parse_vec_n_payload(inserted_data)?;
        let expected_record_id = "1";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = search(&temp_dir, db_name, collection_name, expected_record_id)?;

        //Assert
        let result = result.success();
        result
            .stdout(predicates::str::contains(format!("{:?}", expected_vector)))
            .stdout(predicates::str::contains(expected_payload));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn search_should_fail_when_embedding_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = search(&temp_dir, db_name, collection_name, "2")?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("not found"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn search_should_fail_when_collection_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "non_existent_col";
        let inserted_data = "1.0,2.0,3.0;test_payload";

        init_database(&temp_dir, db_name)?;

        //Act
        let result = search(&temp_dir, db_name, collection_name, inserted_data)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            Error::CollectionDoesNotExist(collection_name.to_owned()).to_string(),
        ));

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn search_should_fail_when_database_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "non_existent_db";

        //Act
        let result = search(&temp_dir, db_name, "non_existent_col", "1")?;

        //Assert
        let specified_path_str = temp_dir.path().join(db_name).to_string_lossy().to_string();

        result.failure().stderr(predicates::str::contains(
            Error::DatabaseDoesNotExist(specified_path_str).to_string(),
        ));

        Ok(())
    }

    #[test]
    fn search_all_should_return_all_embeddings_from_collection() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let inserted_data_2 = "4.0,5.0,6.0;payload2";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;
        insert(&temp_dir, db_name, collection_name, inserted_data_2)?;

        //Act
        let result = search_all(&temp_dir, db_name, collection_name)?;

        //Assert
        let result = result.success();
        result
            .stdout(predicates::str::contains("1.0, 2.0, 3.0"))
            .stdout(predicates::str::contains("payload"))
            .stdout(predicates::str::contains("4.0, 5.0, 6.0"))
            .stdout(predicates::str::contains("payload2"));
        Ok(())
    }

    #[test]
    fn update_should_update_record_when_it_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "1;4.0,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result.success();

        let result = search(&temp_dir, db_name, collection_name, "1")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("4.0, 5.0, 6.0"))
            .stdout(predicates::str::contains("updated_payload"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn update_should_not_update_when_record_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let update_arg = "2;4.0,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("not found"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn update_should_update_when_only_id_and_payload_provided() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "1;;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result.success();

        let result = search(&temp_dir, db_name, collection_name, "1")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("1.0, 2.0, 3.0"))
            .stdout(predicates::str::contains("updated_payload"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn update_should_update_when_only_id_and_vec_provided() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "1;4.0,5.0,6.0;";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result.success();

        let result = search(&temp_dir, db_name, collection_name, "1")?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("4.0, 5.0, 6.0"))
            .stdout(predicates::str::contains("payload"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    //TODO: ### Czy testy wygenerowane przez Copilota to problem? Jeden napisałem ,a potem generowały się same.
    #[test]
    fn update_should_not_update_record_2_args_provided() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "4.0,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            command_query_builder::Error::InvalidDataFormat {
                description: EXPECTED_3_ARG_FORMAT_ERR_M.to_owned(),
            }
            .to_string(),
        ));

        Ok(())
    }

    #[test]
    fn update_should_not_update_if_id_has_not_been_provided() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = ";4.0,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result.failure().stderr(predicates::str::contains(
            command_query_builder::Error::InvalidDataFormat {
                description: NO_RECORD_ID_PROVIDED_ERR_M.to_owned(),
            }
            .to_string(),
        ));

        Ok(())
    }

    #[test]
    fn update_should_not_update_if_id_is_not_a_number() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "a;4.0,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result
            .failure()
            .stderr(predicates::str::contains("ParseIntError"));

        Ok(())
    }

    #[test]
    fn update_should_not_update_if_vector_has_letters() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "1;a,5.0,6.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result
            .failure()
            .stderr(predicates::str::contains("ParseFloatError"));

        Ok(())
    }

    #[test]
    fn update_should_not_update_if_different_vec_dimension_provided() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let update_arg = "1;4.0,5.0;updated_payload";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = update(&temp_dir, db_name, collection_name, update_arg)?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("different dimension"));

        Ok(())
    }

    #[test]
    fn delete_should_remove_record_when_it_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        let inserted_data = "1.0,2.0,3.0;payload";
        let record_id = "1";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, inserted_data)?;

        //Act
        let result = delete(&temp_dir, db_name, collection_name, record_id)?;

        //Assert
        result.success();

        let query_result = search(&temp_dir, db_name, collection_name, record_id)?;
        query_result
            .success()
            .stdout(predicates::str::contains("not found"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn delete_should_not_delete_deleted_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = delete(&temp_dir, db_name, collection_name, "10")?;

        //Assert
        result
            .success()
            .stdout(predicates::str::contains("not found"));

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    #[test]
    fn reindex_should_remove_deleted_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, "1.0,2.0,3.0;payload")?;
        insert(&temp_dir, db_name, collection_name, "4.0,5.0,6.0;payload2")?;
        delete(&temp_dir, db_name, collection_name, "2")?;

        //Act
        let result = reindex(&temp_dir, db_name, collection_name)?;

        //Assert
        result.success();

        let result = search_all(&temp_dir, db_name, collection_name)?;
        let result = result.success();
        result
            .stdout(predicates::str::contains("1.0, 2.0, 3.0"))
            .stdout(predicates::str::contains("payload"))
            .stdout(predicates::str::contains("4.0, 5.0, 6.0").not())
            .stdout(predicates::str::contains("payload2").not());

        Ok(())
    }

    #[test]
    fn truncate_databases_wal_should_result_in_consistant_wal() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, "test_col")?;
        create_collection(&temp_dir, db_name, "test_col2")?;

        //Act
        let result = truncatewal(&temp_dir, db_name, None)?;

        //Assert
        result.success();

        assert!(is_wal_consistent(&temp_dir, db_name, None)?);

        Ok(())
    }

    #[test]
    fn truncate_collections_wal_should_result_in_consistant_wal() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        insert(&temp_dir, db_name, collection_name, "1.0,2.0,3.0;payload")?;
        insert(&temp_dir, db_name, collection_name, "4.0,5.0,6.0;payload2")?;
        delete(&temp_dir, db_name, collection_name, "2")?;

        //Act
        let result = truncatewal(&temp_dir, db_name, Some(collection_name))?;

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        Ok(())
    }

    //Load tests

    #[cfg(feature = "load_tests")]
    fn prepare_data_file(
        temp_dir: &tempfile::TempDir,
        records_count: usize,
        dimensions: usize,
    ) -> Result<PathBuf> {
        let mut file_content = String::new();
        for i in 0..records_count {
            let data = format!(
                "{};test_payload\n",
                vec![i as f32; dimensions]
                    .iter()
                    .map(|x| x.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            );
            file_content.push_str(&data);
        }
        let file_path = temp_dir
            .path()
            .join(format!("test_data_{}_{}.txt", records_count, dimensions));
        std::fs::write(&file_path, file_content)?;
        Ok(file_path)
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn bulk_insert_file_1k_dense_384_dim_vectos_should_success() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 1000, 384)?;

        //Act
        let start = std::time::Instant::now();
        let result = bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in bulk_insert_file_1k_dense_384_dim_vectos_should_success",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let start = std::time::Instant::now();
        let result = search_all(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!("---search time {:?} s for 1k records", duration);
        result
            .success()
            .stdout(predicates::str::contains("Found 1000"));
        Ok(())
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn bulk_insert_file_10k_dense_384_dim_vectors_should_success() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 10_000, 384)?;

        //Act
        let start = std::time::Instant::now();
        let result = bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in bulk_insert_file_10k_dense_384_dim_vectors_should_success",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let start = std::time::Instant::now();
        let result = search_all(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!("---search time {:?} s for 10k records", duration,);
        result
            .success()
            .stdout(predicates::str::contains("Found 10000"));
        Ok(())
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn bulk_insert_file_100k_dense_384_dim_vectors_should_success() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 100_000, 384)?;

        //Act
        let start = std::time::Instant::now();
        let result = bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in bulk_insert_file_100k_dense_384_dim_vectors_should_success",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let start = std::time::Instant::now();
        let result = search_all(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!("---search time {:?} s for 100k records", duration);
        result
            .success()
            .stdout(predicates::str::contains("Found 100000"));
        Ok(())
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn reindex_should_correctly_reindex_1k_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 1000, 384)?;
        bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        delete(&temp_dir, db_name, collection_name, "2")?;

        //Act
        let start = std::time::Instant::now();
        let result = reindex(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in reindex_should_correctly_reindex_1k_records",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let result = search_all(&temp_dir, db_name, collection_name)?;
        result
            .success()
            .stdout(predicates::str::contains("Found 999"));
        Ok(())
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn reindex_should_correctly_reindex_10k_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 10_000, 384)?;
        bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        delete(&temp_dir, db_name, collection_name, "2")?;

        //Act
        let start = std::time::Instant::now();
        let result = reindex(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in reindex_should_correctly_reindex_10k_records",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let result = search_all(&temp_dir, db_name, collection_name)?;
        result
            .success()
            .stdout(predicates::str::contains("Found 9999"));
        Ok(())
    }

    #[cfg(feature = "load_tests")]
    #[test]
    fn reindex_should_correctly_reindex_100k_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";

        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;
        let file_path = prepare_data_file(&temp_dir, 100_000, 384)?;
        bulk_insert_file(&temp_dir, db_name, collection_name, file_path)?;
        delete(&temp_dir, db_name, collection_name, "2")?;

        //Act
        let start = std::time::Instant::now();
        let result = reindex(&temp_dir, db_name, collection_name)?;
        let duration = start.elapsed().as_secs();
        println!(
            "---act time {:?} s in reindex_should_correctly_reindex_100k_records",
            duration
        );

        //Assert
        result.success();

        assert!(is_wal_consistent(
            &temp_dir,
            db_name,
            Some(collection_name)
        )?);

        let result = search_all(&temp_dir, db_name, collection_name)?;
        result
            .success()
            .stdout(predicates::str::contains("Found 99999"));
        Ok(())
    }
}
