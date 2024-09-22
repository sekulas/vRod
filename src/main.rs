mod command_query_builder;
mod components;
mod database;
mod error;
mod types;
mod utils;

use crate::types::WAL_FILE;
use clap::Parser;
use command_query_builder::{Builder, CQBuilder, CQType, Command};
use components::wal::{utils::wal_to_txt, Wal, WalType};
use database::{Database, DbConfig};
use std::path::{Path, PathBuf};
use types::DB_CONFIG;
use utils::embeddings::process_embeddings;

use crate::error::{Error, Result};

#[derive(Parser)]
#[command(arg_required_else_help(true))]
struct Args {
    #[arg(short, long, value_name = "PATH")]
    init_database: Option<PathBuf>,

    #[arg(short = 'n', long, value_name = "NAME")]
    init_database_name: Option<String>,

    #[arg(short, long, value_name = "DIR")]
    database: Option<PathBuf>,

    #[arg(short, long, value_name = "COLLECTION_NAME")]
    collection: Option<String>,

    #[arg(short, long, value_name = "COMMAND")]
    execute: Option<String>,

    #[arg(short = 'a', long, value_name = "COMMAND_ARG")]
    command_arg: Option<String>,

    //TODO To remove / for developmnet only
    #[arg(short, long, value_name = "AMOUNT")]
    generate_embeddings: Option<usize>,

    #[arg(short, long, value_name = "PATH")]
    wal_path: Option<PathBuf>,
}

fn main() {
    match run() {
        Ok(_) => {}
        Err(e) => eprintln!("ERROR: {:?}: {}", e, e),
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

    let target_path = specify_target_path(args.database, args.collection)?;

    let cq_action = CQBuilder::build(&target_path, command_text, args.command_arg)?;

    let wal_path = target_path.join(WAL_FILE);
    let wal_type = Wal::load(&wal_path)?;

    match wal_type {
        WalType::Consistent(wal) => {
            execute_cq_action(cq_action, wal)?;
        }
        WalType::Uncommited {
            mut wal,
            uncommited_command,
            arg,
        } => {
            redo_last_command(&target_path, &mut wal, uncommited_command, arg)?;
            execute_cq_action(cq_action, wal)?;
        }
    }

    Ok(())
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
) -> Result<()> {
    if let CQType::Command(mut last_command) = CQBuilder::build(target_path, command, arg)? {
        let stringified_last_command = last_command.to_string();
        println!("Redoing last command: {:?}", stringified_last_command);

        let mut lsn = wal.append(format!("ROLLBACK {stringified_last_command}"))?;
        last_command.rollback(lsn)?;
        wal.commit()?;

        lsn = wal.append(stringified_last_command)?;
        last_command.execute(lsn)?;
        wal.commit()?;
    }
    Ok(())
}

fn execute_command(wal: &mut Wal, mut command: Box<dyn Command>) -> Result<()> {
    let lsn = wal.append(command.to_string())?;
    command.execute(lsn)?;
    wal.commit()?;
    Ok(())
}

fn specify_target_path(
    database_path: Option<PathBuf>,
    collection_name: Option<String>,
) -> Result<PathBuf> {
    let database_path = get_database_path(database_path)?;

    let target_path = match collection_name {
        Some(collection_name) => {
            validate_collection(&database_path, &collection_name)?;
            database_path.join(collection_name)
        }

        None => database_path,
    };

    Ok(target_path)
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

fn validate_collection(database_path: &Path, collection_name: &str) -> Result<()> {
    let db_config = DbConfig::load(&database_path.join(DB_CONFIG))?;

    match db_config.collection_exists(collection_name) {
        true => Ok(()),
        false => Err(Error::CollectionDoesNotExist(collection_name.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use assert_cmd::{assert::Assert, Command};
    use command_query_builder::parsing_ops::parse_vec_n_payload;
    use types::{INDEX_FILE, STORAGE_FILE};
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;
    const BINARY: &str = "vrod";

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
        result
            .success()
            .stderr(predicates::str::contains("already exists"));

        Ok(())
    }

    #[test]
    fn init_database_missing_name() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut cmd = Command::cargo_bin(BINARY)?;
        let err = Error::MissingInitDatabaseName;

        //Act
        let result = cmd.arg("--init-database").arg(temp_dir.path()).assert();

        //Assert
        result
            .success()
            .stderr(predicates::str::contains(err.to_string()));

        Ok(())
    }

    #[test]
    fn create_collection_should_create_collection() -> Result<()> {
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

        Ok(())
    }

    #[test]
    fn create_collection_should_fail_when_collection_already_exists() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let db_name = "test_db";
        let collection_name = "test_col";
        init_database(&temp_dir, db_name)?;
        create_collection(&temp_dir, db_name, collection_name)?;

        //Act
        let result = create_collection(&temp_dir, db_name, collection_name)?;

        //Assert
        result
            .success()
            .stderr(predicates::str::contains("already exists"));

        Ok(())
    }

    #[test]
    fn insert_embedding_should_store_embedding_in_collection() -> Result<()> {
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

        Ok(())
    }

    #[test]
    fn search_embedding_should_return_embedding_when_it_exists() -> Result<()> {
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

        Ok(())
    }

    #[test]
    fn search_embedding_should_fail_when_embedding_does_not_exist() -> Result<()> {
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

        Ok(())
    }
}
