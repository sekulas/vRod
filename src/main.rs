mod command_query_builder;
mod database;
mod error;
mod types;
mod utils;
mod wal;

use crate::types::WAL_FILE;
use clap::Parser;
use command_query_builder::{Builder, CQBuilder, CQType, Command};
use database::{CollectionsGuard, Database, DbConfig};
use std::path::{Path, PathBuf};
use types::{CommandTarget, DB_CONFIG};
use utils::embeddings::process_embeddings;
use wal::{utils::wal_to_txt, Wal, WalType};

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
        CQType::Query(query) => {
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
    if let CQType::Command(last_command) = CQBuilder::build(target_path, command, arg)? {
        println!("Redoing last command: {:?}", last_command.to_string());
        last_command.rollback()?;
        last_command.execute()?;
        wal.commit()?;
    }
    Ok(())
}

fn execute_command(wal: &mut Wal, command: Box<dyn Command>) -> Result<()> {
    wal.append(command.to_string())?;
    command.execute()?;
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
