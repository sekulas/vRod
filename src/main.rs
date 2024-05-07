mod command_builder;
mod database;
mod error;
mod types;
mod utils;
mod wal;

use crate::types::WAL_FILE;
use clap::Parser;
use command_builder::{commands::Command, Builder, CommandBuilder};
use std::path::{Path, PathBuf};
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

    if let (Some(_), None) = (args.init_database, args.init_database_name) {
        return Err(Error::MissingInitDatabaseName);
    }

    let command_text = args.execute.ok_or(Error::MissingCommand)?;

    let target_path = specify_target_path(args.database, args.collection)?;

    let command = CommandBuilder::build(&target_path, command_text, args.command_arg)?;

    let wal_path = target_path.join(WAL_FILE);
    let wal_type = Wal::load(&wal_path)?;

    match wal_type {
        WalType::Consistent(mut wal) => {
            execute_command(&mut wal, command)?;
        }
        WalType::Uncommited(mut wal, entry) => {
            redo_last_command(&target_path, &mut wal, entry)?;
            execute_command(&mut wal, command)?;
        }
    }

    //TODO To remove / for developmnet only
    wal_to_txt(&wal_path).unwrap_or_else(|error| {
        eprintln!(
            "Error occurred while converting WAL to text.\nWAL Path: {:?}\n{:?}",
            wal_path, error
        );
    });

    Ok(())
}

fn redo_last_command(target_path: &Path, wal: &mut Wal, entry: String) -> Result<()> {
    let last_command = CommandBuilder::build_from_string(target_path, entry)?;
    last_command.rollback()?;
    last_command.execute()?;
    wal.commit()?;
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
    let target_path = match (database_path, collection_name) {
        (Some(database_path), Some(collection_name)) => database_path.join(collection_name),
        (Some(database_path), None) => database_path,
        (None, Some(collection_name)) => std::env::current_dir()?.join(collection_name),
        (None, None) => std::env::current_dir()?,
    };

    validate_target_path(&target_path)?;
    Ok(target_path)
}

fn validate_target_path(target_path: &Path) -> Result<()> {
    if !target_path.join(WAL_FILE).exists() {
        return Err(Error::TargetDoesNotExist(
            target_path.to_string_lossy().to_string(),
        ));
    }
    Ok(())
}
