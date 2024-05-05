mod command;
mod database;
mod error;
mod utils;
mod wal;

use clap::Parser;
use command::{builder::CommandBuilder, types::Command};
use database::{types::WAL_FILE, Database};
use std::path::PathBuf;
use utils::embeddings::process_embeddings;
use wal::{utils::wal_to_txt, WAL};

use crate::error::{Error, Result};
use command::builder::Builder;

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

    if let Some(path) = args.init_database {
        match args.init_database_name {
            Some(name) => {
                return Ok(Database::create(path.to_path_buf(), name)?);
            }
            None => {
                return Err(Error::MissingInitDatabaseName);
            }
        }
    }

    let command_text = args.execute.ok_or(Error::MissingCommand)?;

    let target_path = specify_target_path(args.database, args.collection)?;

    //let database = Rc::new(RefCell::new(Database::load(database_path)?)); //TODO is it needed?

    let command = CommandBuilder::build(target_path.clone(), command_text, args.command_arg)?;

    execute_command(target_path, command)?;

    Ok(())
}

fn execute_command(target_path: PathBuf, command: Box<dyn Command>) -> Result<()> {
    let wal_path = target_path.join(WAL_FILE);
    let mut wal = WAL::load(&wal_path)?;

    wal.append(command.to_string())?;
    command.execute()?;
    wal.commit()?;

    //TODO To remove / for developmnet only
    wal_to_txt(&wal_path).unwrap_or_else(|error| {
        eprintln!(
            "Error occurred while converting WAL to text.\nWAL Path: {:?}\n{:?}",
            wal_path, error
        );
    });

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

    Ok(target_path)
}
