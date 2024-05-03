mod command;
mod database;
mod error;
mod utils;
mod wal;

use clap::Parser;
use command::builder::CommandBuilder;
use database::Database;
use std::{cell::RefCell, path::PathBuf, rc::Rc};
use utils::embeddings::process_embeddings;

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
    let database_path = match args.database {
        Some(path) => path,
        None => std::env::current_dir()?,
    };

    let database = Rc::new(RefCell::new(Database::load(database_path)?));
    let command_builder: CommandBuilder = CommandBuilder::new(database);
    let command = command_builder.build(args.collection, command_text, args.command_arg)?;

    command.execute()?;

    Ok(())
}
