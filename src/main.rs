mod command;
mod database;
mod utils;

use clap::Parser;
use std::path::PathBuf;
use utils::embeddings::process_embeddings;

#[derive(Parser)]
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

#[derive(thiserror::Error, Debug)]
enum ArgsError {
    #[error("Missing '--init_database_name' flag with argument for '--init_database' flag.")]
    MissingInitDatabaseNameFlag,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    //TODO To remove / for developmnet only
    if let Some(amount) = args.generate_embeddings {
        process_embeddings(amount)?;
        return Ok(());
    }

    if let Some(path) = args.init_database.as_deref() {
        match args.init_database_name {
            Some(name) => {
                // Initialize the database
                println!("Database initialized.\nPath: {}/{}", path.display(), name);
            }
            None => {
                return Err(ArgsError::MissingInitDatabaseNameFlag.into());
            }
        }

        return Ok(());
    }

    let database = match args.database {
        //TODO Look for config file
        Some(path) => {
            // Use the specified database directory
            database::Database::new(path)
        }
        None => {
            let current_dir = std::env::current_dir()?;
            database::Database::new(current_dir)
        }
    };

    Ok(())
}
