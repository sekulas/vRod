use super::Result;
use std::path::{Path, PathBuf};

use crate::{
    command_query_builder::{CQAction, Query},
    database::DbConfig,
    types::DB_CONFIG,
};

pub struct ListCollectionsQuery {
    pub db_path: PathBuf,
}

impl ListCollectionsQuery {
    pub fn new(db_path: &Path) -> Self {
        ListCollectionsQuery {
            db_path: db_path.to_owned(),
        }
    }
}

impl Query for ListCollectionsQuery {
    fn execute(&mut self) -> Result<()> {
        let db_config = DbConfig::load(&self.db_path.join(DB_CONFIG))?;

        let collections = db_config.get_collections();

        if collections.is_empty() {
            println!("No collections found.");
            return Ok(());
        }

        print!("Collections:\n[\n");
        for (index, collection) in collections.iter().enumerate() {
            print!("  {}", collection);
            if index < collections.len() - 1 {
                println!(",");
            }
        }
        print!("\n]");

        Ok(())
    }
}

impl CQAction for ListCollectionsQuery {
    fn to_string(&self) -> String {
        "LISTCOLLECTIONS".to_string()
    }
}