use super::Result;

use crate::{
    cq::{CQAction, CQTarget, CQValidator, Query, Validator},
    database::DbConfig,
    types::DB_CONFIG,
};

pub struct ListCollectionsQuery {
    database: CQTarget,
}

impl ListCollectionsQuery {
    pub fn new(database: CQTarget) -> Self {
        ListCollectionsQuery { database }
    }
}

impl Query for ListCollectionsQuery {
    fn execute(&mut self) -> Result<()> {
        CQValidator::target_exists(&self.database);
        let path = self.database.get_target_path();

        let db_config = DbConfig::load(&path.join(DB_CONFIG))?;

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
