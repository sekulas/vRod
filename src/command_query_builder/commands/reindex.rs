use super::Result;
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::Collection,
    types::Lsn,
};

pub struct ReindexCommand {
    collection: Collection,
}

impl ReindexCommand {
    pub fn new(collection: Collection) -> Self {
        Self { collection }
    }
}

impl Command for ReindexCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        self.collection.reindex(lsn)?;
        println!("Reindexation completed.");
        Ok(())
    }

    fn rollback(&mut self, _: Lsn) -> Result<()> {
        Ok(())
    }
}

impl CQAction for ReindexCommand {
    fn to_string(&self) -> String {
        "REINDEX".to_string()
    }
}
