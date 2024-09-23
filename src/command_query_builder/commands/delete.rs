use super::Result;
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::{types::CollectionDeleteResult, Collection},
    types::{Lsn, RecordId},
};

pub struct DeleteCommand {
    collection: Collection,
    record_id: RecordId,
}

impl DeleteCommand {
    pub fn new(collection: Collection, record_id: RecordId) -> Self {
        Self {
            collection,
            record_id,
        }
    }
}

impl Command for DeleteCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        match self.collection.delete(self.record_id, lsn)? {
            CollectionDeleteResult::Deleted => {
                println!("Embedding deleted successfully.");
            }
            CollectionDeleteResult::NotFound => {
                println!("Embedding to delete has been not found.");
            }
        }
        Ok(())
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        Ok(())
    }
}

impl CQAction for DeleteCommand {
    fn to_string(&self) -> String {
        format!("DELETE {}", self.record_id)
    }
}