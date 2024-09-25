use super::Result;
use crate::{
    command_query_builder::{queries::dto::RecordDTO, CQAction, Query},
    components::collection::{types::CollectionSearchResult, Collection},
    types::RecordId,
};

pub struct SearchQuery {
    collection: Collection,
    record_id: RecordId,
}

impl SearchQuery {
    pub fn new(collection: Collection, record_id: RecordId) -> Self {
        Self {
            collection,
            record_id,
        }
    }
}

impl Query for SearchQuery {
    fn execute(&mut self) -> Result<()> {
        let result = self.collection.search(self.record_id)?;

        match result {
            CollectionSearchResult::FoundRecord(record) => {
                println!("{}", RecordDTO(&self.record_id, &record));
            }
            CollectionSearchResult::NotFound => {
                println!("Record not found.");
            }
        };

        Ok(())
    }
}

impl CQAction for SearchQuery {
    fn to_string(&self) -> String {
        format!("SEARCH {}", self.record_id)
    }
}
