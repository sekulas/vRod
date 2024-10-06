use super::Result;
use crate::{
    components::collection::{types::CollectionSearchResult, Collection},
    cq::{queries::dto::RecordDTO, CQAction, CQTarget, CQValidator, Query, Validator},
    types::RecordId,
};

pub struct SearchQuery {
    collection: CQTarget,
    record_id: RecordId,
}

impl SearchQuery {
    pub fn new(collection: CQTarget, record_id: RecordId) -> Self {
        Self {
            collection,
            record_id,
        }
    }
}

impl Query for SearchQuery {
    fn execute(&mut self) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        let result = collection.search(self.record_id)?;

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
