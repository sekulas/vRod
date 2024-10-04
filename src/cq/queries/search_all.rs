use super::Result;
use crate::{
    components::collection::Collection,
    cq::{queries::dto::RecordDTOList, CQAction, Query},
};
pub struct SearchAllQuery {
    collection: Collection,
}

impl SearchAllQuery {
    pub fn new(collection: Collection) -> Self {
        Self { collection }
    }
}

impl Query for SearchAllQuery {
    fn execute(&mut self) -> Result<()> {
        let result = self.collection.search_all()?;
        println!("Found {} records.", result.len());
        println!("{}", RecordDTOList(result));
        Ok(())
    }
}

impl CQAction for SearchAllQuery {
    fn to_string(&self) -> String {
        "SEARCHALL".to_string()
    }
}
