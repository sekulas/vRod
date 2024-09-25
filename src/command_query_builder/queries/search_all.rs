use std::fmt;

use super::Result;
use crate::{
    command_query_builder::{queries::dto::RecordDTOList, CQAction, Query},
    components::collection::{Collection, Record},
    types::RecordId,
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
        println!("{}", RecordDTOList(result));
        Ok(())
    }
}

impl CQAction for SearchAllQuery {
    fn to_string(&self) -> String {
        "SEARCHALL".to_string()
    }
}
