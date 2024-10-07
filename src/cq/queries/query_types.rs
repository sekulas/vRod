use super::Result;

use crate::command_query_builder::{CQAction, Query};

pub struct SearchSimilarQuery {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Query for SearchSimilarQuery {
    fn execute(&mut self) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for SearchSimilarQuery {
    fn to_string(&self) -> String {
        todo!();
    }
}
