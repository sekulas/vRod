use super::Result;
use crate::{
    components::collection::Collection,
    cq::{
        queries::dto::RecordDTOList, types::SEARCH_ALL_Q_STR, CQAction, CQTarget, CQValidator,
        Query, Validator,
    },
};
pub struct SearchAllQuery {
    collection: CQTarget,
}

impl SearchAllQuery {
    pub fn new(collection: CQTarget) -> Self {
        Self { collection }
    }
}

impl Query for SearchAllQuery {
    fn execute(&self) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        let result = collection.search_all()?;

        println!("Found {} records.", result.len());
        println!("{}", RecordDTOList(result));
        Ok(())
    }
}

impl CQAction for SearchAllQuery {
    fn to_string(&self) -> String {
        SEARCH_ALL_Q_STR.to_string()
    }
}
