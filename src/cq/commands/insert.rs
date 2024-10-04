use super::Result;
use crate::components::collection::types::CollectionInsertResult;
use crate::cq::parsing_ops::parse_string_from_vector_option;
use crate::types::{Dim, Lsn};
use crate::{
    components::collection::Collection,
    cq::{CQAction, Command},
};

pub struct InsertCommand {
    collection: Collection,
    vector: Vec<Dim>,
    payload: String,
}

impl InsertCommand {
    pub fn new(collection: Collection, vector: Vec<Dim>, payload: String) -> Self {
        Self {
            collection,
            vector,
            payload,
        }
    }
}

impl Command for InsertCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        match self.collection.insert(&self.vector, &self.payload, lsn)? {
            CollectionInsertResult::Inserted => {
                println!("Embedding inserted successfully");
            }
            CollectionInsertResult::NotInserted { description } => {
                println!("Embedding not inserted: {}", description);
            }
        }
        Ok(())
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        self.collection.rollback_insertion_like_command(lsn)?;
        Ok(())
    }
}

impl CQAction for InsertCommand {
    fn to_string(&self) -> String {
        format!(
            "INSERT {};{}",
            parse_string_from_vector_option(Some(&self.vector)),
            self.payload
        )
    }
}
