use super::Result;
use crate::command_query_builder::parsing_ops::parse_string_from_vector_option;
use crate::types::{Dim, Lsn};
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::Collection,
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
        self.collection.insert(&self.vector, &self.payload, lsn)?;
        println!("Embedding inserted successfully");
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
