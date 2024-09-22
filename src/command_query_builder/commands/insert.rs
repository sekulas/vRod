use super::Result;
use crate::command_query_builder::parsing_ops::parse_vec_n_payload;
use crate::types::Lsn;
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::Collection,
};

pub struct InsertCommand {
    collection: Collection,
    data: String,
}

impl InsertCommand {
    pub fn new(collection: Collection, data: String) -> Self {
        Self { collection, data }
    }
}

impl Command for InsertCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        let (vector, payload) = parse_vec_n_payload(&self.data)?;
        self.collection.insert(&vector, &payload, lsn)?;
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
        format!("INSERT {}", self.data)
    }
}
