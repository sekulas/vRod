use super::Result;
use crate::{
    command_query_builder::{parsing_ops::parse_string_from_vector_option, CQAction, Command},
    components::collection::Collection,
    types::{Dim, Lsn, RecordId},
};

pub struct UpdateCommand {
    collection: Collection,
    record_id: RecordId,
    vector: Option<Vec<Dim>>,
    payload: Option<String>,
}

impl UpdateCommand {
    pub fn new(
        collection: Collection,
        record_id: RecordId,
        vector: Option<Vec<Dim>>,
        payload: Option<String>,
    ) -> Self {
        Self {
            collection,
            record_id,
            vector,
            payload,
        }
    }
}
impl Command for UpdateCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        self.collection.update(
            self.record_id,
            self.vector.as_deref(),
            self.payload.as_deref(),
            lsn,
        )?;
        println!("Record updated successfully");
        Ok(())
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        self.collection.rollback_update_command(lsn)?;
        Ok(())
    }
}

impl CQAction for UpdateCommand {
    fn to_string(&self) -> String {
        format!(
            "UPDATE {};{};{}",
            self.record_id,
            parse_string_from_vector_option(self.vector.as_deref()),
            self.payload.as_deref().unwrap_or_default()
        )
    }
}
