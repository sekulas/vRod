use super::Result;
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::Collection,
    types::{Dim, Lsn},
};

pub struct BulkInsertCommand {
    pub collection: Collection,
    pub vectors_and_payloads: Vec<(Vec<Dim>, String)>,
}

impl BulkInsertCommand {
    pub fn new(collection: Collection, vectors_and_payloads: Vec<(Vec<Dim>, String)>) -> Self {
        Self {
            collection,
            vectors_and_payloads,
        }
    }
}

impl Command for BulkInsertCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        let vectors_and_payloads_ref: Vec<(&[Dim], &str)> = self
            .vectors_and_payloads
            .iter()
            .map(|(vec, string)| (vec.as_slice(), string.as_str()))
            .collect();

        self.collection
            .bulk_insert(&vectors_and_payloads_ref, lsn)?;

        Ok(())
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        self.collection.rollback_insertion_like_command(lsn)?;
        Ok(())
    }
}

impl CQAction for BulkInsertCommand {
    fn to_string(&self) -> String {
        format!(
            "BULKINSERT {}",
            self.vectors_and_payloads
                .iter()
                .map(|(vector, payload)| {
                    format!(
                        "{};{}",
                        vector
                            .iter()
                            .map(|dim| dim.to_string())
                            .collect::<Vec<String>>()
                            .join(","),
                        payload
                    )
                })
                .collect::<Vec<String>>()
                .join(" ")
        )
    }
}
