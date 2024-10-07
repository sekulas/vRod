use super::Result;
use crate::{
    components::{collection::Collection, wal::Wal},
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
    types::Dim,
};

pub struct BulkInsertCommand {
    collection: CQTarget,
    vectors_and_payloads: Vec<(Vec<Dim>, String)>,
}

impl BulkInsertCommand {
    pub fn new(collection: CQTarget, vectors_and_payloads: Vec<(Vec<Dim>, String)>) -> Self {
        Self {
            collection,
            vectors_and_payloads,
        }
    }
}

impl Command for BulkInsertCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        let vectors_and_payloads_ref: Vec<(&[Dim], &str)> = self
            .vectors_and_payloads
            .iter()
            .map(|(vec, string)| (vec.as_slice(), string.as_str()))
            .collect();

        collection.bulk_insert(&vectors_and_payloads_ref, lsn)?;

        wal.commit()?;
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(format!("ROLLBACK {}", self.to_string()))?; //TODO: ### Not having inserted records in WAL? For rollback no need i see.

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        collection.rollback_insertion_like_command(lsn)?;

        wal.commit()?;
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
