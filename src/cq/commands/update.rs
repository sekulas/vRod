use super::Result;
use crate::{
    components::{
        collection::{types::CollectionUpdateResult, Collection},
        wal::Wal,
    },
    cq::{
        parsing_ops::parse_string_from_vector_option, CQAction, CQTarget, CQValidator, Command,
        Validator,
    },
    types::{Dim, RecordId},
};

pub struct UpdateCommand {
    collection: CQTarget,
    record_id: RecordId,
    vector: Option<Vec<Dim>>,
    payload: Option<String>,
}

impl UpdateCommand {
    pub fn new(
        collection: CQTarget,
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
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        match collection.update(
            self.record_id,
            self.vector.as_deref(),
            self.payload.as_deref(),
            lsn,
        )? {
            CollectionUpdateResult::Updated => {
                println!("Embedding updated successfully.");
            }
            CollectionUpdateResult::NotFound => {
                println!("Embedding to update has been not found.");
            }
            CollectionUpdateResult::NotUpdated { description } => {
                println!("Embedding not updated: {}", description);
            }
        }

        wal.commit()?;
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(format!("ROLLBACK {}", self.to_string()))?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        collection.rollback_update_command(lsn)?;

        wal.commit()?;
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
