use super::Result;
use crate::{
    components::{
        collection::{types::CollectionDeleteResult, Collection},
        wal::Wal,
    },
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
    types::RecordId,
};

pub struct DeleteCommand {
    collection: CQTarget,
    record_id: RecordId,
}

impl DeleteCommand {
    pub fn new(collection: CQTarget, record_id: RecordId) -> Self {
        Self {
            collection,
            record_id,
        }
    }
}

impl Command for DeleteCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        match collection.delete(self.record_id, lsn)? {
            CollectionDeleteResult::Deleted => {
                println!("Embedding deleted successfully.");
            }
            CollectionDeleteResult::NotFound => {
                println!("Embedding to delete has been not found.");
            }
        }

        wal.commit()?;

        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        println!("No ROLLBACK for DELETE command provided. Commiting."); //TODO: Maybe rollback?

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for DeleteCommand {
    fn to_string(&self) -> String {
        format!("DELETE {}", self.record_id)
    }
}
