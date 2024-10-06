use super::Result;
use crate::{
    components::{collection::Collection, wal::Wal},
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
};

pub struct ReindexCommand {
    collection: CQTarget,
}

impl ReindexCommand {
    pub fn new(collection: CQTarget) -> Self {
        Self { collection }
    }
}

impl Command for ReindexCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        collection.reindex(lsn)?;
        wal.commit()?;

        println!("Reindexation completed.");
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        println!("No ROLLBACK for REINDEX command provided. Commiting."); //TODO: Maybe rollback?

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for ReindexCommand {
    fn to_string(&self) -> String {
        "REINDEX".to_string()
    }
}
