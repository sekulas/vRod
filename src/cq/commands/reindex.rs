use std::fs;

use super::Result;
use crate::{
    components::{collection::Collection, wal::Wal},
    cq::{CQAction, CQTarget, CQValidator, Command, Validator},
    types::{INDEX_FILE, STORAGE_FILE},
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
        //TODO: ### Is this rollback fine? Should it contain Rollback entry in old file?
        //TODO: ### Is this okay that we are not doing this in collection abstraction as cannot load it?

        let path = self.collection.get_target_path();

        let bak_strg_path = path.join(format!("{STORAGE_FILE}.bak"));
        let bak_idx_path = path.join(format!("{INDEX_FILE}.bak"));

        if (!bak_strg_path.exists()) || (!bak_idx_path.exists()) {
            //TODO: ### Is this okay, readonly in this situation?
            println!("No backup files found for ROLLBACK REINDEX.");
        } else {
            let cur_strg_path = path.join(STORAGE_FILE);
            let cur_idx_path = path.join(INDEX_FILE);
            fs::rename(bak_strg_path, cur_strg_path)?;
            fs::rename(bak_idx_path, cur_idx_path)?;
        }

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for ReindexCommand {
    fn to_string(&self) -> String {
        "REINDEX".to_string()
    }
}
