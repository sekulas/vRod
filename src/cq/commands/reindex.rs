use std::fs;

use super::{Error, Result};
use crate::{
    components::{collection::Collection, wal::Wal},
    cq::{types::REINDEX_C_STR, CQAction, CQTarget, CQValidator, Command, Validator},
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
    fn execute(&self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        collection.reindex(lsn)?;
        wal.commit()?;

        println!("Reindexation completed.");
        Ok(())
    }

    fn rollback(&self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        let path = self.collection.get_target_path();

        let bak_strg_path = path.join(format!("{STORAGE_FILE}.bak"));
        let bak_idx_path = path.join(format!("{INDEX_FILE}.bak"));

        if (!bak_strg_path.exists()) || (!bak_idx_path.exists()) {
            eprintln!("no backup files found for ROLLBACK {}.", REINDEX_C_STR);
            return Err(Error::RollbackFailed {
                command: self.to_string(),
            });
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
        REINDEX_C_STR.to_string()
    }
}
