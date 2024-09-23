use super::Result;
use crate::command_query_builder::{CQAction, Command};
use crate::components::wal::Wal;
use crate::types::{Lsn, WAL_FILE};
use std::fs;
use std::path::{Path, PathBuf};

pub struct TruncateWalCommand {
    pub target_path: PathBuf,
}

impl TruncateWalCommand {
    pub fn new(target_path: &Path) -> Self {
        TruncateWalCommand {
            target_path: target_path.to_owned(),
        }
    }
}

impl Command for TruncateWalCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        let wal_path = self.target_path.join(WAL_FILE);

        fs::remove_file(&wal_path)?;

        Wal::create(&wal_path)?;

        Ok(())
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        Ok(())
    }
}

impl CQAction for TruncateWalCommand {
    fn to_string(&self) -> String {
        "TRUNCATEWAL".to_string()
    }
}

pub struct BulkInsertCommand {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Command for BulkInsertCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for BulkInsertCommand {
    fn to_string(&self) -> String {
        todo!();
    }
}

pub struct ReindexCommand {}

impl Command for ReindexCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&mut self, lsn: Lsn) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for ReindexCommand {
    fn to_string(&self) -> String {
        todo!();
    }
}
