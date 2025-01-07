use super::{Error, Result};

use crate::{
    components::wal::Wal,
    cq::{types::TRUNCATE_WAL_C_STR, CQAction, Command},
};

pub struct TruncateWalCommand {}

impl TruncateWalCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Command for TruncateWalCommand {
    fn execute(&self, wal: &mut Wal) -> Result<()> {
        let lsn = wal.append(self.to_string())?;
        wal.truncate(lsn)?;
        wal.commit()?;

        println!("WAL truncated successfully.");
        Ok(())
    }

    fn rollback(&self, wal: &mut Wal) -> Result<()> {
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        println!("No ROLLBACK for TRUNCATEWAL command provided.");

        Err(Error::RollbackFailed {
            command: self.to_string(),
        })
    }
}

impl CQAction for TruncateWalCommand {
    fn to_string(&self) -> String {
        TRUNCATE_WAL_C_STR.to_string()
    }
}
