use super::Result;

use crate::{
    components::wal::Wal,
    cq::{CQAction, Command},
};

pub struct TruncateWalCommand {}

impl TruncateWalCommand {
    pub fn new() -> Self {
        Self {}
    }
}

impl Command for TruncateWalCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        let lsn = wal.append(self.to_string())?;
        wal.truncate(lsn)?;
        wal.commit()?;

        println!("WAL truncated successfully.");
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        wal.append(format!("ROLLBACK {}", self.to_string()))?;

        println!("No ROLLBACK for TRUNCATEWAL command provided. Commiting."); //TODO: ### Is the rollback necessary here?

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for TruncateWalCommand {
    fn to_string(&self) -> String {
        "TRUNCATEWAL".to_string()
    }
}
