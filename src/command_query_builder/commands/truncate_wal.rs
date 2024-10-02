use super::{Error, Result};
use std::path::{Path, PathBuf};

use crate::{
    command_query_builder::{CQAction, Command},
    components::wal::{Wal, WalType},
    types::{Lsn, WAL_FILE},
};

pub struct TruncateWalCommand {
    pub wal_parent_path: PathBuf,
}

impl TruncateWalCommand {
    pub fn new(target_path: &Path) -> Self {
        TruncateWalCommand {
            wal_parent_path: target_path.to_owned(),
        }
    }
}

impl Command for TruncateWalCommand {
    fn execute(&mut self, lsn: Lsn) -> Result<()> {
        let wal_path = self.wal_parent_path.join(WAL_FILE);
        let wal = Wal::load(&wal_path)?;

        match wal {
            WalType::Uncommited {
                wal,
                uncommited_command,
                arg: _,
            } => {
                if uncommited_command != self.to_string() {
                    return Err(Error::Unexpected {
                        description: "Last command should be TRUNCATEWAL.".to_string(),
                    });
                }
                wal.truncate(lsn)?;
                println!("WAL truncated successfully.");
                Ok(())
            }
            _ => Err(Error::Unexpected {
                description: "Wal during command execution should be in not consistent state."
                    .to_string(),
            }),
        }
    }

    fn rollback(&mut self, _: Lsn) -> Result<()> {
        Ok(())
    }
}

impl CQAction for TruncateWalCommand {
    fn to_string(&self) -> String {
        "TRUNCATEWAL".to_string()
    }
}
