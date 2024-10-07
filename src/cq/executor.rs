use super::Result;
use super::{Builder, CQBuilder, CQTarget, CQType};
use crate::{
    components::wal::{Wal, WalType},
    types::WAL_FILE,
};
use std::path::PathBuf;

pub struct CQExecutor;

pub trait Executor {
    fn execute(target: &CQTarget, cq: CQType) -> Result<()>;
}

impl Executor for CQExecutor {
    fn execute(target: &CQTarget, cq: CQType) -> Result<()> {
        let target_path = target.get_target_path();
        let wal_type = Wal::load(&target_path.join(WAL_FILE))?;

        //TODO: ### How to proceed with the rollback? Perform it and make the user run the command once more?
        match wal_type {
            WalType::Consistent(wal) => {
                CQExecutor::execute_cq(cq, wal)?;
                Ok(())
            }
            WalType::Uncommited {
                mut wal,
                uncommited_command,
                arg,
            } => {
                CQExecutor::rollback_last_cq(target, &mut wal, uncommited_command, arg, None)?;
                CQExecutor::execute_cq(cq, wal)?;
                Ok(())
            }
        }
    }
}

impl CQExecutor {
    fn execute_cq(cq: CQType, mut wal: Wal) -> Result<()> {
        match cq {
            CQType::Command(mut command) => {
                println!("Executing command: {:?}", command.to_string());
                command.execute(&mut wal)?
            }
            CQType::Query(mut query) => {
                println!("Executing query: {:?}", query.to_string());
                query.execute()?
            }
        };
        Ok(())
    }

    fn rollback_last_cq(
        target: &CQTarget,
        wal: &mut Wal,
        command: String,
        arg: Option<String>,
        file_path: Option<PathBuf>,
    ) -> Result<()> {
        if let CQType::Command(mut last_command) =
            CQBuilder::build(target, command, arg, file_path)?
        {
            let stringified_last_command = last_command.to_string();
            println!("Rollbacking last command: {:?}", stringified_last_command);

            last_command.rollback(wal)?;

            println!("Rollback completed.");
        }
        Ok(())
    }
}
