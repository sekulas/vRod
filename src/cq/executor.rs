use super::Result;
use super::{Builder, CQBuilder, CQTarget, CQType, Command};
use crate::cq::types::HANDLE_FAILED_ROLLBACK_C_STR;
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
                Ok(())
            }
        }
    }
}

impl CQExecutor {
    fn execute_cq(cq: CQType, mut wal: Wal) -> Result<()> {
        match cq {
            CQType::Command(command) => {
                println!("Executing command: {:?}", command.to_string());
                command.execute(&mut wal)?
            }
            CQType::Query(query) => {
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
        if let CQType::Command(last_command) = CQBuilder::build(target, command, arg, file_path)? {
            let stringified_last_command = last_command.to_string();

            if stringified_last_command == HANDLE_FAILED_ROLLBACK_C_STR {
                Self::handle_failed_rollback(last_command, wal)?;
                return Ok(());
            }

            println!("Rollbacking last command: {:?}", stringified_last_command);
            last_command.rollback(wal)?;
            println!("Rollback completed.");
            println!("Please re-run the last command to try again or proceed with a new command.");
        }
        Ok(())
    }

    fn handle_failed_rollback(handle_command: Box<dyn Command>, wal: &mut Wal) -> Result<()> {
        println!("Failed to rollback last command.");
        println!("Executing command: {:?}", HANDLE_FAILED_ROLLBACK_C_STR);
        handle_command.execute(wal)?;
        println!("Command executed successfully.");
        Ok(())
    }
}
