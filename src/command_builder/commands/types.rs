use super::Result;

pub trait Command {
    fn execute(&self) -> Result<()>;
    fn rollback(&self) -> Result<()>;
    fn to_string(&self) -> String;
}
