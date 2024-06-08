use super::Result;
use crate::{
    collection::Collection,
    command_query_builder::{CQAction, Command},
};
use std::{cell::RefCell, rc::Rc};

pub struct InsertCommand {
    pub collection: Rc<RefCell<Collection>>,
    pub data: String,
}

impl InsertCommand {
    pub fn new(collection: Rc<RefCell<Collection>>, data: String) -> Self {
        Self { collection, data }
    }
}

impl Command for InsertCommand {
    fn execute(&self) -> Result<()> {
        todo!("Not implemented.")
    }

    fn rollback(&self) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for InsertCommand {
    fn to_string(&self) -> String {
        todo!();
    }
}
