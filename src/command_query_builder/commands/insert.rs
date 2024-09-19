use super::Result;
use crate::command_query_builder::parsing_ops::parse_vec_n_payload;
use crate::types::Lsn;
use crate::{
    command_query_builder::{CQAction, Command},
    components::collection::Collection,
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
    fn execute(&self, lsn: Lsn) -> Result<()> {
        let (vector, payload) = parse_vec_n_payload(&self.data)?;

        let mut collection = self.collection.borrow_mut();
        collection.insert(&vector, &payload, lsn)?;

        Ok(())
    }

    fn rollback(&self, lsn: Lsn) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for InsertCommand {
    fn to_string(&self) -> String {
        todo!();
    }
}
