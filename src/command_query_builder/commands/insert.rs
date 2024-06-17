use super::{Error, Result};
use crate::{
    collection::Collection,
    command_query_builder::{CQAction, Command},
};
use std::num::ParseFloatError;
use std::{cell::RefCell, rc::Rc, result};

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
        let (vector, payload) = parse_data(&self.data)?;

        let mut collection = self.collection.borrow_mut();
        collection.insert(&vector, &payload)?;

        Ok(())
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

fn parse_data(data: &str) -> Result<(Vec<f32>, String)> {
    let splitted_data = data.split(';').collect::<Vec<&str>>();

    if splitted_data.len() != 2 {
        return Err(Error::InvalidDataFormat {
            data: data.to_string(),
        });
    }

    let vector = parse_vector(splitted_data[0])?;

    Ok((vector, splitted_data[1].to_string()))
}

fn parse_vector(data: &str) -> result::Result<Vec<f32>, ParseFloatError> {
    data.split(',').map(|s| s.trim().parse::<f32>()).collect()
}
