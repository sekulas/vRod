use super::Result;
use crate::components::collection::types::CollectionInsertResult;
use crate::components::wal::Wal;
use crate::cq::parsing_ops::parse_string_from_vector_option;
use crate::cq::{CQTarget, CQValidator, Validator};
use crate::types::Dim;
use crate::{
    components::collection::Collection,
    cq::{CQAction, Command},
};

pub struct InsertCommand {
    collection: CQTarget,
    vector: Vec<Dim>,
    payload: String,
}

impl InsertCommand {
    pub fn new(collection: CQTarget, vector: Vec<Dim>, payload: String) -> Self {
        Self {
            collection,
            vector,
            payload,
        }
    }
}

impl Command for InsertCommand {
    fn execute(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(self.to_string())?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        match collection.insert(&self.vector, &self.payload, lsn)? {
            CollectionInsertResult::Inserted => {
                println!("Embedding inserted successfully");
            }
            CollectionInsertResult::NotInserted { description } => {
                println!("Embedding not inserted: {}", description);
            }
        }

        wal.commit()?;
        Ok(())
    }

    fn rollback(&mut self, wal: &mut Wal) -> Result<()> {
        CQValidator::target_exists(&self.collection);
        let lsn = wal.append(format!("ROLLBACK {}", self.to_string()))?;

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        collection.rollback_insertion_like_command(lsn)?;

        wal.commit()?;
        Ok(())
    }
}

impl CQAction for InsertCommand {
    fn to_string(&self) -> String {
        format!(
            "INSERT {};{}",
            parse_string_from_vector_option(Some(&self.vector)),
            self.payload
        )
    }
}
