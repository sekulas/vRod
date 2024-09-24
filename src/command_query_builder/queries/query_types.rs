use super::{Error, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::command_query_builder::{CQAction, Query};

pub struct ListCollectionsQuery {
    pub db_path: PathBuf,
}

impl ListCollectionsQuery {
    pub fn new(db_path: &Path) -> Self {
        ListCollectionsQuery {
            db_path: db_path.to_owned(),
        }
    }
}

impl Query for ListCollectionsQuery {
    fn execute(&mut self) -> Result<()> {
        let entries = fs::read_dir(&self.db_path)?;
        let mut any_collections: bool = false;

        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                println!(
                    "{}",
                    path.file_name()
                        .ok_or(Error::CollectionPathProblem(path.to_owned()))?
                        .to_str()
                        .ok_or(Error::CollectionNameToStrProblem(path.to_owned()))?
                );
                any_collections = true;
            }
        }

        if !any_collections {
            println!("No collections.");
        }

        Ok(())
    }
}

impl CQAction for ListCollectionsQuery {
    fn to_string(&self) -> String {
        format!("LISTCOLLECTIONS {}", self.db_path.to_string_lossy())
    }
}

pub struct SearchSimilarQuery {
    pub collection_name: Option<String>,
    pub arg: Option<String>,
}

impl Query for SearchSimilarQuery {
    fn execute(&mut self) -> Result<()> {
        todo!("Not implemented.")
    }
}

impl CQAction for SearchSimilarQuery {
    fn to_string(&self) -> String {
        todo!();
    }
}
