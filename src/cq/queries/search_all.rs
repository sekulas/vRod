use std::io::Write;
use std::path::PathBuf;

use super::Result;
use crate::{
    components::collection::Collection,
    cq::{
        queries::dto::RecordDTOList, types::SEARCH_ALL_Q_STR, CQAction, CQTarget, CQValidator,
        Query, Validator,
    },
};
pub struct SearchAllQuery {
    collection: CQTarget,
    file: Option<PathBuf>,
    arg: Option<String>,
}

impl SearchAllQuery {
    pub fn new(collection: CQTarget, file: Option<PathBuf>, arg: Option<String>) -> Self {
        Self {
            collection,
            file,
            arg,
        }
    }
}

impl Query for SearchAllQuery {
    fn execute(&self) -> Result<()> {
        CQValidator::target_exists(&self.collection);

        let path = self.collection.get_target_path();
        let mut collection = Collection::load(&path)?;

        let mut time = std::time::Instant::now();
        let mut result = collection.search_all()?;

        println!(
            "Found {} records in {}s.",
            result.len(),
            time.elapsed().as_secs_f32()
        );

        if let Some(arg) = RecordsOrder::from_str(&self.arg) {
            match arg {
                RecordsOrder::Asc => {
                    result.sort_by(|a, b| a.0.cmp(&b.0));
                }
                RecordsOrder::Desc => {}
            }
        }

        match &self.file {
            Some(file_path) => {
                time = std::time::Instant::now();
                println!("Saving to file...");

                let file = std::fs::File::create(file_path)?;
                let mut writer = std::io::BufWriter::new(file);

                write!(writer, "{}", RecordDTOList(result))?;

                println!("Saved in {}s.", time.elapsed().as_secs_f32());
            }
            None => {
                println!("{}", RecordDTOList(result));
            }
        }
        Ok(())
    }
}

impl CQAction for SearchAllQuery {
    fn to_string(&self) -> String {
        SEARCH_ALL_Q_STR.to_string()
    }
}

enum RecordsOrder {
    Asc,
    Desc,
}

impl RecordsOrder {
    fn from_str(arg: &Option<String>) -> Option<Self> {
        match arg {
            Some(arg) => match arg.to_uppercase().as_str() {
                "ASC" => Some(Self::Asc),
                "DESC" => Some(Self::Desc),
                _ => Some(Self::Desc),
            },
            None => Some(Self::Desc),
        }
    }
}
