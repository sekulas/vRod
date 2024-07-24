use std::{
    fs::File,
    hash::Hash,
    hash::{DefaultHasher, Hasher},
    path::PathBuf,
};

use serde::{Deserialize, Serialize};

pub struct IdOffsetStorage {
    path: PathBuf,
    file: File,
    header: IdOffsetStorageHeader,
}

#[derive(Serialize, Deserialize)]
pub struct IdOffsetStorageHeader {
    current_max_lsn: u64,
    current_max_id: u64,
    checksum: u64,
}

impl IdOffsetStorageHeader {}

impl IdOffsetStorageHeader {
    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for IdOffsetStorageHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.current_max_lsn.hash(state);
        self.current_max_id.hash(state);
    }
}
