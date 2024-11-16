use std::path::{Path, PathBuf};

use hnsw::types::Distance;

use crate::types::HNSW_DIR_NAME;

pub fn get_vector_index_path(path: &Path, distance: &Distance) -> PathBuf {
    path.join(format!("{}_{}", HNSW_DIR_NAME, distance.to_string()))
}
