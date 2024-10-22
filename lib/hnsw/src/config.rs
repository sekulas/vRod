use super::Result;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    io_ops::{read_json, save_json},
    types::Distance,
};

pub const HNSW_INDEX_CONFIG_FILE: &str = "hnsw_config.json";

#[derive(Debug, Deserialize, Serialize, Validate, Clone, PartialEq, Eq)]
pub struct HnswConfig {
    /// Number of edges per node in the index graph. Larger the value - more accurate the search, more space required.
    pub m: usize,
    /// Number of neighbours to consider during the index building. Larger the value - more accurate the search, more time required to build index.
    #[validate(range(min = 4))]
    pub ef_construct: usize,
    /// Number of parallel threads used for background index building.
    /// If 0 - automatically select from 8 to 16.
    /// Best to keep between 8 and 16 to prevent likelihood of slow building or broken/inefficient HNSW graphs.
    /// On small CPUs, less threads are used.
    #[serde(default)]
    pub max_indexing_threads: usize,
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub struct HnswGraphConfig {
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub ef: usize,
    pub distance: Distance,
    #[serde(default)]
    pub max_indexing_threads: usize,
    #[serde(default)]
    pub indexed_vector_count: Option<usize>,
}

impl HnswGraphConfig {
    pub fn new(
        m: usize,
        ef_construct: usize,
        max_indexing_threads: usize,
        indexed_vector_count: usize,
        distance: Distance,
    ) -> Self {
        HnswGraphConfig {
            m,
            m0: m * 2,
            ef_construct,
            ef: ef_construct,
            max_indexing_threads,
            indexed_vector_count: Some(indexed_vector_count),
            distance,
        }
    }

    pub fn get_config_path(path: &Path) -> PathBuf {
        path.join(HNSW_INDEX_CONFIG_FILE)
    }

    pub fn load(path: &Path) -> Result<Self> {
        read_json(path)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        save_json(path, self)
    }
}
