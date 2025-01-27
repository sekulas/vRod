// Inspired by Project qdrant (https://github.com/qdrant/qdrant/tree/master)
// Licensed under the Apache License, Version 2.0 (the "License");
// See: http://www.apache.org/licenses/LICENSE-2.0
// Modifications made by sekulas, 2024:
// - simplified structs to meet project requirements. 

use super::Result;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use validator::Validate;

use crate::{
    io_ops::{read_json, save_json},
    types::{Distance, HNSW_INDEX_CONFIG_FILE},
};

#[derive(Debug, Deserialize, Serialize, Validate, Clone, PartialEq, Eq)]
pub struct HnswConfig {
    pub m: usize,
    #[validate(range(min = 4))]
    pub ef_construct: usize,
}

#[derive(Debug, Deserialize, Serialize, Copy, Clone, PartialEq, Eq)]
pub struct HnswGraphConfig {
    pub m: usize,
    pub m0: usize,
    pub ef_construct: usize,
    pub distance: Distance,
    #[serde(default)]
    pub indexed_vector_count: Option<usize>,
}

impl HnswGraphConfig {
    pub fn new(
        m: usize,
        ef_construct: usize,
        indexed_vector_count: usize,
        distance: Distance,
    ) -> Self {
        HnswGraphConfig {
            m,
            m0: m * 2,
            ef_construct,
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
