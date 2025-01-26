// Inspired by Project qdrant (https://github.com/qdrant/qdrant/tree/master)
// Licensed under the Apache License, Version 2.0 (the "License");
// See: http://www.apache.org/licenses/LICENSE-2.0
// Modifications made by sekulas, 2024:
// - used existing trait methods for in memory vector_storage,
// - made VectorStorageImpl.

use core::fmt;
use std::borrow::Cow;

use crate::types::{Distance, PointOffsetType, VectorElementType};

pub type VectorStorageSS = dyn VectorStorage + Send + Sync;

pub trait VectorStorage: fmt::Debug {
    fn total_vector_count(&self) -> usize;
    fn get(&self, key: PointOffsetType) -> &[VectorElementType];
    fn get_opt(&self, key: PointOffsetType) -> Option<&[VectorElementType]>;
    fn get_vector(&self, point_id: PointOffsetType) -> Cow<[VectorElementType]>;
    fn get_vector_opt(&self, point_id: PointOffsetType) -> Option<Cow<[VectorElementType]>>;
}

#[derive(Debug)]
pub struct VectorStorageImpl {
    vectors: Vec<Vec<VectorElementType>>,
    distance: Distance,
}

impl VectorStorageImpl {
    pub fn new(distance: &Distance) -> Self {
        Self {
            vectors: Vec::new(),
            distance: *distance,
        }
    }
}

impl VectorStorageImpl {
    pub fn add_vector(&mut self, vector: Vec<VectorElementType>) {
        self.vectors.push(self.distance.preprocess_vec(vector));
    }
}

impl VectorStorage for VectorStorageImpl {
    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get(&self, key: PointOffsetType) -> &[VectorElementType] {
        self.get_opt(key).expect("vector not found")
    }

    fn get_opt(&self, key: PointOffsetType) -> Option<&[VectorElementType]> {
        self.vectors.get(key as usize).map(|v| v.as_slice())
    }

    fn get_vector(&self, key: PointOffsetType) -> Cow<[VectorElementType]> {
        self.get_vector_opt(key).expect("vector not found")
    }

    fn get_vector_opt(&self, key: PointOffsetType) -> Option<Cow<[VectorElementType]>> {
        self.vectors
            .get(key as usize)
            .map(|slice| Cow::Borrowed(slice.as_slice()))
    }
}
