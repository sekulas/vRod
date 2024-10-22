use core::fmt;
use std::borrow::Cow;

use crate::types::{PointIdType, VectorElementType};

pub type VectorStorageSS = dyn VectorStorage + Send + Sync;

pub trait VectorStorage: fmt::Debug {
    fn total_vector_count(&self) -> usize;
    fn get(&self, key: PointIdType) -> &[VectorElementType];
    fn get_opt(&self, key: PointIdType) -> Option<&[VectorElementType]>;
    fn get_vector(&self, point_id: PointIdType) -> Cow<[VectorElementType]>;
    fn get_vector_opt(&self, point_id: PointIdType) -> Option<Cow<[VectorElementType]>>;
}

#[derive(Debug, Default)]
pub struct VectorStorageImpl {
    vectors: Vec<Vec<VectorElementType>>,
}

impl VectorStorageImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl VectorStorageImpl {
    pub fn add_vector(&mut self, vector: Vec<VectorElementType>) {
        self.vectors.push(vector);
    }
}

impl VectorStorage for VectorStorageImpl {
    fn total_vector_count(&self) -> usize {
        self.vectors.len()
    }

    fn get(&self, key: PointIdType) -> &[VectorElementType] {
        self.get_opt(key).expect("vector not found")
    }

    fn get_opt(&self, key: PointIdType) -> Option<&[VectorElementType]> {
        self.vectors.get(key as usize).map(|v| v.as_slice())
    }

    fn get_vector(&self, key: PointIdType) -> Cow<[VectorElementType]> {
        self.get_vector_opt(key).expect("vector not found")
    }

    /// Get vector by key, if it exists.
    fn get_vector_opt(&self, key: PointIdType) -> Option<Cow<[VectorElementType]>> {
        self.vectors
            .get(key as usize)
            .map(|slice| Cow::Borrowed(slice.as_slice())) // TODO: CHECK THIS
    }
}
