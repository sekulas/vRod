use std::fmt;

use crate::types::{PointIdType, PointOffsetType};

pub type IdTrackerSS = dyn IdTracker + Send + Sync;

pub trait IdTracker: fmt::Debug {
    fn add_new_external_id(&mut self, external_id: PointIdType) -> PointOffsetType;
    fn get_external_id(&self, internal_id: PointOffsetType) -> PointIdType;
    fn iter_internal_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_>;
}

#[derive(Debug, Default)]
pub struct IdTrackerImpl {
    internal_to_external: Vec<PointOffsetType>,
}

impl IdTrackerImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IdTracker for IdTrackerImpl {
    fn add_new_external_id(&mut self, external_id: PointIdType) -> PointOffsetType {
        let internal_id = self.internal_to_external.len() as PointOffsetType;
        self.internal_to_external.push(external_id);
        internal_id
    }

    fn get_external_id(&self, internal_id: PointOffsetType) -> PointIdType {
        self.internal_to_external[internal_id as usize]
    }

    fn iter_internal_ids(&self) -> Box<dyn Iterator<Item = PointOffsetType> + '_> {
        Box::new(0..self.internal_to_external.len() as PointOffsetType)
    }
}
