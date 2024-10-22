use std::fmt;

use crate::types::PointIdType;

pub type IdTrackerSS = dyn IdTracker + Send + Sync;

pub trait IdTracker: fmt::Debug {
    fn add_new_external_id(&mut self, external_id: PointIdType) -> PointIdType;
    fn get_external_id(&self, internal_id: PointIdType) -> PointIdType;
    fn iter_internal_ids(&self) -> Box<dyn Iterator<Item = PointIdType> + '_>;
}

#[derive(Debug, Default)]
pub struct IdTrackerImpl {
    internal_to_external: Vec<PointIdType>,
}

impl IdTrackerImpl {
    pub fn new() -> Self {
        Self::default()
    }
}

impl IdTracker for IdTrackerImpl {
    fn add_new_external_id(&mut self, external_id: PointIdType) -> PointIdType {
        let internal_id = self.internal_to_external.len() as PointIdType;
        self.internal_to_external.push(external_id);
        internal_id
    }

    fn get_external_id(&self, internal_id: PointIdType) -> PointIdType {
        self.internal_to_external[internal_id as usize]
    }

    fn iter_internal_ids(&self) -> Box<dyn Iterator<Item = PointIdType> + '_> {
        Box::new(0..self.internal_to_external.len() as PointIdType)
    }
}
