// Inspired by Project qdrant (https://github.com/qdrant/qdrant/tree/master)
// Licensed under the Apache License, Version 2.0 (the "License");
// See: http://www.apache.org/licenses/LICENSE-2.0
// Modifications made by sekulas, 2024:
// - one entry point.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct EntryPoint {
    pub point_id: PointOffsetType,
    pub level: usize,
}

impl PartialOrd for EntryPoint {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EntryPoint {
    fn cmp(&self, other: &Self) -> Ordering {
        self.level.cmp(&other.level)
    }
}

#[derive(Deserialize, Serialize, Clone, Debug)]
pub struct EntryPointContainer {
    entry_point: Option<EntryPoint>,
}

impl EntryPointContainer {
    pub fn new() -> Self {
        Self { entry_point: None }
    }

    pub fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_point.clone()
    }

    pub fn set_if_higher(&mut self, new_point: PointOffsetType, level: usize) {
        match &self.entry_point {
            Some(current_highest) if current_highest.level >= level => (),
            _ => {
                self.entry_point = Some(EntryPoint {
                    point_id: new_point,
                    level,
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_if_higher_should_set_new_point() {
        let mut entry_point_container = EntryPointContainer::new();

        entry_point_container.set_if_higher(1, 1);

        assert_eq!(
            entry_point_container.get_entry_point(),
            Some(EntryPoint {
                point_id: 1,
                level: 1
            })
        );
    }

    #[test]
    fn set_if_higher_should_not_update_entry_point_if_lower_level() {
        let mut entry_point_container = EntryPointContainer::new();

        entry_point_container.set_if_higher(1, 1);
        entry_point_container.set_if_higher(2, 0);

        assert_eq!(
            entry_point_container.get_entry_point(),
            Some(EntryPoint {
                point_id: 1,
                level: 1
            })
        );
    }

    #[test]
    fn set_if_higher_should_update_entry_point_if_higher_level_and_point() {
        let mut entry_point_container = EntryPointContainer::new();

        entry_point_container.set_if_higher(1, 1);
        entry_point_container.set_if_higher(2, 2);

        assert_eq!(
            entry_point_container.get_entry_point(),
            Some(EntryPoint {
                point_id: 2,
                level: 2
            })
        );
    }
}
