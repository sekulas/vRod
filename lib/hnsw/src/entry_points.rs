use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::{fixed_length_priority_queue::FixedLengthPriorityQueue, types::PointIdType};

#[derive(Deserialize, Serialize, Clone, Debug, PartialEq, Eq)]
pub struct EntryPoint {
    pub point_id: PointIdType,
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
pub struct EntryPoints {
    entry_points: Vec<EntryPoint>,
    extra_entry_points: FixedLengthPriorityQueue<EntryPoint>,
}

impl EntryPoints {
    pub fn new(extra_entry_points: usize) -> Self {
        EntryPoints {
            entry_points: vec![],
            extra_entry_points: FixedLengthPriorityQueue::new(extra_entry_points),
        }
    }

    pub fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_points.first().cloned().or_else(|| {
            // Searching for at least some entry point
            self.extra_entry_points
                .iter()
                .cloned()
                .max_by_key(|ep| ep.level)
        })
    }

    pub fn new_point(&mut self, new_point: PointIdType, level: usize) -> Option<EntryPoint> {
        // there are 3 cases:
        // - There is proper entry point for a new point higher or same level - return the point
        // - The new point is higher than any alternative - return the next best thing
        // - There is no point and alternatives - return None

        if self.entry_points.is_empty() {
            // No entry points found. Create a new one and return self
            let new_entry = EntryPoint {
                point_id: new_point,
                level,
            };
            self.entry_points.push(new_entry);
            return None;
        }

        let candidate = &self.entry_points[0];

        // Found checkpoint candidate
        if candidate.level >= level {
            // The good checkpoint exists.
            // Return it, and also try to save given if required
            self.extra_entry_points.push(EntryPoint {
                point_id: new_point,
                level,
            });
            Some(candidate.clone())
        } else {
            // The current point is better than existing
            let entry = self.entry_points[0].clone();
            self.entry_points[0] = EntryPoint {
                point_id: new_point,
                level,
            };
            self.extra_entry_points.push(entry.clone());
            Some(entry)
        }
    }
}
