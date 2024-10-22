use crate::types::PointIdType;
use lazy_static::lazy_static;
use parking_lot::RwLock;

lazy_static! {
    /// Max number of pooled elements to preserve in memory.
    /// Scaled according to the number of logical CPU cores to account for concurrent operations.
    pub static ref POOL_KEEP_LIMIT: usize = num_cpus::get().clamp(1, 16);
}

/// Keeps a list of `VisitedList` which could be requested and released from multiple threads
///
/// If there are more requests than lists - creates a new list, but only keeps max defined amount.
#[derive(Debug)]
pub struct VisitedPool {
    pool: RwLock<Vec<VisitedList>>,
}

impl VisitedPool {
    pub fn new() -> Self {
        VisitedPool {
            pool: RwLock::new(Vec::with_capacity(*POOL_KEEP_LIMIT)),
        }
    }

    pub fn get(&self, num_points: usize) -> VisitedListHandle {
        // If there are more concurrent requests, a new temporary list is created dynamically.
        // This limit is implemented to prevent memory leakage.
        match self.pool.write().pop() {
            None => VisitedListHandle::new(self, VisitedList::new(num_points)),
            Some(mut data) => {
                data.visit_counters.resize(num_points, 0);
                let mut visited_list = VisitedListHandle::new(self, data);
                visited_list.next_iteration();
                visited_list
            }
        }
    }

    fn return_back(&self, data: VisitedList) {
        let mut pool = self.pool.write();
        if pool.len() < *POOL_KEEP_LIMIT {
            pool.push(data);
        }
    }
}

impl Default for VisitedPool {
    fn default() -> Self {
        VisitedPool::new()
    }
}

/// Visited list reuses same memory to keep track of visited points ids among multiple consequent queries
///
/// It stores the sequence number of last processed operation next to the point ID, which allows to avoid memory allocation
/// and reuse same counter for multiple queries.
#[derive(Debug)]
struct VisitedList {
    current_iter: usize,
    visit_counters: Vec<usize>,
}

impl Default for VisitedList {
    fn default() -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![],
        }
    }
}

impl VisitedList {
    fn new(num_points: usize) -> Self {
        VisitedList {
            current_iter: 1,
            visit_counters: vec![0; num_points],
        }
    }
}

/// Visited list handle is an owner of the `VisitedList`, which is returned by `VisitedPool` and returned back to it
#[derive(Debug)]
pub struct VisitedListHandle<'a> {
    pool: &'a VisitedPool,
    visited_list: VisitedList,
}

impl<'a> Drop for VisitedListHandle<'a> {
    fn drop(&mut self) {
        self.pool
            .return_back(std::mem::take(&mut self.visited_list));
    }
}

impl<'a> VisitedListHandle<'a> {
    fn new(pool: &'a VisitedPool, data: VisitedList) -> Self {
        VisitedListHandle {
            pool,
            visited_list: data,
        }
    }

    /// Return `true` if visited
    pub fn check(&self, point_id: PointIdType) -> bool {
        self.visited_list
            .visit_counters
            .get(point_id as usize)
            .map_or(false, |x| *x >= self.visited_list.current_iter)
    }

    /// Updates visited list
    /// return `true` if point was visited before
    pub fn check_and_update_visited(&mut self, point_id: PointIdType) -> bool {
        let idx = point_id as usize;
        if idx >= self.visited_list.visit_counters.len() {
            self.visited_list.visit_counters.resize(idx + 1, 0);
        }
        let prev_value = self.visited_list.visit_counters[idx];
        self.visited_list.visit_counters[idx] = self.visited_list.current_iter;
        prev_value >= self.visited_list.current_iter
    }

    pub fn next_iteration(&mut self) {
        self.visited_list.current_iter += 1;
    }
}
