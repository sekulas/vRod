use std::collections::BinaryHeap;

use crate::{
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    types::{ScoreType, ScoredPointOffset},
};

pub struct SearchContext {
    pub nearest: FixedLengthPriorityQueue<ScoredPointOffset>,
    pub candidates: BinaryHeap<ScoredPointOffset>,
}

impl SearchContext {
    pub fn new(entry_point: ScoredPointOffset, ef: usize) -> Self {
        let mut nearest = FixedLengthPriorityQueue::new(ef);
        nearest.push(entry_point);
        SearchContext {
            nearest,
            candidates: BinaryHeap::from_iter([entry_point]),
        }
    }

    pub fn lower_bound(&self) -> ScoreType {
        match self.nearest.top() {
            None => ScoreType::MIN,
            Some(worst_of_the_best) => worst_of_the_best.score,
        }
    }

    pub fn process_candidate(&mut self, score_point: ScoredPointOffset) {
        let was_added = match self.nearest.push(score_point) {
            None => true,
            Some(removed) => removed.idx != score_point.idx,
        };
        if was_added {
            self.candidates.push(score_point);
        }
    }
}
