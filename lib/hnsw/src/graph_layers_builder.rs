use super::Result;
use bitvec::vec::BitVec;
use rand::{distributions::Uniform, Rng};
use std::{
    cmp::min,
    collections::{BinaryHeap, HashMap},
    path::Path,
    sync::atomic::AtomicUsize,
};

use parking_lot::{lock_api, Mutex, RwLock};

use crate::{
    entry_point::{EntryPoint, EntryPointContainer},
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    graph_layers::{GraphLayers, GraphLayersBase, LinkContainer},
    graph_links::{GraphLinks, GraphLinksConverter},
    scorer::FilteredScorer,
    search_context::SearchContext,
    types::{PointIdType, ScoreType, ScoredPointOffset},
    visited_pool::{VisitedListHandle, VisitedPool},
};

pub type LockedLinkContainer = RwLock<LinkContainer>;
pub type LockedLayersContainer = Vec<LockedLinkContainer>;

pub struct GraphLayersBuilder {
    max_level: AtomicUsize,
    m: usize,
    m0: usize,
    ef_construct: usize,
    level_factor: f64,
    use_heuristic: bool,
    links_layers: Vec<LockedLayersContainer>,
    entry_point: Mutex<EntryPointContainer>,
    visited_pool: VisitedPool,
    ready_list: RwLock<BitVec>,
}

impl GraphLayersBase for GraphLayersBuilder {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.num_points())
    }

    fn links_map<F>(&self, point_id: PointIdType, level: usize, mut f: F)
    where
        F: FnMut(PointIdType),
    {
        let links = self.links_layers[point_id as usize][level].read();
        let ready_list = self.ready_list.read();
        for link in links.iter() {
            if ready_list[*link as usize] {
                f(*link);
            }
        }
    }

    fn get_layer_max_links(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }
}

impl GraphLayersBuilder {
    pub fn new(
        num_vectors: usize,
        m: usize,
        m0: usize,
        ef_construct: usize,
        use_heuristic: bool,
    ) -> Self {
        Self {
            max_level: AtomicUsize::new(0),
            m,
            m0,
            ef_construct,
            level_factor: 1.0 / (m.max(2) as f64).ln(),
            use_heuristic,
            links_layers: Self::initialize_links_layers(num_vectors, m0),
            entry_point: Mutex::new(EntryPointContainer::new()),
            visited_pool: VisitedPool::new(),
            ready_list: RwLock::new(BitVec::repeat(false, num_vectors)),
        }
    }

    fn initialize_links_layers(
        num_vectors: usize,
        initial_capacity: usize,
    ) -> Vec<Vec<RwLock<Vec<PointIdType>>>> {
        (0..num_vectors)
            .map(|_| vec![RwLock::new(Vec::with_capacity(initial_capacity))])
            .collect()
    }

    pub fn into_graph_layers(self, path: &Path) -> Result<GraphLayers> {
        let unlocker_links_layers = self
            .links_layers
            .into_iter()
            .map(|l| l.into_iter().map(|l| l.into_inner()).collect())
            .collect();

        let mut links_converter = GraphLinksConverter::new(unlocker_links_layers);
        links_converter.save_as(path)?;

        let links = GraphLinks::from_converter(links_converter)?;
        Ok(GraphLayers {
            m: self.m,
            m0: self.m0,
            ef_construct: self.ef_construct,
            links,
            entry_point: self.entry_point.into_inner(),
            visited_pool: self.visited_pool,
        })
    }

    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.num_points())
    }

    pub fn get_random_layer<R: Rng>(&self, rng: &mut R) -> usize {
        let sample: f64 = rng.sample(Uniform::new(0.0, 1.0));
        (-sample.ln() * self.level_factor).round() as usize
    }

    fn get_point_level(&self, point_id: PointIdType) -> usize {
        self.links_layers[point_id as usize].len() - 1
    }

    pub fn set_levels(&mut self, point_id: PointIdType, level: usize) {
        if self.links_layers.len() <= point_id as usize {
            while self.links_layers.len() <= point_id as usize {
                self.links_layers.push(vec![]);
            }
        }
        let point_layers = &mut self.links_layers[point_id as usize];
        while point_layers.len() <= level {
            let links = Vec::with_capacity(self.m);
            point_layers.push(RwLock::new(links));
        }
        self.max_level
            .fetch_max(level, std::sync::atomic::Ordering::Relaxed);
    }

    fn select_candidate_with_heuristic_from_sorted<F>(
        candidates: impl Iterator<Item = ScoredPointOffset>,
        m: usize,
        mut score_internal: F,
    ) -> Vec<PointIdType>
    where
        F: FnMut(PointIdType, PointIdType) -> ScoreType,
    {
        let mut result_list = Vec::with_capacity(m);
        for current_closest in candidates {
            if result_list.len() >= m {
                break;
            }

            let is_good = result_list.iter().all(|&selected_point| {
                let dist_to_selected = score_internal(current_closest.idx, selected_point);
                dist_to_selected <= current_closest.score
            });

            if is_good {
                result_list.push(current_closest.idx);
            }
        }

        result_list
    }

    fn select_candidates_with_heuristic<F>(
        candidates: FixedLengthPriorityQueue<ScoredPointOffset>,
        m: usize,
        score_internal: F,
    ) -> Vec<PointIdType>
    where
        F: FnMut(PointIdType, PointIdType) -> ScoreType,
    {
        let closest_iter = candidates.into_iter();
        Self::select_candidate_with_heuristic_from_sorted(closest_iter, m, score_internal)
    }

    pub fn link_new_point(&self, point_id: PointIdType, mut scorer: FilteredScorer) {
        let level = self.get_point_level(point_id);

        if let Some(entry_point) = self.get_entry_point() {
            let level_entry = self.get_level_entry(&entry_point, level, &mut scorer, point_id);
            let linking_level = min(level, entry_point.level);

            for curr_level in (0..=linking_level).rev() {
                self.link_point_at_level(point_id, level_entry, curr_level, &mut scorer);
            }
        }

        self.mark_point_ready(point_id);
        self.update_entry_point(point_id, level);
    }

    fn get_level_entry(
        &self,
        entry_point: &EntryPoint,
        level: usize,
        points_scorer: &mut FilteredScorer,
        point_id: PointIdType,
    ) -> ScoredPointOffset {
        if entry_point.level > level {
            self.search_entry(
                entry_point.point_id,
                entry_point.level,
                level,
                points_scorer,
            )
        } else {
            ScoredPointOffset {
                idx: entry_point.point_id,
                score: points_scorer.score_internal(point_id, entry_point.point_id),
            }
        }
    }

    fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_point.lock().get_entry_point()
    }

    fn make_nearest_point_level_entry(
        search_context: &SearchContext,
        level_entry: &mut ScoredPointOffset,
    ) {
        if let Some(the_nearest) = search_context.nearest.iter().max() {
            *level_entry = *the_nearest;
        }
    }

    fn get_links_for_point_on_level(
        &self,
        point_id: PointIdType,
        level: usize,
    ) -> lock_api::RwLockWriteGuard<'_, parking_lot::RawRwLock, LinkContainer> {
        self.links_layers[point_id as usize][level].write()
    }

    fn reconsider_links_with_new_point<F>(
        &self,
        point: PointIdType,
        level_m: usize,
        mut links: lock_api::RwLockWriteGuard<'_, parking_lot::RawRwLock, LinkContainer>,
        new_point: PointIdType,
        scorer: F,
    ) where
        F: Fn(PointIdType, PointIdType) -> ScoreType,
    {
        if links.len() < level_m {
            links.push(new_point);
        } else {
            let mut candidates = BinaryHeap::with_capacity(level_m + 1);

            candidates.push(ScoredPointOffset {
                idx: new_point,
                score: scorer(new_point, point),
            });

            links
                .iter()
                .take(level_m)
                .copied()
                .for_each(|linked_neighbour| {
                    candidates.push(ScoredPointOffset {
                        idx: linked_neighbour,
                        score: scorer(linked_neighbour, point),
                    });
                });

            let selected_candidates = Self::select_candidate_with_heuristic_from_sorted(
                candidates.into_sorted_vec().into_iter().rev(),
                level_m,
                scorer,
            );

            links.clear();
            links.extend(selected_candidates);
        }
    }

    fn link_point_at_level(
        &self,
        point_id: PointIdType,
        mut level_entry: ScoredPointOffset,
        level: usize,
        points_scorer: &mut FilteredScorer,
    ) {
        let level_m = self.get_layer_max_links(level);
        let mut visited_list = self.get_visited_list_from_pool();

        visited_list.check_and_update_visited(level_entry.idx);

        let mut search_context = SearchContext::new(level_entry, self.ef_construct);

        self._search_on_level(&mut search_context, level, &mut visited_list, points_scorer);

        Self::make_nearest_point_level_entry(&search_context, &mut level_entry);

        let scorer = |a, b| points_scorer.score_internal(a, b);

        if self.use_heuristic {
            let selected_nearest = {
                let mut existing_links = self.get_links_for_point_on_level(point_id, level);

                let selected_nearest =
                    Self::select_candidates_with_heuristic(search_context.nearest, level_m, scorer);

                existing_links.clone_from(&selected_nearest);
                selected_nearest
            };

            for &other_point in &selected_nearest {
                let other_point_links = self.get_links_for_point_on_level(other_point, level);
                self.reconsider_links_with_new_point(
                    other_point,
                    level_m,
                    other_point_links,
                    point_id,
                    scorer,
                );
            }
        } else {
            panic!("TODO: Implement this part"); //TODO: Implement this part??
        }
    }

    fn mark_point_ready(&self, point_id: PointIdType) {
        self.ready_list.write().set(point_id as usize, true);
    }

    fn update_entry_point(&self, point_id: PointIdType, level: usize) {
        self.entry_point.lock().set_if_higher(point_id, level);
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }

    pub fn print_layer_diagnostics(&self) {
        let mut layer_counts: HashMap<usize, usize> = HashMap::new();

        for layer in &self.links_layers {
            let max_level = layer.len();
            *layer_counts.entry(max_level).or_insert(0) += 1;
        }

        for (level, count) in layer_counts {
            println!("Level: {} - Points: {}", level, count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::thread_rng;

    #[test]
    fn test_graph_layers_builder_creation() {
        let builder = GraphLayersBuilder::new(100, 16, 32, 200, true);
        assert_eq!(builder.links_layers.len(), 100);
    }

    #[test]
    fn test_random_layer_generation() {
        let builder = GraphLayersBuilder::new(100, 16, 32, 200, true);
        let mut rng = thread_rng();

        for _ in 0..100 {
            let layer = builder.get_random_layer(&mut rng);
            assert!(layer < 10);
        }
    }
}
