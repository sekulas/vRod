use super::Result;
use bitvec::vec::BitVec;
use rand::{distributions::Uniform, Rng};
use std::{
    cmp::{max, min},
    collections::BinaryHeap,
    path::Path,
    sync::atomic::AtomicUsize,
};

use parking_lot::{Mutex, RwLock};

use crate::{
    entry_points::EntryPoints,
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    graph_layers::{GraphLayers, GraphLayersBase, LinkContainer},
    graph_links_bin::{GraphLinks, GraphLinksConverter},
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
    // Factor of level probability
    level_factor: f64,
    // Exclude points according to "not closer than base" heuristic?
    use_heuristic: bool,
    links_layers: Vec<LockedLayersContainer>,
    entry_points: Mutex<EntryPoints>,

    // Fields used on construction phase only
    visited_pool: VisitedPool,

    // List of bool flags, which defines if the point is already indexed or not
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

    fn get_m(&self, level: usize) -> usize {
        if level == 0 {
            self.m0
        } else {
            self.m
        }
    }
}

impl GraphLayersBuilder {
    pub fn new_with_params(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
        reserve: bool,
    ) -> Self {
        let links_layers = std::iter::repeat_with(|| {
            vec![RwLock::new(if reserve {
                Vec::with_capacity(m0)
            } else {
                vec![]
            })]
        })
        .take(num_vectors)
        .collect();

        let ready_list = RwLock::new(BitVec::repeat(false, num_vectors));

        Self {
            max_level: AtomicUsize::new(0),
            m,
            m0,
            ef_construct,
            level_factor: 1.0 / (max(m, 2) as f64).ln(),
            use_heuristic,
            links_layers,
            entry_points: Mutex::new(EntryPoints::new(entry_points_num)),
            visited_pool: VisitedPool::new(),
            ready_list,
        }
    }

    pub fn new(
        num_vectors: usize, // Initial number of points in index
        m: usize,           // Expected M for non-first layer
        m0: usize,          // Expected M for first layer
        ef_construct: usize,
        entry_points_num: usize, // Depends on number of points
        use_heuristic: bool,
    ) -> Self {
        Self::new_with_params(
            num_vectors,
            m,
            m0,
            ef_construct,
            entry_points_num,
            use_heuristic,
            true,
        )
    }

    pub fn into_graph_layers(self, path: Option<&Path>) -> Result<GraphLayers> {
        let unlocker_links_layers = self
            .links_layers
            .into_iter()
            .map(|l| l.into_iter().map(|l| l.into_inner()).collect())
            .collect();

        let mut links_converter = GraphLinksConverter::new(unlocker_links_layers);
        if let Some(path) = path {
            links_converter.save_as(path)?;
        }

        let links = GraphLinks::from_converter(links_converter)?;
        Ok(GraphLayers {
            m: self.m,
            m0: self.m0,
            ef_construct: self.ef_construct,
            links,
            entry_points: self.entry_points.into_inner(),
            visited_pool: self.visited_pool,
        })
    }

    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.num_points())
    }

    /// Generate random level for a new point, according to geometric distribution
    pub fn get_random_layer<R>(&self, rng: &mut R) -> usize
    where
        R: Rng + ?Sized,
    {
        let distribution = Uniform::new(0.0, 1.0);
        let sample: f64 = rng.sample(distribution);
        let picked_level = -sample.ln() * self.level_factor;
        picked_level.round() as usize
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

    /// Connect new point to links, so that links contains only closest points
    fn connect_new_point<F>(
        links: &mut LinkContainer,
        new_point_id: PointIdType,
        target_point_id: PointIdType,
        level_m: usize,
        mut score_internal: F,
    ) where
        F: FnMut(PointIdType, PointIdType) -> ScoreType,
    {
        // ToDo: binary search here ? (most likely does not worth it)
        let new_to_target = score_internal(target_point_id, new_point_id);

        let mut id_to_insert = links.len();
        for (i, &item) in links.iter().enumerate() {
            let target_to_link = score_internal(target_point_id, item);
            if target_to_link < new_to_target {
                id_to_insert = i;
                break;
            }
        }

        if links.len() < level_m {
            links.insert(id_to_insert, new_point_id);
        } else if id_to_insert != links.len() {
            links.pop();
            links.insert(id_to_insert, new_point_id);
        }
    }

    /// <https://github.com/nmslib/hnswlib/issues/99>
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
            let mut is_good = true;
            for &selected_point in &result_list {
                let dist_to_already_selected = score_internal(current_closest.idx, selected_point);
                if dist_to_already_selected > current_closest.score {
                    is_good = false;
                    break;
                }
            }
            if is_good {
                result_list.push(current_closest.idx);
            }
        }

        result_list
    }

    /// <https://github.com/nmslib/hnswlib/issues/99>
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

    pub fn link_new_point(&self, point_id: PointIdType, mut points_scorer: FilteredScorer) {
        // Check if there is an suitable entry point
        //   - entry point level if higher or equal
        //   - it satisfies filters

        let level = self.get_point_level(point_id);

        let entry_point_opt = self.entry_points.lock().get_entry_point();
        match entry_point_opt {
            // New point is a new empty entry (for this filter, at least)
            // We can't do much here, so just quit
            None => {}

            // Entry point found.
            Some(entry_point) => {
                let mut level_entry = if entry_point.level > level {
                    // The entry point is higher than a new point
                    // Let's find closest one on same level

                    // greedy search for a single closest point
                    self.search_entry(
                        entry_point.point_id,
                        entry_point.level,
                        level,
                        &mut points_scorer,
                    )
                } else {
                    ScoredPointOffset {
                        idx: entry_point.point_id,
                        score: points_scorer.score_internal(point_id, entry_point.point_id),
                    }
                };
                // minimal common level for entry points
                let linking_level = min(level, entry_point.level);

                for curr_level in (0..=linking_level).rev() {
                    let level_m = self.get_m(curr_level);
                    let mut visited_list = self.get_visited_list_from_pool();

                    visited_list.check_and_update_visited(level_entry.idx);

                    let mut search_context = SearchContext::new(level_entry, self.ef_construct);

                    self._search_on_level(
                        &mut search_context,
                        curr_level,
                        &mut visited_list,
                        &mut points_scorer,
                    );

                    if let Some(the_nearest) = search_context.nearest.iter().max() {
                        level_entry = *the_nearest;
                    }

                    let scorer = |a, b| points_scorer.score_internal(a, b);

                    if self.use_heuristic {
                        let selected_nearest = {
                            let mut existing_links =
                                self.links_layers[point_id as usize][curr_level].write();

                            {
                                //TODO: TO REMOVE PROBABLY
                                let ready_list = self.ready_list.read();
                                for &existing_link in existing_links.iter() {
                                    if !visited_list.check(existing_link)
                                        && ready_list[existing_link as usize]
                                    {
                                        search_context.process_candidate(ScoredPointOffset {
                                            idx: existing_link,
                                            score: points_scorer.score_point(existing_link),
                                        });
                                    }
                                }
                            }

                            let selected_nearest = Self::select_candidates_with_heuristic(
                                search_context.nearest,
                                level_m,
                                scorer,
                            );
                            existing_links.clone_from(&selected_nearest);
                            selected_nearest
                        };

                        for &other_point in &selected_nearest {
                            let mut other_point_links =
                                self.links_layers[other_point as usize][curr_level].write();
                            if other_point_links.len() < level_m {
                                // If linked point is lack of neighbours
                                other_point_links.push(point_id);
                            } else {
                                let mut candidates = BinaryHeap::with_capacity(level_m + 1);
                                candidates.push(ScoredPointOffset {
                                    idx: point_id,
                                    score: scorer(point_id, other_point),
                                });
                                for other_point_link in
                                    other_point_links.iter().take(level_m).copied()
                                {
                                    candidates.push(ScoredPointOffset {
                                        idx: other_point_link,
                                        score: scorer(other_point_link, other_point),
                                    });
                                }
                                let selected_candidates =
                                    Self::select_candidate_with_heuristic_from_sorted(
                                        candidates.into_sorted_vec().into_iter().rev(),
                                        level_m,
                                        scorer,
                                    );
                                other_point_links.clear(); // this do not free memory, which is good
                                for selected in selected_candidates.iter().copied() {
                                    other_point_links.push(selected);
                                }
                            }
                        }
                    } else {
                        for nearest_point in &search_context.nearest {
                            {
                                let mut links =
                                    self.links_layers[point_id as usize][curr_level].write();
                                Self::connect_new_point(
                                    &mut links,
                                    nearest_point.idx,
                                    point_id,
                                    level_m,
                                    scorer,
                                );
                            }

                            {
                                let mut links = self.links_layers[nearest_point.idx as usize]
                                    [curr_level]
                                    .write();
                                Self::connect_new_point(
                                    &mut links,
                                    point_id,
                                    nearest_point.idx,
                                    level_m,
                                    scorer,
                                );
                            }
                        }
                    }
                }
            }
        }
        self.ready_list.write().set(point_id as usize, true);
        self.entry_points.lock().new_point(point_id, level);
    }

    fn num_points(&self) -> usize {
        self.links_layers.len()
    }
}
