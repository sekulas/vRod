use std::{
    cmp::max,
    path::{Path, PathBuf},
};

use super::{Error, Result};

use serde::{Deserialize, Serialize};

use crate::{
    entry_points::{EntryPoint, EntryPoints},
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    graph_links_bin::{GraphLinks, GraphLinksImpl},
    io_ops::{read_bin, save_bin},
    scorer::FilteredScorer,
    search_context::SearchContext,
    types::{PointIdType, ScoredPointOffset},
    visited_pool::{VisitedListHandle, VisitedPool},
};

pub type LinkContainer = Vec<PointIdType>;

pub const HNSW_GRAPH_FILE: &str = "graph.bin";
pub const HNSW_LINKS_FILE: &str = "links.bin";

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayers {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,

    #[serde(skip)]
    pub(super) links: GraphLinksImpl,
    pub(super) entry_points: EntryPoints,

    #[serde(skip)]
    pub(super) visited_pool: VisitedPool,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle;

    fn links_map<F>(&self, point_id: PointIdType, level: usize, f: F)
    where
        F: FnMut(PointIdType);

    fn get_m(&self, level: usize) -> usize;

    /// Greedy search for closest points within a single graph layer
    fn _search_on_level(
        &self,
        searcher: &mut SearchContext,
        level: usize,
        visited_list: &mut VisitedListHandle,
        points_scorer: &mut FilteredScorer,
    ) {
        let limit = self.get_m(level);
        let mut points_ids: Vec<PointIdType> = Vec::with_capacity(2 * limit);

        while let Some(candidate) = searcher.candidates.pop() {
            if candidate.score < searcher.lower_bound() {
                break;
            }

            points_ids.clear();
            self.links_map(candidate.idx, level, |link| {
                if !visited_list.check(link) {
                    points_ids.push(link);
                }
            });

            let scores = points_scorer.score_points(&mut points_ids, limit);
            scores.iter().copied().for_each(|score_point| {
                searcher.process_candidate(score_point);
                visited_list.check_and_update_visited(score_point.idx);
            });
        }
    }

    fn search_on_level(
        &self,
        level_entry: ScoredPointOffset,
        level: usize,
        ef: usize,
        points_scorer: &mut FilteredScorer,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef);

        self._search_on_level(&mut search_context, level, &mut visited_list, points_scorer);
        search_context.nearest
    }

    /// Greedy searches for entry point of level `target_level`.
    /// Beam size is 1.
    fn search_entry(
        &self,
        entry_point: PointIdType,
        top_level: usize,
        target_level: usize,
        points_scorer: &mut FilteredScorer,
    ) -> ScoredPointOffset {
        let mut links: Vec<PointIdType> = Vec::with_capacity(2 * self.get_m(0));

        let mut current_point = ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        };
        for level in rev_range(top_level, target_level) {
            //TODO: Maybe can be done differently
            let limit = self.get_m(level);

            let mut changed = true;
            while changed {
                changed = false;

                links.clear();
                self.links_map(current_point.idx, level, |link| {
                    links.push(link);
                });

                let scores = points_scorer.score_points(&mut links, limit);
                scores.iter().copied().for_each(|score_point| {
                    if score_point.score > current_point.score {
                        changed = true;
                        current_point = score_point;
                    }
                });
            }
        }
        current_point
    }
}

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointIdType, level: usize, mut f: F)
    where
        F: FnMut(PointIdType),
    {
        for link in self.links.links(point_id, level) {
            f(*link);
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

impl GraphLayers {
    fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_points.get_entry_point()
    }

    pub fn search(
        &self,
        top: usize,
        ef: usize,
        mut points_scorer: FilteredScorer,
    ) -> Vec<ScoredPointOffset> {
        let Some(entry_point) = self.get_entry_point() else {
            return Vec::default();
        };

        let zero_level_entry = self.search_entry(
            entry_point.point_id,
            entry_point.level,
            0,
            &mut points_scorer,
        );
        let nearest = self.search_on_level(zero_level_entry, 0, max(top, ef), &mut points_scorer);
        nearest.into_iter().take(top).collect()
    }

    pub fn get_path(path: &Path) -> PathBuf {
        path.join(HNSW_GRAPH_FILE)
    }

    pub fn get_links_path(path: &Path) -> PathBuf {
        path.join(HNSW_LINKS_FILE)
    }
}

impl GraphLayers {
    pub fn load(graph_path: &Path, links_path: &Path) -> Result<Self> {
        let mut slf: GraphLayers = match links_path.exists() {
            true => read_bin(graph_path),
            false => Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("Links file does not exist: {links_path:?}"),
            ))),
        }?;

        let links = GraphLinksImpl::load_from_file(links_path)?;
        slf.links = links;
        Ok(slf)
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        save_bin(path, self)
    }
}

fn rev_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}
