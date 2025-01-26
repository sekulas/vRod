// Inspired by Project qdrant (https://github.com/qdrant/qdrant/tree/master)
// Licensed under the Apache License, Version 2.0 (the "License");
// See: http://www.apache.org/licenses/LICENSE-2.0
// Modifications made by sekulas, 2024:
// - made code cleaner.

use std::{
    cmp::max,
    path::{Path, PathBuf},
};

use super::{Error, Result};

use serde::{Deserialize, Serialize};

use crate::{
    entry_point::{EntryPoint, EntryPointContainer},
    fixed_length_priority_queue::FixedLengthPriorityQueue,
    graph_links::{GraphLinks, GraphLinksImpl},
    io_ops::{read_bin, save_bin},
    scorer::Scorer,
    search_context::SearchContext,
    types::{PointOffsetType, ScoredPointOffset, HNSW_GRAPH_FILE, HNSW_LINKS_FILE},
    visited_pool::{VisitedListHandle, VisitedPool},
};

pub type LinkContainer = Vec<PointOffsetType>;

#[derive(Deserialize, Serialize, Debug)]
pub struct GraphLayers {
    pub(super) m: usize,
    pub(super) m0: usize,
    pub(super) ef_construct: usize,

    #[serde(skip)]
    pub(super) links: GraphLinksImpl,
    pub(super) entry_point: EntryPointContainer,

    #[serde(skip)]
    pub(super) visited_pool: VisitedPool,
}

pub trait GraphLayersBase {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle;

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, f: F)
    where
        F: FnMut(PointOffsetType);

    fn get_layer_max_links(&self, level: usize) -> usize;

    fn explore_layer(
        &self,
        searcher: &mut SearchContext,
        level: usize,
        visited_list: &mut VisitedListHandle,
        points_scorer: &mut Scorer,
    ) {
        let limit = self.get_layer_max_links(level);
        let mut points_ids: Vec<PointOffsetType> = Vec::with_capacity(2 * limit);

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

            let scores = points_scorer.score_points(&points_ids, limit);
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
        ef_search: usize,
        points_scorer: &mut Scorer,
    ) -> FixedLengthPriorityQueue<ScoredPointOffset> {
        let mut visited_list = self.get_visited_list_from_pool();
        visited_list.check_and_update_visited(level_entry.idx);
        let mut search_context = SearchContext::new(level_entry, ef_search);

        self.explore_layer(&mut search_context, level, &mut visited_list, points_scorer);
        search_context.nearest
    }

    fn search_entry(
        &self,
        entry_point: PointOffsetType,
        top_level: usize,
        target_level: usize,
        points_scorer: &mut Scorer,
    ) -> ScoredPointOffset {
        let mut links: Vec<PointOffsetType> = Vec::with_capacity(2 * self.get_layer_max_links(0));
        let mut current_point = self.initialize_entry_point(entry_point, points_scorer);

        for level in reverse_range(top_level, target_level) {
            current_point =
                self.refine_entry_point(&mut links, current_point, level, points_scorer);
        }

        current_point
    }

    fn initialize_entry_point(
        &self,
        entry_point: PointOffsetType,
        points_scorer: &Scorer,
    ) -> ScoredPointOffset {
        ScoredPointOffset {
            idx: entry_point,
            score: points_scorer.score_point(entry_point),
        }
    }

    fn refine_entry_point(
        &self,
        links: &mut Vec<PointOffsetType>,
        mut current_point: ScoredPointOffset,
        level: usize,
        points_scorer: &mut Scorer,
    ) -> ScoredPointOffset {
        let limit = self.get_layer_max_links(level);
        let mut best_point = current_point;
        let mut changed = true;

        while changed {
            changed = false;
            links.clear();

            self.links_map(current_point.idx, level, |link| {
                links.push(link);
            });

            let scores = points_scorer.score_points(links, limit);
            for score_point in scores.iter().copied() {
                if score_point.score > best_point.score {
                    changed = true;
                    best_point = score_point;
                }
            }

            current_point = best_point;
        }

        best_point
    }
}

impl GraphLayersBase for GraphLayers {
    fn get_visited_list_from_pool(&self) -> VisitedListHandle {
        self.visited_pool.get(self.links.num_points())
    }

    fn links_map<F>(&self, point_id: PointOffsetType, level: usize, mut f: F)
    where
        F: FnMut(PointOffsetType),
    {
        for link in self.links.links(point_id, level) {
            f(*link);
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

impl GraphLayers {
    fn get_entry_point(&self) -> Option<EntryPoint> {
        self.entry_point.get_entry_point()
    }

    pub fn search(
        &self,
        top: usize,
        ef_search: usize,
        mut points_scorer: Scorer,
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

        let nearest =
            self.search_on_level(zero_level_entry, 0, max(top, ef_search), &mut points_scorer);

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

fn reverse_range(a: usize, b: usize) -> impl Iterator<Item = usize> {
    (b + 1..=a).rev()
}
