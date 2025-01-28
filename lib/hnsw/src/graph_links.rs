// Inspired by Project qdrant (https://github.com/qdrant/qdrant/tree/master)
// Licensed under the Apache License, Version 2.0 (the "License");
// See: http://www.apache.org/licenses/LICENSE-2.0
// Modifications made by sekulas, 2024:
// - replaced memory-mapped files (mmap) with standard file operations.

use std::fs::File;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::Result;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

use crate::types::PointOffsetType;

#[derive(Debug, Default, Serialize, Deserialize)]
struct GraphLinksFileData {
    links: Vec<PointOffsetType>,
    offsets: Vec<u64>,
    level_offsets: Vec<u64>,
    reindex: Vec<PointOffsetType>,
}

pub struct GraphLinksConverter {
    edges: Vec<Vec<Vec<PointOffsetType>>>,
    reindex: Vec<PointOffsetType>,
    back_index: Vec<usize>,
    total_links_len: usize,
    total_offsets_len: usize,
    path: Option<PathBuf>,
}

impl GraphLinksConverter {
    pub fn new(edges: Vec<Vec<Vec<PointOffsetType>>>) -> Self {
        if edges.is_empty() {
            return Self::default();
        }

        let (reindex, back_index) = Self::compute_reindexing(&edges);
        let (total_links_len, total_offsets_len) = Self::compute_lengths(&edges);

        Self {
            edges,
            reindex,
            back_index,
            total_links_len,
            total_offsets_len,
            path: None,
        }
    }

    fn compute_reindexing(
        edges: &[Vec<Vec<PointOffsetType>>],
    ) -> (Vec<PointOffsetType>, Vec<usize>) {
        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        back_index.sort_unstable_by_key(|&i| edges[i].len());
        back_index.reverse();

        let mut reindex = vec![0; back_index.len()];
        for (point_idx, &back_idx) in back_index.iter().enumerate() {
            reindex[back_idx] = point_idx as PointOffsetType;
        }

        (reindex, back_index)
    }

    fn compute_lengths(edges: &[Vec<Vec<PointOffsetType>>]) -> (usize, usize) {
        let mut total_links_len = 0;
        let mut total_offsets_len = 1;

        for point in edges.iter() {
            for layer in point.iter() {
                total_links_len += layer.len();
                total_offsets_len += 1;
            }
        }

        (total_links_len, total_offsets_len)
    }

    fn get_file_data(&self) -> GraphLinksFileData {
        let mut links = Vec::with_capacity(self.total_links_len);
        let mut offsets = Vec::with_capacity(self.total_offsets_len);
        let mut level_offsets = Vec::with_capacity(self.get_levels_count());

        offsets.push(0);
        for level in 0..self.get_levels_count() {
            level_offsets.push(offsets.len() as u64 - 1);
            self.iterate_level_points(level, |_, point_links| {
                links.extend_from_slice(point_links);
                offsets.push(links.len() as u64);
            });
        }

        GraphLinksFileData {
            links,
            offsets,
            level_offsets,
            reindex: self.reindex.clone(),
        }
    }

    pub fn save_as(&mut self, path: &Path) -> Result<()> {
        self.path = Some(path.to_path_buf());
        let temp_path = path.with_extension("tmp");

        let file_data = self.get_file_data();
        let serialized_data = serialize(&file_data)?;

        let mut file = File::create(&temp_path)?;
        file.write_all(&serialized_data)?;
        file.flush()?;

        std::fs::rename(temp_path, path)?;

        Ok(())
    }

    pub fn get_levels_count(&self) -> usize {
        if self.back_index.is_empty() {
            return 0;
        }
        self.edges[self.back_index[0]].len()
    }

    pub fn iterate_level_points<F>(&self, level: usize, mut f: F)
    where
        F: FnMut(usize, &Vec<PointOffsetType>),
    {
        let edges_len = self.edges.len();
        if level == 0 {
            (0..edges_len).for_each(|point_id| f(point_id, &self.edges[point_id][0]));
        } else {
            for i in 0..edges_len {
                let point_id = self.back_index[i];
                if level >= self.edges[point_id].len() {
                    break;
                }
                f(point_id, &self.edges[point_id][level]);
            }
        }
    }
}

impl Default for GraphLinksConverter {
    fn default() -> Self {
        Self {
            edges: Vec::new(),
            reindex: Vec::new(),
            back_index: Vec::new(),
            total_links_len: 0,
            total_offsets_len: 1,
            path: None,
        }
    }
}

pub trait GraphLinks: Default {
    fn load_from_file(path: &Path) -> Result<Self>;

    fn from_converter(converter: GraphLinksConverter) -> Result<Self>;

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType];

    fn get_links_range(&self, idx: usize) -> Range<usize>;

    fn get_level_offset(&self, level: usize) -> usize;

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType;

    fn num_points(&self) -> usize;

    fn links(&self, point_id: PointOffsetType, level: usize) -> &[PointOffsetType] {
        match level {
            0 => {
                let links_range = self.get_links_range(point_id as usize);
                self.get_links(links_range)
            }
            _ => {
                let reindexed_point_id = self.reindex(point_id) as usize;
                let layer_offsets_start = self.get_level_offset(level);
                let links_range = self.get_links_range(layer_offsets_start + reindexed_point_id);
                self.get_links(links_range)
            }
        }
    }
}

#[derive(Debug, Default)]
pub struct GraphLinksImpl {
    links: Vec<PointOffsetType>,
    offsets: Vec<u64>, // offsets[point_id] = start_offset, offsets[point_id + 1] = end_offset. Stored from lowest to highest layer.
    level_offsets: Vec<u64>,
    reindex: Vec<PointOffsetType>, // reindex[point_id] = new_point_id (used for access to links in offsets especially for layers > 0)
}

impl GraphLinksImpl {
    pub fn load_from_memory(data: &[u8]) -> Result<Self> {
        let file_data: GraphLinksFileData = deserialize(data)?;

        Ok(Self {
            links: file_data.links,
            offsets: file_data.offsets,
            level_offsets: file_data.level_offsets,
            reindex: file_data.reindex,
        })
    }
}

impl GraphLinks for GraphLinksImpl {
    fn load_from_file(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        Self::load_from_memory(&buffer)
    }

    fn from_converter(converter: GraphLinksConverter) -> Result<Self> {
        let file_data = converter.get_file_data();
        Ok(Self {
            links: file_data.links,
            offsets: file_data.offsets,
            level_offsets: file_data.level_offsets,
            reindex: file_data.reindex,
        })
    }

    fn get_links(&self, range: Range<usize>) -> &[PointOffsetType] {
        &self.links[range]
    }

    fn get_links_range(&self, idx: usize) -> Range<usize> {
        let start = self.offsets[idx];
        let end = self.offsets[idx + 1];
        start as usize..end as usize
    }

    fn get_level_offset(&self, level: usize) -> usize {
        self.level_offsets[level] as usize
    }

    fn reindex(&self, point_id: PointOffsetType) -> PointOffsetType {
        self.reindex[point_id as usize]
    }

    fn num_points(&self) -> usize {
        self.reindex.len()
    }
}

#[cfg(test)]
mod tests {
    use super::{GraphLinks, GraphLinksConverter, GraphLinksImpl};

    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn graph_links_load_from_file_should_not_change_graph_structure() -> Result<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let file_name = "graph_links.bin";

        // lvl2: 1
        // lvl1: 1, 3, 4
        // lvl0: 0, 1, 2, 3, 4
        let edges = vec![
            vec![vec![1, 2, 3, 4]],
            vec![vec![0, 2, 3, 4], vec![4], vec![]],
            vec![vec![0, 1, 3, 4]],
            vec![vec![0, 1, 2, 4], vec![4]],
            vec![vec![0, 1, 2, 3], vec![1, 3]],
        ];

        let mut converter = GraphLinksConverter::new(edges);
        converter.save_as(&path.join(file_name))?;

        // Act
        let graph_links = GraphLinksImpl::load_from_file(&path.join(file_name))?;

        // Assert
        assert_eq!(graph_links.num_points(), 5);
        assert_eq!(graph_links.links(0, 0), [1, 2, 3, 4]);
        assert_eq!(graph_links.links(1, 0), [0, 2, 3, 4]);
        assert_eq!(graph_links.links(2, 0), [0, 1, 3, 4]);
        assert_eq!(graph_links.links(3, 0), [0, 1, 2, 4]);
        assert_eq!(graph_links.links(4, 0), [0, 1, 2, 3]);
        assert_eq!(graph_links.links(1, 1), [4]);
        assert_eq!(graph_links.links(3, 1), [4]);
        assert_eq!(graph_links.links(4, 1), [1, 3]);
        assert_eq!(graph_links.links(1, 2), &[] as &[u32]);

        Ok(())
    }
}
