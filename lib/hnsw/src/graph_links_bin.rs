use std::fs::File;
use std::io::{Read, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use super::Result;
use bincode::{deserialize, serialize};
use serde::{Deserialize, Serialize};

use crate::types::PointIdType;

#[derive(Debug, Default, Serialize, Deserialize)]
struct GraphLinksFileData {
    point_count: u64,
    levels_count: u64,
    links: Vec<PointIdType>,
    offsets: Vec<u64>,
    level_offsets: Vec<u64>,
    reindex: Vec<PointIdType>,
}

pub struct GraphLinksConverter {
    edges: Vec<Vec<Vec<PointIdType>>>,
    reindex: Vec<PointIdType>,
    back_index: Vec<usize>,
    total_links_len: usize,
    total_offsets_len: usize,
    path: Option<PathBuf>,
}

impl GraphLinksConverter {
    pub fn new(edges: Vec<Vec<Vec<PointIdType>>>) -> Self {
        if edges.is_empty() {
            return Self {
                edges,
                reindex: Vec::new(),
                back_index: Vec::new(),
                total_links_len: 0,
                total_offsets_len: 1,
                path: None,
            };
        }

        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        back_index.sort_unstable_by_key(|&i| edges[i].len());
        back_index.reverse();

        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i]] = i as PointIdType;
        }

        let mut total_links_len = 0;
        let mut total_offsets_len = 1;
        for point in edges.iter() {
            for layer in point.iter() {
                total_links_len += layer.len();
                total_offsets_len += 1;
            }
        }

        Self {
            edges,
            reindex,
            back_index,
            total_links_len,
            total_offsets_len,
            path: None,
        }
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
            point_count: self.reindex.len() as u64,
            levels_count: self.get_levels_count() as u64,
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
        F: FnMut(usize, &Vec<PointIdType>),
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

pub trait GraphLinks: Default {
    fn load_from_file(path: &Path) -> Result<Self>;

    fn from_converter(converter: GraphLinksConverter) -> Result<Self>;

    fn get_links(&self, range: Range<usize>) -> &[PointIdType];

    fn get_links_range(&self, idx: usize) -> Range<usize>;

    fn get_level_offset(&self, level: usize) -> usize;

    fn reindex(&self, point_id: PointIdType) -> PointIdType;

    fn num_points(&self) -> usize;

    fn links(&self, point_id: PointIdType, level: usize) -> &[PointIdType] {
        if level == 0 {
            let links_range = self.get_links_range(point_id as usize);
            self.get_links(links_range)
        } else {
            let reindexed_point_id = self.reindex(point_id) as usize;
            let layer_offsets_start = self.get_level_offset(level);
            let links_range = self.get_links_range(layer_offsets_start + reindexed_point_id);
            self.get_links(links_range)
        }
    }
}

#[derive(Debug, Default)]
pub struct GraphLinksImpl {
    links: Vec<PointIdType>,
    offsets: Vec<u64>,
    level_offsets: Vec<u64>,
    reindex: Vec<PointIdType>,
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

    fn get_links(&self, range: Range<usize>) -> &[PointIdType] {
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

    fn reindex(&self, point_id: PointIdType) -> PointIdType {
        self.reindex[point_id as usize]
    }

    fn num_points(&self) -> usize {
        self.reindex.len()
    }
}
