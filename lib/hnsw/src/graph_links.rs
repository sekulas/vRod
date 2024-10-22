use std::{
    cmp::max,
    fs::OpenOptions,
    mem::size_of,
    ops::Range,
    path::{Path, PathBuf},
};

use super::Result;

use crate::types::PointIdType;

#[derive(Debug, Default)]
struct GraphLinksFileHeader {
    pub point_count: u64,
    pub levels_count: u64,
    pub total_links_len: u64,
    pub total_offsets_len: u64,
    pub offsets_padding: u64,
}

impl GraphLinksFileHeader {
    pub fn new(
        point_count: usize,
        levels_count: usize,
        total_links_len: usize,
        total_offsets_len: usize,
    ) -> GraphLinksFileHeader {
        let offsets_padding = if (point_count + total_links_len) % 2 == 0 {
            0
        } else {
            4
        };
        GraphLinksFileHeader {
            point_count: point_count as u64,
            levels_count: levels_count as u64,
            total_links_len: total_links_len as u64,
            total_offsets_len: total_offsets_len as u64,
            offsets_padding,
        }
    }

    pub fn raw_size() -> usize {
        size_of::<u64>() * 5
    }

    pub fn serialize_bytes_to(&self, raw_data: &mut [u8]) {
        let byte_slice = &mut raw_data[0..Self::raw_size()];
        let arr: &mut [u64] = mmap_ops::transmute_from_u8_to_mut_slice(byte_slice);
        arr[0] = self.point_count;
        arr[1] = self.levels_count;
        arr[2] = self.total_links_len;
        arr[3] = self.total_offsets_len;
        arr[4] = self.offsets_padding;
    }

    pub fn deserialize_bytes_from(raw_data: &[u8]) -> GraphLinksFileHeader {
        let byte_slice = &raw_data[0..Self::raw_size()];
        let arr: &[u64] = mmap_ops::transmute_from_u8_to_slice(byte_slice);
        GraphLinksFileHeader {
            point_count: arr[0],
            levels_count: arr[1],
            total_links_len: arr[2],
            total_offsets_len: arr[3],
            offsets_padding: arr[4],
        }
    }

    pub fn get_data_size(&self) -> u64 {
        self.get_offsets_range().end as u64
    }

    pub fn get_level_offsets_range(&self) -> Range<usize> {
        // level offsets are stored after header
        // but we might want to have some extra space for future changes
        let start = max(64, Self::raw_size());
        start..start + self.levels_count as usize * size_of::<u64>()
    }

    pub fn get_reindex_range(&self) -> Range<usize> {
        let start = self.get_level_offsets_range().end;
        start..start + self.point_count as usize * size_of::<PointIdType>()
    }

    pub fn get_links_range(&self) -> Range<usize> {
        let start = self.get_reindex_range().end;
        start..start + self.total_links_len as usize * size_of::<PointIdType>()
    }

    pub fn get_offsets_range(&self) -> Range<usize> {
        let start = self.get_links_range().end + self.offsets_padding as usize;
        start..start + self.total_offsets_len as usize * size_of::<u64>()
    }
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

        // create map from index in `offsets` to point_id
        let mut back_index: Vec<usize> = (0..edges.len()).collect();
        // sort by max layer and use this map to build `Self.reindex`
        back_index.sort_unstable_by_key(|&i| edges[i].len());
        back_index.reverse();

        // `reindex` is map from point id to index in `Self.offsets`
        let mut reindex = vec![0; back_index.len()];
        for i in 0..back_index.len() {
            reindex[back_index[i]] = i as PointIdType;
        }

        // estimate size of `links` and `offsets`
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

    fn get_header(&self) -> GraphLinksFileHeader {
        GraphLinksFileHeader::new(
            self.reindex.len(),
            self.get_levels_count(),
            self.total_links_len,
            self.total_offsets_len,
        )
    }

    pub fn data_size(&self) -> u64 {
        self.get_header().get_data_size()
    }

    pub fn serialize_to(&self, bytes_data: &mut [u8]) {
        let header = self.get_header();

        header.serialize_bytes_to(bytes_data);

        {
            let reindex_range = header.get_reindex_range();
            let reindex_byte_slice = &mut bytes_data[reindex_range];
            let reindex_slice: &mut [PointIdType] =
                mmap_ops::transmute_from_u8_to_mut_slice(reindex_byte_slice);
            reindex_slice.copy_from_slice(&self.reindex);
        }

        let header_levels_count = header.levels_count as usize;
        let mut level_offsets = Vec::with_capacity(header_levels_count);
        {
            let links_range = header.get_links_range();
            let offsets_range = header.get_offsets_range();
            let union_range = links_range.start..offsets_range.end;
            let (links_mmap, offsets_with_padding_mmap) = bytes_data[union_range]
                .as_mut()
                .split_at_mut(links_range.len());
            let offsets_mmap = &mut offsets_with_padding_mmap[header.offsets_padding as _..];
            let links_mmap: &mut [PointIdType] =
                mmap_ops::transmute_from_u8_to_mut_slice(links_mmap);
            let offsets_mmap: &mut [u64] = mmap_ops::transmute_from_u8_to_mut_slice(offsets_mmap);
            offsets_mmap[0] = 0;

            let mut links_pos = 0;
            let mut offsets_pos = 1;
            for level in 0..header_levels_count {
                level_offsets.push(offsets_pos as u64 - 1);
                self.iterate_level_points(level, |_, links| {
                    links_mmap[links_pos..links_pos + links.len()].copy_from_slice(links);
                    links_pos += links.len();

                    offsets_mmap[offsets_pos] = links_pos as u64;
                    offsets_pos += 1;
                });
            }
        }

        {
            let level_offsets_range = header.get_level_offsets_range();
            let level_offsets_byte_slice = &mut bytes_data[level_offsets_range];
            let level_offsets_slice: &mut [u64] =
                mmap_ops::transmute_from_u8_to_mut_slice(level_offsets_byte_slice);
            level_offsets_slice.copy_from_slice(&level_offsets);
        }
    }

    pub fn save_as(&mut self, path: &Path) -> Result<()> {
        self.path = Some(path.to_path_buf());
        let temp_path = path.with_extension("tmp");
        {
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(temp_path.as_path())?;
            file.set_len(self.data_size())?;

            let m = unsafe { MmapMut::map_mut(&file) };
            let mut mmap = m?;

            self.serialize_to(&mut mmap);

            mmap.flush()?;
        }
        std::fs::rename(temp_path, path)?;

        Ok(())
    }

    pub fn get_levels_count(&self) -> usize {
        if self.back_index.is_empty() {
            return 0;
        }
        // because back_index is sorted by point`s max layer, we can retrieve max level from `point_id = back_index[0]`
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

    fn offsets_len(&self) -> usize;

    fn levels_count(&self) -> usize;

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

    fn point_level(&self, point_id: PointIdType) -> usize {
        let reindexed_point_id = self.reindex(point_id) as usize;
        // level 0 is always present, start checking from level 1. Stop checking when level is incorrect
        for level in 1.. {
            if let Some(offsets_range) = self.get_level_offsets_range(level) {
                if offsets_range.start + reindexed_point_id >= offsets_range.end {
                    // incorrect level because point_id is out of range
                    return level - 1;
                }
            } else {
                // incorrect level because this level is larger that available levels
                return level - 1;
            }
        }
        unreachable!()
    }

    fn get_level_offsets_range(&self, level: usize) -> Option<Range<usize>> {
        if level < self.levels_count() {
            let layer_offsets_start = self.get_level_offset(level);
            let layer_offsets_end = if level + 1 < self.levels_count() {
                // `level` is not last, next level_offsets is end of range
                self.get_level_offset(level + 1)
            } else {
                // `level` is last, next `offsets.len()` is end of range
                self.offsets_len() - 1
            };
            Some(layer_offsets_start..layer_offsets_end)
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
pub struct GraphLinksImpl {
    // all flattened links of all levels
    links: Vec<PointIdType>,
    // all ranges in `links`. each range is `links[offsets[i]..offsets[i+1]]`
    // ranges are sorted by level
    offsets: Vec<u64>,
    // start offset of each level in `offsets`
    level_offsets: Vec<u64>,
    // for level 1 and above: reindex[point_id] = index of point_id in offsets
    reindex: Vec<PointIdType>,
}

impl GraphLinksImpl {
    pub fn load_from_memory(data: &[u8]) -> Result<Self> {
        let header = GraphLinksFileHeader::deserialize_bytes_from(data);

        let mut links: Vec<PointIdType> = Vec::new();
        let mut offsets: Vec<u64> = Vec::new();
        let mut level_offsets: Vec<u64> = Vec::new();
        let mut reindex: Vec<PointIdType> = Vec::new();

        let link_slice = get_links_slice(data, &header);
        links.try_set_capacity_exact(link_slice.len())?;
        links.extend_from_slice(link_slice);

        offsets.try_set_capacity_exact(header.get_offsets_range().len() / size_of::<u64>())?;
        offsets.extend(get_offsets_iter(data, &header));

        let level_offsets_slice = get_level_offsets(data, &header);
        level_offsets.try_set_capacity_exact(level_offsets_slice.len())?;
        level_offsets.extend_from_slice(level_offsets_slice);

        let reindex_slice = get_reindex_slice(data, &header);
        reindex.try_set_capacity_exact(reindex_slice.len())?;
        reindex.extend_from_slice(reindex_slice);

        let graph_links = Self {
            links,
            offsets,
            level_offsets,
            reindex,
        };

        Ok(graph_links)
    }
}

impl GraphLinks for GraphLinksImpl {
    fn load_from_file(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(path)?;

        let mmap = unsafe { Mmap::map(&file)? };

        Self::load_from_memory(&mmap)
    }

    fn from_converter(converter: GraphLinksConverter) -> Result<Self> {
        let mut data = vec![0; converter.data_size() as usize];
        converter.serialize_to(&mut data);
        drop(converter);

        Self::load_from_memory(&data)
    }

    fn offsets_len(&self) -> usize {
        self.offsets.len()
    }

    fn levels_count(&self) -> usize {
        self.level_offsets.len()
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
