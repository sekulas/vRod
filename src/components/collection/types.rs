use super::storage::strg::Record;

pub const NOT_SET: u16 = 0;
pub const NONE: u64 = 0;

pub enum CollectionSearchResult {
    Found(Record),
    NotFound,
}
