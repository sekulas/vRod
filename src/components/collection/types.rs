use super::storage::strg::Record;

pub const NOT_SET: u16 = 0;
pub const NONE: u64 = 0;

#[cfg_attr(test, derive(PartialEq, Debug))]
pub enum CollectionSearchResult {
    FoundRecord(Record),
    NotFound,
}

pub enum CollectionUpdateResult {
    Updated,
    NotFound,
}

pub enum CollectionDeleteResult {
    Deleted,
    NotFound,
}
