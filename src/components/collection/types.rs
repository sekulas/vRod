use super::storage::strg::Record;

pub const NOT_SET: u16 = 0;
pub const NONE: u64 = 0;

pub enum CollectionInsertResult {
    Inserted,
    NotInserted { description: String },
}

#[cfg_attr(test, derive(PartialEq, Debug))]
pub enum CollectionSearchResult {
    FoundRecord(Record),
    NotFound,
}

pub enum CollectionUpdateResult {
    Updated,
    NotFound,
    NotUpdated { description: String },
}

pub enum CollectionDeleteResult {
    Deleted,
    NotFound,
}
