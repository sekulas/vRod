use std::mem;

use crate::types::{Offset, RecordId};

//TODO: Change DEFAULT_BRANCHING_FACTOR;
pub type NodeIdx = u16;
pub const DEFAULT_BRANCHING_FACTOR: NodeIdx = 3;
pub const FIRST_VALUE_SLOT: NodeIdx = DEFAULT_BRANCHING_FACTOR - 1;
pub const MAX_KEYS: NodeIdx = DEFAULT_BRANCHING_FACTOR - 1;
pub const HIGHEST_KEY_SLOT: usize = 0;
pub const EMPTY_KEY_SLOT: u32 = 0;
pub const EMPTY_CHILD_SLOT: u64 = 0;
pub const SERIALIZED_NODE_SIZE: usize = 8
    + 1
    + (MAX_KEYS * mem::size_of::<RecordId>() as u16 + 8) as usize
    + (DEFAULT_BRANCHING_FACTOR * mem::size_of::<Offset>() as u16 + 8) as usize
    + 8
    + 2;

pub enum InsertionResult {
    Inserted {
        existing_child_new_offset: Offset,
    },
    InsertedAndPromoted {
        promoted_key: RecordId,
        existing_child_new_offset: Offset,
        new_child_offset: Offset,
    },
}

pub enum UpdateResult {
    Updated { existing_child_new_offset: Offset },
    KeyNotFound,
}

pub enum FindKeyResult {
    Found { idx: usize },
    NotFound { idx: usize },
}
