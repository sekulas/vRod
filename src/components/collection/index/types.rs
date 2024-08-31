use std::mem;

use crate::types::{Offset, RecordId};

pub const M: usize = 3;
pub const FIRST_VALUE_SLOT: usize = M - 1;
pub const MAX_KEYS: usize = M - 1;
pub const HIGHEST_KEY_SLOT: usize = 0;
pub const EMPTY_KEY_SLOT: u32 = 0;
pub const EMPTY_CHILD_SLOT: u64 = 0;
pub const SERIALIZED_NODE_SIZE: usize = 8
    + 1
    + (MAX_KEYS * mem::size_of::<RecordId>() + 8)
    + (M * mem::size_of::<Offset>() + 8)
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
