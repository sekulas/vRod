use std::mem;

use crate::types::{Offset, RecordId};

pub const M: usize = 4;
pub const MAX_KEYS: usize = M - 1;
pub const EMPTY_KEY_SLOT: u32 = 0;
pub const EMPTY_CHILD_SLOT: u64 = 0;
pub const SERIALIZED_NODE_SIZE: usize =
    8 + 1 + (M * mem::size_of::<RecordId>() + 8) + (M * mem::size_of::<Offset>() + 8) + 8 + 8 + 2;
