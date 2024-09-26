use std::mem;

use super::Result;
use crate::types::{Lsn, Offset, RecordId};

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

pub trait Index {
    fn perform_command(&mut self, command: IndexCommand, lsn: Lsn) -> Result<IndexCommandResult>;
    fn perform_query(&mut self, query: IndexQuery) -> Result<IndexQueryResult>;
    fn perform_rollback(&mut self, lsn: Lsn) -> Result<()>;
}

pub struct BPTreeCreationSettings {
    pub name: String,
    pub branching_factor: NodeIdx,
    pub modification_lsn: Lsn,
    pub current_max_id: RecordId,
}

pub enum IndexCommand {
    BulkInsert(Vec<Offset>),
    Insert(Offset),
    Update(RecordId, Offset),
}

#[cfg_attr(test, derive(PartialEq, Debug))]
pub enum IndexCommandResult {
    BulkInserted,
    Inserted,
    Updated,
    NotFound,
}

pub enum IndexQuery {
    SearchAll,
    Search(RecordId),
}

#[derive(PartialEq, Debug)]
pub enum IndexQueryResult {
    FoundKeysAndValues(Vec<(RecordId, Offset)>),
    FoundValue(Offset),
    NotFound,
}

pub enum IndexUpdateResult {
    Updated,
    NotFound,
}

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
    Updated {
        existing_child_new_offset: Offset,
        next_leaf_to_connect_offset_from_child: Offset,
    },
    KeyNotFound,
}

pub enum FindKeyResult {
    Found { idx: usize },
    NotFound { idx: usize },
}
