mod error;
pub use error::{Error, Result};

mod create_collection;
pub(super) use create_collection::*;
mod drop_collection;
pub(super) use drop_collection::*;

mod truncate_wal;
pub(super) use truncate_wal::*;

mod insert;
pub(super) use insert::*;
mod bulk_insert;
pub(super) use bulk_insert::*;
mod update;
pub(super) use update::*;
mod delete;
pub(super) use delete::*;
mod reindex;
pub(super) use reindex::*;