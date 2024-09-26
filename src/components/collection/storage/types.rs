use crate::types::{Dim, Lsn, Offset};

use super::strg::Record;
use super::Result;

pub trait StorageInterface {
    fn perform_command(
        &mut self,
        command: StorageCommand,
        lsn: Lsn,
    ) -> Result<StorageCommandResult>;
    fn perform_query(&mut self, query: StorageQuery) -> Result<StorageQueryResult>;
    fn perform_rollback(&mut self, lsn: Lsn) -> Result<()>;
}
pub struct StorageCreationSettings {
    pub name: String,
    pub modification_lsn: Lsn,
    pub vector_dim_amount: u16,
}
pub enum StorageCommand<'a> {
    BulkInsert {
        vectors_and_payloads: &'a [(&'a [Dim], &'a str)],
    },
    Insert {
        vector: &'a [Dim],
        payload: &'a str,
    },
    Update {
        offset: Offset,
        vector: Option<&'a [Dim]>,
        payload: Option<&'a str>,
    },
    Delete {
        offset: Offset,
    },
}

pub enum StorageQuery {
    Search { offset: Offset },
}

pub enum StorageCommandResult {
    BulkInserted { offsets: Vec<Offset> },
    Inserted { offset: Offset },
    Updated { new_offset: Offset },
    Deleted,
    NotFound,
}

#[cfg_attr(test, derive(PartialEq, Debug))]
pub enum StorageQueryResult {
    FoundRecord { record: Record },
    NotFound,
}

pub enum StorageDeleteResult {
    Deleted,
    NotFound,
}
pub enum StorageUpdateResult {
    Updated { new_offset: Offset },
    NotFound,
}
