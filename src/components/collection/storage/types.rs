use crate::types::{Dim, Offset, LSN};

use super::strg::Record;
use super::Result;

pub trait StorageInterface {
    fn perform_command(
        &mut self,
        command: StorageCommand,
        lsn: LSN,
    ) -> Result<StorageCommandResult>;
    fn perform_query(&mut self, query: StorageQuery) -> Result<StorageQueryResult>;
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
