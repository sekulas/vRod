use crate::types::Offset;

pub enum StorageDeleteResult {
    Deleted,
    NotFound,
}
pub enum StorageUpdateResult {
    Updated { new_offset: Offset },
    NotFound,
}
