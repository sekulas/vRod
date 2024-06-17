use std::{fs::File, path::PathBuf};

pub struct IdOffsetStorage {
    path: PathBuf,
    file: File,
    header: IdOffsetStorageHeader,
}

pub struct IdOffsetStorageHeader {
    pub id_offset: u64,
}
