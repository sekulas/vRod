use std::{fs::File, path::PathBuf};

pub struct IdOffsetStorage {
    path: PathBuf,
    file: File,
}
