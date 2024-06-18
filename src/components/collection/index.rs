use std::{fs::File, path::PathBuf};

pub struct Index {
    path: PathBuf,
    file: File,
}
