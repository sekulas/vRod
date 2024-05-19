use std::{fs::File, path::PathBuf};

pub struct Storage {
    path: PathBuf,
    file: File,
}
