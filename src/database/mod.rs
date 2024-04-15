use std::path::PathBuf;

pub struct Database {
    pub path: PathBuf,
    //TODO collections: todo!("Implement collections"),
    //TODO wal: Wal
}

impl Database {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }
}
