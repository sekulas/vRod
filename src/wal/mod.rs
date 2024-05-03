mod error;

use bincode::{deserialize_from, serialize_into};
pub use error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::mem;
use std::path::Path;

pub struct WAL {
    file: File,
    header: WALHeader,
}

#[derive(Serialize, Deserialize)]
struct WALEntry {
    lsn: u64,
    committed: bool,
    data_len: u16,
    data: String,
}

#[derive(Serialize, Deserialize)]
struct WALHeader {
    last_entry_offset: u64,
    lsn: u64,
}

impl WALHeader {
    fn new(last_entry_offset: u64, lsn: u64) -> Self {
        Self {
            last_entry_offset,
            lsn,
        }
    }
}

impl Default for WALHeader {
    fn default() -> Self {
        Self {
            last_entry_offset: mem::size_of::<WALHeader>() as u64,
            lsn: 0,
        }
    }
}

impl WAL {
    pub fn create(path: &Path) -> Result<Self> {
        fs::File::create(path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header = WALHeader::default();
        let mut wal = Self { file, header };
        wal.flush_header()?;

        Ok(wal)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header: WALHeader = match deserialize_from(&mut BufReader::new(&file)) {
            Ok(header) => header,
            Err(_) => {
                let wal = WAL::recreate_wal(path)?;
                return Ok(wal);
            }
        };

        //TODO consistency check

        Ok(Self { file, header })
    }

    fn append(&mut self, data: String) -> Result<()> {
        self.header.lsn += 1;
        let entry = WALEntry {
            lsn: self.header.lsn,
            committed: false,
            data_len: data.len() as u16,
            data,
        };

        self.file.seek(SeekFrom::End(0))?;
        self.header.last_entry_offset = self.file.stream_position()?;

        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        self.flush_header()?;
        Ok(())
    }

    fn commit(&mut self, lsn: u64) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let mut entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        entry.committed = true;

        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;
        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        Ok(())
    }

    fn get_last_entry(&mut self) -> Result<Option<WALEntry>> {
        let file_size = self.file.metadata()?.len();

        if file_size <= mem::size_of::<WALHeader>() as u64 {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        Ok(Some(entry))
    }

    fn flush_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;
        Ok(())
    }

    fn recreate_wal(path: &Path) -> Result<Self> {
        fs::remove_file(path)?;

        let wal = WAL::create(path)?;
        //TODO when other files will exist, we have to check header lsn and create wal with highest lsn

        Ok(wal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    struct TestFileGuard {
        path: PathBuf,
    }

    impl TestFileGuard {
        fn new(path: &Path) -> std::io::Result<Self> {
            let path: PathBuf = PathBuf::from(path);

            if path.exists() {
                fs::remove_file(&path)?;
            }
            fs::File::create(&path)?;

            Ok(Self { path })
        }
    }

    impl Drop for TestFileGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn append_and_commit_should_commit_last_entry() -> Result<()> {
        let wal_file: &Path = Path::new("test1.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let mut wal = WAL::load(wal_file)?;

        let data = "Hello, World!".to_string();
        wal.append(data.clone())?;

        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert_eq!(entry.data, data);

        assert!(!entry.committed);

        wal.commit(entry.lsn)?;

        let entry = wal.get_last_entry().unwrap().unwrap();

        assert!(entry.committed);

        Ok(())
    }

    #[test]
    fn flush_header_changes_lsn_value() -> Result<()> {
        let wal_file: &Path = Path::new("test2.wal");

        let _guard = TestFileGuard::new(wal_file)?;

        let mut wal = WAL::load(wal_file)?;
        wal.header.lsn = 10;
        wal.flush_header()?;

        let wal = WAL::load(wal_file)?;

        assert_eq!(wal.header.lsn, 10);

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_none_if_no_entries() -> Result<()> {
        let wal_file: &Path = Path::new("test3.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let mut wal = WAL::load(wal_file)?;

        let entry = wal.get_last_entry()?;

        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_last_entry_after_header_update() -> Result<()> {
        let wal_file: &Path = Path::new("test4.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let mut wal = WAL::load(wal_file)?;

        let data1 = "Hello, World!".to_string();
        wal.append(data1.clone())?;

        let data2 = "2World, Hello!2".to_string();
        wal.append(data2.clone())?;

        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert_eq!(entry.data, data2);
        assert_eq!(entry.lsn, 2);

        Ok(())
    }
}
