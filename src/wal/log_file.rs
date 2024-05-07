use super::Result;
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::mem;
use std::path::{Path, PathBuf};

pub enum WalType {
    Consistent(Wal),
    Uncommited(Wal, String),
}

pub struct Wal {
    path: PathBuf,
    file: File,
    header: WalHeader,
}

#[derive(Serialize, Deserialize)]
pub struct WalHeader {
    last_entry_offset: u64,
    lsn: u64,
}

impl WalHeader {
    fn new(last_entry_offset: u64, lsn: u64) -> Self {
        Self {
            last_entry_offset,
            lsn,
        }
    }
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            last_entry_offset: mem::size_of::<WalHeader>() as u64,
            lsn: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct WalEntry {
    lsn: u64,
    committed: bool,
    data_len: u16,
    data: String,
}

impl WalEntry {
    pub fn new(lsn: u64, data: String) -> Self {
        Self {
            lsn,
            committed: false,
            data_len: data.len() as u16,
            data,
        }
    }

    pub fn is_committed(&self) -> bool {
        self.committed
    }

    pub fn get_data(&self) -> &str {
        &self.data
    }
}

impl Wal {
    pub fn create(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header = WalHeader::default();
        let mut wal = Self {
            path: path.to_owned(),
            file,
            header,
        };
        wal.flush_header()?;

        Ok(wal)
    }

    pub fn load(path: &Path) -> Result<WalType> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header: WalHeader = match deserialize_from(&mut BufReader::new(&file)) {
            Ok(header) => header,
            Err(_) => {
                let wal = Wal::recreate_wal(path)?;
                return Ok(WalType::Consistent(wal));
            }
        };

        let wal = Self {
            path: path.to_owned(),
            file,
            header,
        };

        match wal.define_consistency() {
            Ok(wal) => Ok(wal),
            Err(e) => Err(e),
        }
    }

    pub fn append(&mut self, data: String) -> Result<()> {
        self.header.lsn += 1;
        let entry = WalEntry::new(self.header.lsn, data);

        self.file.seek(SeekFrom::End(0))?;
        self.header.last_entry_offset = self.file.stream_position()?;

        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        self.flush_header()?;
        Ok(())
    }

    pub fn commit(&mut self) -> Result<()> {
        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let mut entry: WalEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        entry.committed = true;

        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;
        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        Ok(())
    }

    fn define_consistency(mut self) -> Result<WalType> {
        match self.get_last_entry() {
            Ok(Some(entry)) if !entry.is_committed() => {
                Ok(WalType::Uncommited(self, entry.get_data().to_owned()))
            }
            Ok(_) => Ok(WalType::Consistent(self)),
            Err(_) => Ok(WalType::Consistent(Wal::recreate_wal(&self.path)?)),
        }
    }

    fn get_last_entry(&mut self) -> Result<Option<WalEntry>> {
        let file_size = self.file.metadata()?.len();

        if file_size <= mem::size_of::<WalHeader>() as u64 {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let entry: WalEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        Ok(Some(entry))
    }

    fn flush_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;
        Ok(())
    }

    fn recreate_wal(path: &Path) -> Result<Self> {
        fs::remove_file(path)?;

        let wal = Wal::create(path)?;
        //TODO when other files will exist, we have to check header lsn and create wal with highest lsn

        Ok(wal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        fs,
        path::{Path, PathBuf},
    };
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    struct TestFileGuard {
        path: PathBuf,
    }

    impl TestFileGuard {
        fn new(path: &Path) -> std::io::Result<Self> {
            if path.exists() {
                fs::remove_file(path)?;
            }
            fs::File::create(path)?;

            Ok(Self {
                path: path.to_owned(),
            })
        }
    }

    impl Drop for TestFileGuard {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.path);
        }
    }

    #[test]
    fn load_should_return_error_if_file_does_not_exist() {
        let wal_file: &Path = Path::new("test.wal");

        let result = Wal::load(wal_file);

        assert!(result.is_err());
    }

    #[test]
    fn append_and_commit_should_commit_last_entry() -> Result<()> {
        let wal_file: &Path = Path::new("test1.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let wal = Wal::load(wal_file)?;

        let data = "Hello, World!".to_string();

        let mut wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited(_, _) => return Err("WAL is inconsistent.".into()),
        };

        wal.append(data.clone())?;

        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert_eq!(entry.data, data);

        assert!(!entry.committed);

        wal.commit()?;

        let entry = wal.get_last_entry().unwrap().unwrap();

        assert!(entry.committed);

        Ok(())
    }

    #[test]
    fn flush_header_changes_lsn_value() -> Result<()> {
        let wal_file: &Path = Path::new("test2.wal");

        let _guard = TestFileGuard::new(wal_file)?;

        let wal = Wal::load(wal_file)?;

        let mut wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited(_, _) => return Err("WAL is inconsistent.".into()),
        };

        wal.header.lsn = 10;
        wal.flush_header()?;

        let wal = Wal::load(wal_file)?;

        let wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited(_, _) => return Err("WAL is inconsistent.".into()),
        };

        assert_eq!(wal.header.lsn, 10);

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_none_if_no_entries() -> Result<()> {
        let wal_file: &Path = Path::new("test3.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let wal = Wal::load(wal_file)?;

        let mut wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited(_, _) => return Err("WAL is inconsistent.".into()),
        };

        let entry = wal.get_last_entry()?;

        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_last_entry_after_header_update() -> Result<()> {
        let wal_file: &Path = Path::new("test4.wal");

        let _guard = TestFileGuard::new(wal_file)?;
        let wal = Wal::load(wal_file)?;

        let mut wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited(_, _) => return Err("WAL is inconsistent.".into()),
        };

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
