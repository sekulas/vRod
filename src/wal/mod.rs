pub mod types;

use self::types::HEADER_SIZE;
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};

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
            last_entry_offset: HEADER_SIZE,
            lsn: 0,
        }
    }
}

impl WAL {
    fn load(path: &str) -> Result<Self, bincode::Error> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header = match deserialize_from(&mut BufReader::new(&file)) {
            Ok(header) => header,
            Err(_) => {
                let wal = WAL::recreate_wal(path)?;
                return Ok(wal);
            }
        };

        Ok(Self { file, header })
    }

    fn append(&mut self, data: String) -> Result<(), bincode::Error> {
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

    fn commit(&mut self, lsn: u64) -> Result<(), bincode::Error> {
        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let mut entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        entry.committed = true;

        if entry.lsn == lsn {
            self.file
                .seek(SeekFrom::Start(self.header.last_entry_offset))?;
            let mut writer = BufWriter::new(&self.file);
            serialize_into(&mut writer, &entry)?;
            writer.flush()?;
        }
        Ok(())
    }

    fn get_last_entry(&mut self) -> Result<Option<WALEntry>, bincode::Error> {
        let file_size = self.file.metadata()?.len();

        if file_size <= HEADER_SIZE {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.header.last_entry_offset))?;

        let entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        Ok(Some(entry))
    }

    fn flush_header(&mut self) -> Result<(), bincode::Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&self.file);
        serialize_into(&mut writer, &self.header)?;
        writer.flush()?;
        Ok(())
    }

    fn recreate_wal(path: &str) -> Result<Self, bincode::Error> {
        fs::remove_file(path)?;
        fs::File::create(path)?;

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header = WALHeader::default();
        let mut wal = Self { file, header };
        wal.flush_header()?;
        //TODO when other files will exist, we have to check header lsn and create wal with highest lsn

        Ok(wal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{fs, path::PathBuf};

    struct TestFileGuard {
        path: PathBuf,
    }

    impl TestFileGuard {
        fn new(path: &str) -> std::io::Result<Self> {
            let path = PathBuf::from(path);

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
    fn append_and_commit_should_commit_last_entry() -> Result<(), Box<dyn std::error::Error>> {
        let wal_file = "test1.wal";

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
    fn flush_header_changes_lsn_value() -> Result<(), bincode::Error> {
        let wal_file: &str = "test2.wal";

        let _guard = TestFileGuard::new(wal_file)?;

        let mut wal = WAL::load(wal_file)?;
        wal.header.lsn = 10;
        wal.flush_header()?;

        let wal = WAL::load(wal_file)?;

        assert_eq!(wal.header.lsn, 10);

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_none_if_no_entries() -> Result<(), bincode::Error> {
        let wal_file: &str = "test3.wal";

        let _guard = TestFileGuard::new(wal_file)?;
        let mut wal = WAL::load(wal_file)?;

        let entry = wal.get_last_entry()?;

        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn get_last_entry_returns_last_entry_after_header_update(
    ) -> Result<(), Box<dyn std::error::Error>> {
        let wal_file: &str = "test4.wal";

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
