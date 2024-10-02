use crate::types::{Lsn, WAL_FILE};
use crate::utils::common::get_file_name_from_path;

use super::{Error, Result};
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom};
use std::mem;
use std::path::{Path, PathBuf};

pub enum WalType {
    Consistent(Wal),
    Uncommited {
        wal: Wal,
        uncommited_command: String,
        arg: Option<String>,
    },
}

pub struct Wal {
    file_name: String,
    parent_path: PathBuf,
    file: File,
    header: WalHeader,
}

//TODO:: Checksum?
#[derive(Serialize, Deserialize)]
pub struct WalHeader {
    last_entry_offset: u64,
    current_max_lsn: u64,
}

impl WalHeader {
    pub fn new(current_max_lsn: Lsn) -> Self {
        Self {
            last_entry_offset: mem::size_of::<WalHeader>() as u64,
            current_max_lsn,
        }
    }
}

impl Default for WalHeader {
    fn default() -> Self {
        Self {
            last_entry_offset: mem::size_of::<WalHeader>() as u64,
            current_max_lsn: 0,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct WalEntry {
    lsn: Lsn,
    commited: bool,
    data_len: u16, //TODO: Is that needed? Maybe hash.
    data: String,
}

impl WalEntry {
    pub fn new(lsn: Lsn, commited: bool, data: String) -> Self {
        Self {
            lsn,
            commited,
            data_len: data.len() as u16,
            data,
        }
    }

    pub fn is_committed(&self) -> bool {
        self.commited
    }

    pub fn get_command_and_arg(&self) -> Result<(String, Option<String>)> {
        let mut parts = self.data.split_whitespace();

        match (parts.next(), parts.next()) {
            (Some(command), Some(arg)) => Ok((command.to_string(), Some(arg.to_string()))),
            (Some(command), None) => Ok((command.to_string(), None)),
            _ => Err(Error::ParsingEntry(self.data.to_owned())),
        }
    }
}

pub struct WalCreationSettings {
    pub name: String,
    pub current_max_lsn: Lsn,
}

impl Wal {
    pub fn create(path: &Path, custom_settings: Option<WalCreationSettings>) -> Result<Self> {
        let (file_name, header) = match custom_settings {
            Some(settings) => {
                let header = WalHeader::new(settings.current_max_lsn);
                (settings.name, header)
            }
            None => {
                let header = WalHeader::default();
                (WAL_FILE.to_owned(), header)
            }
        };

        let file_path = path.join(&file_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        let mut wal = Self {
            file_name,
            parent_path: path.to_owned(),
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
                let wal = Wal::recreate_wal(path)?; //TODO: ### Cannot deserialize header -> Read only state of collection or Database?
                return Ok(WalType::Consistent(wal));
            }
        };

        let file_name = get_file_name_from_path(path)?;

        let path = path.to_owned();
        let parent_path = path.parent().ok_or(Error::Unexpected {
            description: "Cannot get wal file parent's path.".to_owned(),
        })?;

        let wal = Self {
            file_name,
            parent_path: parent_path.to_owned(),
            file,
            header,
        };

        match wal.define_consistency() {
            Ok(wal) => Ok(wal),
            Err(e) => Err(e),
        }
    }

    pub fn append(&mut self, data: String) -> Result<Lsn> {
        self.header.current_max_lsn += 1;
        let entry = WalEntry::new(self.header.current_max_lsn, false, data);

        self.file.seek(SeekFrom::End(0))?;
        self.header.last_entry_offset = self.file.stream_position()?;

        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        self.flush_header()?;
        Ok(self.header.current_max_lsn)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(
            self.header.last_entry_offset + mem::size_of::<u64>() as u64,
        ))?;

        serialize_into(&mut BufWriter::new(&self.file), &true)?;

        Ok(())
    }

    fn define_consistency(mut self) -> Result<WalType> {
        match self.get_last_entry() {
            Ok(Some(entry)) if !entry.is_committed() => {
                let (uncommited_command, arg) = entry.get_command_and_arg()?;
                Ok(WalType::Uncommited {
                    wal: self,
                    uncommited_command,
                    arg,
                })
            }
            Ok(_) => Ok(WalType::Consistent(self)),
            Err(_) => Ok(WalType::Consistent(Wal::recreate_wal(
                &self.parent_path.join(self.file_name),
            )?)),
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

    pub fn truncate(self, lsn: Lsn) -> Result<Self> {
        let new_wal_name = format!("new_{WAL_FILE}");
        let cur_wal_path = self.parent_path.join(&self.file_name);
        let new_wal_path = self.parent_path.join(&new_wal_name);
        let bak_wal_path = self.parent_path.join(format!("{WAL_FILE}.bak"));

        let wal_settings = WalCreationSettings {
            name: new_wal_name,
            current_max_lsn: lsn,
        };

        let new_wal = Wal::create(&self.parent_path, Some(wal_settings))?;

        fs::rename(&cur_wal_path, &bak_wal_path)?;

        fs::rename(new_wal_path, &cur_wal_path)?;

        fs::remove_file(&bak_wal_path)?;

        Ok(new_wal)
    }

    fn recreate_wal(path: &Path) -> Result<Self> {
        fs::remove_file(path)?;

        let wal = Wal::create(path, None)?;
        //TODO when other files will exist, we have to check header lsn and create wal with highest lsn

        Ok(wal)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn append_should_append_uncommited_entry() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::create(temp_dir.path(), None)?;

        let data = "Hello, World!".to_string();

        //Act
        let lsn = wal.append(data.clone())?;

        //Assert
        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert_eq!(entry.data, data);
        assert_eq!(entry.lsn, lsn);
        assert!(!entry.commited);

        Ok(())
    }

    #[test]
    fn commit_should_mark_last_entry_as_committed() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::create(temp_dir.path(), None)?;

        let data = "Hello, World!".to_string();
        wal.append(data.clone())?;

        //Act
        wal.commit()?;

        //Assert
        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert!(entry.commited);

        Ok(())
    }

    #[test]
    fn flush_header_changes_lsn_value() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::create(temp_dir.path(), None)?;
        wal.header.current_max_lsn = 10;

        //Act
        wal.flush_header()?;

        //Assert
        let wal = Wal::load(&temp_dir.path().join(WAL_FILE))?;

        let wal = match wal {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited { .. } => return Err("WAL is inconsistent.".into()),
        };

        assert_eq!(wal.header.current_max_lsn, 10);

        Ok(())
    }

    #[test]
    fn get_last_entry_should_return_none_if_no_entries() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::create(temp_dir.path(), None)?;

        //Act
        let entry = wal.get_last_entry()?;

        //Assert
        assert!(entry.is_none());

        Ok(())
    }

    #[test]
    fn get_last_entry_should_return_last_entry_when_many_entries() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::create(temp_dir.path(), None)?;

        let data1 = "Hello, World!".to_string();
        let data2 = "2World, Hello!2".to_string();

        //Act
        wal.append(data1.clone())?;
        wal.append(data2.clone())?;

        //Assert
        let entry = wal.get_last_entry()?.ok_or("No last entry.")?;

        assert_eq!(entry.data, data2);
        assert_eq!(entry.lsn, 2);

        Ok(())
    }

    #[test]
    fn truncate_should_leave_wal_trucated_with_same_options() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let wal_name = "test_wal".to_string();

        let settings = WalCreationSettings {
            name: wal_name.clone(),
            current_max_lsn: 24,
        };

        let mut wal = Wal::create(temp_dir.path(), Some(settings))?;
        let last_lsn = wal.append("Hello, World!".to_string())?;

        let truncate_lsn = last_lsn + 1;

        //Act
        wal.truncate(truncate_lsn)?;

        //Assert
        let mut new_wal = match Wal::load(&temp_dir.path().join(&wal_name))? {
            WalType::Consistent(wal) => wal,
            WalType::Uncommited { .. } => return Err("WAL is inconsistent.".into()),
        };

        assert!(new_wal.get_last_entry()?.is_none());
        assert!(new_wal.header.current_max_lsn == truncate_lsn);
        assert!(new_wal.file_name == wal_name);

        Ok(())
    }
}
