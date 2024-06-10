use std::{
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::{Path, PathBuf},
};

use bincode::{deserialize_from, serialize_into, serialized_size};
use serde::{Deserialize, Serialize};

use crate::types::STORAGE_FILE;

use super::{Error, Result};

pub struct Storage {
    path: PathBuf,
    file: File,
    header: StorageHeader,
}

#[derive(Serialize, Deserialize, Default)]
pub struct StorageHeader {
    current_max_lsn: u64,
}

impl StorageHeader {
    fn define_header(storage_path: &Path) -> Result<StorageHeader> {
        let header = StorageHeader::default();
        let mut storage = Storage {
            path: storage_path.to_owned(),
            file: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(storage_path)?,
            header,
        };

        //TODO: modify header basing on the latest record.

        storage.flush_header()?;
        Ok(storage.header)
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    record_header: RecordHeader,
    vector: Vec<f32>,
    payload: String,
}

impl Record {
    pub fn new(lsn: u64, id: u64, payload_offset: u64, vector: Vec<f32>, payload: &str) -> Self {
        let mut record = Self {
            record_header: RecordHeader::new(lsn, id, payload_offset),
            vector,
            payload: payload.to_owned(),
        };

        record.record_header.checksum = record.calculate_checksum();
        record
    }

    pub fn calculate_checksum(&self) -> u64 {
        let mut temp_record = self.clone();
        temp_record.record_header.checksum = 0;

        let mut hasher = DefaultHasher::new();
        let mut temp_buffer = Vec::new();
        serialize_into(&mut temp_buffer, &temp_record).unwrap();
        temp_buffer.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct RecordHeader {
    lsn: u64,
    id: u64,
    deleted: bool,
    checksum: u64,
    payload_offset: u64,
}

impl RecordHeader {
    pub fn new(lsn: u64, id: u64, payload_offset: u64) -> Self {
        Self {
            lsn,
            id,
            deleted: false,
            checksum: 0,
            payload_offset,
        }
    }
}

impl Storage {
    pub fn create(path: &Path) -> Result<Self> {
        let file_path = path.join(STORAGE_FILE);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)?;

        let header = StorageHeader::default();
        let mut storage = Self {
            path: file_path,
            file,
            header,
        };
        storage.flush_header()?;

        Ok(storage)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header: StorageHeader = match deserialize_from(&mut BufReader::new(&file)) {
            Ok(header) => header,
            Err(_) => StorageHeader::define_header(path)?,
        };

        let storage = Self {
            path: path.to_owned(),
            file,
            header,
        };

        Ok(storage)
    }

    pub fn insert(&mut self, lsn: u64, id: u64, vector: Vec<f32>, payload: &str) -> Result<u64> {
        let record_offset = self.file.seek(SeekFrom::End(0))?;

        let payload_offset =
            record_offset + mem::size_of::<RecordHeader>() as u64 + serialized_size(&vector)?;

        let record = Record::new(lsn, id, payload_offset, vector, payload);

        serialize_into(&mut BufWriter::new(&self.file), &record)?;

        self.header.current_max_lsn += 1;
        self.flush_header()?;

        Ok(record_offset)
    }

    pub fn search(&mut self, offset: u64) -> Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;
        match deserialize_from(&mut BufReader::new(&self.file)) {
            Ok(record) => Ok(record),
            Err(e) => Err(Error::CannotDeserializeRecord { offset, source: e }),
        }
    }

    pub fn delete(&mut self, lsn: u64, offset: u64) -> Result<()> {
        let mut record = self.search(offset)?;
        record.record_header.lsn = lsn;
        record.record_header.deleted = true;
        record.record_header.checksum = record.calculate_checksum();

        self.file.seek(SeekFrom::Start(offset))?;
        serialize_into(&mut BufWriter::new(&self.file), &record.record_header)?;

        Ok(())
    }

    pub fn update(
        &mut self,
        lsn: u64,
        offset: u64,
        vector: Option<Vec<f32>>,
        payload: Option<&str>,
    ) -> Result<u64> {
        let mut record = self.search(offset)?;

        if let Some(vector) = vector {
            record.vector = vector;
        }
        if let Some(payload) = payload {
            record.payload = payload.to_owned();
        }

        self.delete(lsn, offset)?;

        let new_offset =
            self.insert(lsn, record.record_header.id, record.vector, &record.payload)?;

        Ok(new_offset)
    }

    fn flush_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn insert_record_should_store_record_and_return_offset() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        //Act
        let offset = storage.insert(lsn, next_id, vector.clone(), payload)?;

        //Assert
        let mut file = File::open(storage.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let record: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert_eq!(record.record_header.lsn, lsn);
        assert_eq!(record.record_header.id, next_id);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);

        Ok(())
    }

    #[test]
    fn inserting_two_records_should_store_two_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let lsn2 = 2;
        let next_id2 = 2;
        let vector2: Vec<f32> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        //Act
        let offset1 = storage.insert(lsn, next_id, vector.clone(), payload)?;
        let offset2 = storage.insert(lsn2, next_id2, vector2.clone(), payload2)?;

        //Assert
        let mut file = File::open(storage.path)?;
        file.seek(SeekFrom::Start(offset1))?;
        let record1: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert_eq!(record1.record_header.lsn, lsn);
        assert_eq!(record1.record_header.id, next_id);
        assert!(!record1.record_header.deleted);
        assert_eq!(record1.record_header.checksum, record1.calculate_checksum());
        assert_eq!(record1.vector, vector);
        assert_eq!(record1.payload, payload);

        file.seek(SeekFrom::Start(offset2))?;
        let record2: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert_eq!(record2.record_header.lsn, lsn2);
        assert_eq!(record2.record_header.id, next_id2);
        assert!(!record2.record_header.deleted);
        assert_eq!(record2.record_header.checksum, record2.calculate_checksum());
        assert_eq!(record2.vector, vector2);
        assert_eq!(record2.payload, payload2);

        Ok(())
    }

    #[test]
    fn search_record_should_return_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let offset = storage.insert(lsn, next_id, vector.clone(), payload)?;

        //Act
        let record = storage.search(offset)?;

        //Assert
        assert_eq!(record.record_header.lsn, lsn);
        assert_eq!(record.record_header.id, next_id);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);

        Ok(())
    }

    #[test]
    fn search_record_should_return_error_when_offset_does_not_exist() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let _ = storage.insert(lsn, next_id, vector.clone(), payload)?;
        let offset = storage.insert(lsn, next_id, vector.clone(), payload)?;
        let record = storage.search(offset)?;
        let invalid_offset = offset - serialized_size(&record)? - 1;

        //Act
        let result = storage.search(invalid_offset);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn delete_record_should_delete_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let new_lsn = lsn + 1;

        let offset = storage.insert(lsn, next_id, vector.clone(), payload)?;

        //Act
        storage.delete(new_lsn, offset)?;

        //Assert
        let record = storage.search(offset)?;

        assert_eq!(record.record_header.lsn, new_lsn);
        assert_eq!(record.record_header.id, next_id);
        assert!(record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);
        Ok(())
    }

    #[test]
    fn update_record_should_update_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let lsn = 1;
        let next_id = 1;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let new_lsn = lsn + 1;
        let new_vector: Vec<f32> = vec![2.0, 3.0, 4.0];
        let new_payload = "test2";

        let offset = storage.insert(lsn, next_id, vector.clone(), payload)?;

        //Act
        let new_offset =
            storage.update(new_lsn, offset, Some(new_vector.clone()), Some(new_payload))?;

        //Assert
        let old_record = storage.search(offset)?;

        assert_eq!(old_record.record_header.lsn, new_lsn);
        assert!(old_record.record_header.deleted);

        let record = storage.search(new_offset)?;

        assert_eq!(record.record_header.lsn, new_lsn);
        assert_eq!(record.record_header.id, next_id);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, new_vector);
        assert_eq!(record.payload, new_payload);
        Ok(())
    }
}
