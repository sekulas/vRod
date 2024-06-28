use std::{
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::{Path, PathBuf},
};

use super::{
    types::{OperationMode, NONE, NOT_SET},
    Error, Result,
};
use bincode::{deserialize_from, serialize_into, serialized_size};
use serde::{Deserialize, Serialize};

use crate::types::{Dim, Offset, STORAGE_FILE};

pub struct Storage {
    path: PathBuf,
    file: File,
    header: StorageHeader,
}

#[derive(Serialize, Deserialize, Default, Clone)]
struct StorageHeader {
    current_max_lsn: u64,
    vector_dim_amount: u16,
    checksum: u64,
}

impl StorageHeader {
    fn new(current_max_lsn: u64, vector_dim_amount: u16) -> Self {
        let mut header = Self {
            current_max_lsn,
            vector_dim_amount,
            checksum: NONE,
        };

        header.checksum = header.calculate_checksum();
        header
    }

    fn define_header(file: &mut File) -> Result<StorageHeader> {
        file.seek(SeekFrom::Start(mem::size_of::<StorageHeader>() as u64))?;
        let mut reader = BufReader::new(file);

        let mut max_lsn = NONE;
        let mut vec_dim_amount = NOT_SET;

        match deserialize_from::<_, Record>(&mut reader) {
            Ok(record) => {
                max_lsn = record.record_header.lsn;
                vec_dim_amount = record.vector.len() as u16;

                while let Ok(record) = deserialize_from::<_, Record>(&mut reader) {
                    if record.record_header.lsn > max_lsn {
                        max_lsn = record.record_header.lsn;
                    }
                }
            }
            Err(_) => {
                println!(
                    "Cannot deserialize first record in storage file - leaving default values."
                );
                // TODO: isn't this a problem in the future?
                // Rollbacks are going to be working basing on the LSN's
                // But if this will be default accidently then
                // maybe let's do WAL truncating - and begin from 0?
                // But will be always during truncate go to 0 in WAL?
                // Won't that cause problems in the future?
                // If it's cannot be deserialized then whatever
                // Collection has nothing then maybe mark it as unused?
            }
        }
        println!(
            "Header defined: max_lsn: {}, vec_dim_amount: {}",
            max_lsn, vec_dim_amount
        );

        Ok(StorageHeader::new(max_lsn, vec_dim_amount))
    }

    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for StorageHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.current_max_lsn.hash(state);
        self.vector_dim_amount.hash(state);
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    record_header: RecordHeader,
    vector: Vec<Dim>,
    payload: String,
}

impl Record {
    fn new(lsn: u64, payload_offset: Offset, vector: &[Dim], payload: &str) -> Self {
        let mut record = Self {
            record_header: RecordHeader::new(lsn, payload_offset),
            vector: vector.to_owned(),
            payload: payload.to_owned(),
        };

        record.record_header.checksum = record.calculate_checksum();
        record
    }

    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.record_header.lsn.hash(state);
        self.record_header.deleted.hash(state);
        self.record_header.payload_offset.hash(state);

        //TODO: checksum based on the whole vector?
        for dim in &self.vector {
            // Hash each f32 value individually
            dim.to_bits().hash(state);
        }

        self.payload.hash(state);
    }
}

#[derive(Serialize, Deserialize, Clone)]
struct RecordHeader {
    lsn: u64,
    deleted: bool,
    checksum: u64,
    payload_offset: Offset,
}

impl RecordHeader {
    fn new(lsn: u64, payload_offset: Offset) -> Self {
        Self {
            lsn,
            deleted: false,
            checksum: NONE,
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
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header: StorageHeader =
            match deserialize_from::<_, StorageHeader>(&mut BufReader::new(&file)) {
                Ok(header) => {
                    if header.checksum != header.calculate_checksum() {
                        println!("Checksum incorrect for 'Storage' header - defining header.");
                        StorageHeader::define_header(&mut file)?;
                    }

                    header
                }
                Err(_) => {
                    println!("Cannot deserialize header for the 'Storage' - defining header.");
                    StorageHeader::define_header(&mut file)?
                }
            };

        let storage = Self {
            path: path.to_owned(),
            file,
            header,
        };

        Ok(storage)
    }

    pub fn insert(
        &mut self,
        vector: &[Dim],
        payload: &str,
        mode: &OperationMode,
    ) -> Result<Offset> {
        self.validate_vector(vector)?;

        if let OperationMode::RawOperation = mode {
            self.header.current_max_lsn += 1;
        }

        let record_offset = self.file.seek(SeekFrom::End(0))?;

        let payload_offset =
            record_offset + mem::size_of::<RecordHeader>() as u64 + serialized_size(&vector)?;

        let record = Record::new(self.header.current_max_lsn, payload_offset, vector, payload);

        serialize_into(&mut BufWriter::new(&self.file), &record)?;
        if let OperationMode::RawOperation = mode {
            self.flush_header()?;
        }

        Ok(record_offset)
    }

    pub fn search(&mut self, offset: Offset) -> Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;
        match deserialize_from(&mut BufReader::new(&self.file)) {
            Ok(record) => Ok(record),
            Err(e) => Err(Error::CannotDeserializeRecord { offset, source: e }),
        }
    }

    pub fn delete(&mut self, offset: Offset, mode: &OperationMode) -> Result<()> {
        let mut record = self.search(offset)?;

        if let OperationMode::RawOperation = mode {
            self.header.current_max_lsn += 1;
        }

        record.record_header.lsn = self.header.current_max_lsn;
        record.record_header.deleted = true;
        record.record_header.checksum = record.calculate_checksum();

        self.file.seek(SeekFrom::Start(offset))?;
        serialize_into(&mut BufWriter::new(&self.file), &record.record_header)?;

        if let OperationMode::RawOperation = mode {
            self.flush_header()?;
        }

        Ok(())
    }

    pub fn update(
        &mut self,
        offset: Offset,
        vector: Option<&[Dim]>,
        payload: Option<&str>,
    ) -> Result<u64> {
        let mut record = self.search(offset)?;

        if let Some(vector) = vector {
            self.validate_vector(vector)?;
            record.vector = vector.to_owned();
        }
        if let Some(payload) = payload {
            record.payload = payload.to_owned();
        }

        self.header.current_max_lsn += 1;
        let mode = OperationMode::InUpdateOperation;

        self.delete(offset, &mode)?;

        let new_offset = self.insert(&record.vector, &record.payload, &mode)?;

        self.flush_header()?;

        Ok(new_offset)
    }

    fn flush_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;
        Ok(())
    }

    fn validate_vector(&mut self, vector: &[Dim]) -> Result<()> {
        match self.header.vector_dim_amount {
            NOT_SET => {
                self.header.vector_dim_amount = vector.len() as u16;
            }
            expected => {
                if vector.len() as u16 != expected {
                    return Err(Error::InvalidVectorDim {
                        expected,
                        actual: vector.len() as u16,
                        vector: vector.to_owned(),
                    });
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn insert_record_should_store_record_and_return_offset() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        //Act
        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;

        //Assert
        let mut file = File::open(storage.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let record: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert_eq!(record.record_header.lsn, storage.header.current_max_lsn);
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
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        //Act
        let offset1 = storage.insert(&vector, payload, &OperationMode::RawOperation)?;
        let offset2 = storage.insert(&vector2, payload2, &OperationMode::RawOperation)?;

        //Assert
        let mut file = File::open(storage.path)?;
        file.seek(SeekFrom::Start(offset1))?;
        let record1: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert!(!record1.record_header.deleted);
        assert_eq!(record1.record_header.checksum, record1.calculate_checksum());
        assert_eq!(record1.vector, vector);
        assert_eq!(record1.payload, payload);

        file.seek(SeekFrom::Start(offset2))?;
        let record2: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert!(!record2.record_header.deleted);
        assert_eq!(record2.record_header.checksum, record2.calculate_checksum());
        assert_eq!(record2.vector, vector2);
        assert_eq!(record2.payload, payload2);

        assert_eq!(storage.header.current_max_lsn, record2.record_header.lsn);
        Ok(())
    }

    #[test]
    fn search_record_should_return_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;

        //Act
        let record = storage.search(offset)?;

        //Assert
        assert_eq!(record.record_header.lsn, storage.header.current_max_lsn);
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
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let _ = storage.insert(&vector, payload, &OperationMode::RawOperation)?;
        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;

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
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;

        //Act
        storage.delete(offset, &OperationMode::RawOperation)?;

        //Assert
        let record = storage.search(offset)?;

        assert!(record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);

        assert_eq!(storage.header.current_max_lsn, record.record_header.lsn);
        Ok(())
    }

    #[test]
    fn update_record_should_update_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let new_vector: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let new_payload = "test2";

        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;

        //Act
        let new_offset = storage.update(offset, Some(&new_vector), Some(new_payload))?;

        //Assert
        let old_record = storage.search(offset)?;

        assert_eq!(old_record.record_header.lsn, storage.header.current_max_lsn);
        assert!(old_record.record_header.deleted);

        let record = storage.search(new_offset)?;

        assert_eq!(record.record_header.lsn, storage.header.current_max_lsn);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, new_vector);
        assert_eq!(record.payload, new_payload);
        Ok(())
    }

    #[test]
    fn inserting_vecs_with_different_dim_amounts_should_return_error() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0, 5.0];
        let payload2 = "test2";

        //Act
        let result = storage.insert(&vector, payload, &OperationMode::RawOperation);
        let result2 = storage.insert(&vector2, payload2, &OperationMode::RawOperation);

        //Assert
        assert!(result.is_ok());
        assert!(result2.is_err());
        Ok(())
    }

    #[test]
    fn updating_vec_to_different_dim_amount_should_return_error() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0, 5.0];

        //Act
        let offset = storage.insert(&vector, payload, &OperationMode::RawOperation)?;
        let result = storage.update(offset, Some(&vector2), None);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn load_should_define_header_on_when_header_has_been_corrupted() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let _ = storage.insert(&vector, payload, &OperationMode::RawOperation)?;
        let checksum = storage.header.checksum;

        let mut file = File::open(&storage.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&file);
        writer.write_all(b"corrupted data")?;

        //Act
        let storage = Storage::load(&storage.path)?;

        //Assert
        assert_eq!(storage.header.current_max_lsn, 1);
        assert_eq!(storage.header.vector_dim_amount, 3);
        assert_eq!(storage.header.checksum, checksum);
        Ok(())
    }

    #[test]
    fn load_should_define_header_with_default_values_when_no_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let storage = Storage::create(temp_dir.path())?;
        let checksum = storage.header.checksum;

        let mut file = File::open(&storage.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&file);
        writer.write_all(b"corrupted data")?;

        //Act
        let storage = Storage::load(&storage.path)?;

        //Assert
        assert_eq!(storage.header.current_max_lsn, 0);
        assert_eq!(storage.header.vector_dim_amount, 0);
        assert_eq!(storage.header.checksum, checksum);
        Ok(())
    }
}
