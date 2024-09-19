use std::{
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::{Path, PathBuf},
};

use super::{
    types::{
        StorageCommand, StorageCommandResult, StorageDeleteResult, StorageInterface, StorageQuery,
        StorageQueryResult, StorageUpdateResult,
    },
    Error, Result,
};
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};

use crate::{
    components::collection::types::{NONE, NOT_SET},
    types::{Dim, Offset, LSN, STORAGE_FILE},
};

impl StorageInterface for Storage {
    fn perform_command(
        &mut self,
        command: StorageCommand,
        lsn: LSN,
    ) -> Result<StorageCommandResult> {
        let result = match command {
            StorageCommand::BulkInsert {
                vectors_and_payloads,
            } => {
                let offsets = self.bulk_insert(vectors_and_payloads, lsn)?;
                StorageCommandResult::BulkInserted { offsets }
            }
            StorageCommand::Insert { vector, payload } => {
                let offset = self.insert(vector, payload, lsn)?;
                StorageCommandResult::Inserted { offset }
            }
            StorageCommand::Update {
                offset,
                vector,
                payload,
            } => {
                let strg_update_result = self.update(offset, vector, payload, lsn)?;

                match strg_update_result {
                    StorageUpdateResult::NotFound => return Ok(StorageCommandResult::NotFound),
                    StorageUpdateResult::Updated { new_offset } => {
                        StorageCommandResult::Updated { new_offset }
                    }
                }
            }
        };

        self.header.modification_lsn = lsn; //TODO:: Modification lsn updated even if no changes were made?
        self.update_header()?;

        Ok(result)
    }

    fn perform_query(&mut self, query: StorageQuery) -> Result<StorageQueryResult> {
        let result = match query {
            StorageQuery::Search { offset } => {
                let record = self.search(offset)?;
                StorageQueryResult::SearchResult { record }
            }
        };

        Ok(result)
    }
}

pub struct Storage {
    path: PathBuf,
    file: File,
    header: StorageHeader,
}

//TODO: Offset backup for storing recently deleted record?
#[derive(Serialize, Deserialize, Clone)]
struct StorageHeader {
    modification_lsn: u64,
    vector_dim_amount: u16,
    checksum: u64,
}

impl StorageHeader {
    fn new(modification_lsn: u64, vector_dim_amount: u16) -> Self {
        let mut header = Self {
            modification_lsn,
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
                // TODO: mark collection as read-only
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
        self.modification_lsn.hash(state);
        self.vector_dim_amount.hash(state);
    }
}

impl Default for StorageHeader {
    fn default() -> Self {
        let mut header = Self {
            modification_lsn: 0,
            vector_dim_amount: 0,
            checksum: 0,
        };

        header.checksum = header.calculate_checksum();
        header
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Record {
    pub record_header: RecordHeader,
    pub vector: Vec<Dim>,
    pub payload: String,
}

impl Record {
    fn new(lsn: u64, vector: &[Dim], payload: &str) -> Self {
        let mut record = Self {
            record_header: RecordHeader::new(lsn),
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

    pub fn validate_checksum(&self) -> Result<()> {
        if self.record_header.checksum == self.calculate_checksum() {
            Ok(())
        } else {
            Err(Error::IncorrectChecksum {
                record: self.clone(),
                expected: self.record_header.checksum,
                actual: self.calculate_checksum(),
            })
        }
    }
}

impl Hash for Record {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.record_header.lsn.hash(state);
        self.record_header.deleted.hash(state);

        for dim in &self.vector {
            dim.to_bits().hash(state);
        }

        self.payload.hash(state);
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RecordHeader {
    pub lsn: u64,
    pub deleted: bool,
    checksum: u64,
}

impl RecordHeader {
    fn new(lsn: u64) -> Self {
        Self {
            lsn,
            deleted: false,
            checksum: NONE,
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
        storage.update_header()?;

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

    pub fn insert(&mut self, vector: &[Dim], payload: &str, lsn: LSN) -> Result<Offset> {
        self.validate_vector(vector)?;

        let record_offset = self.file.seek(SeekFrom::End(0))?;

        let record = Record::new(lsn, vector, payload);

        serialize_into(&mut BufWriter::new(&self.file), &record)?;

        Ok(record_offset)
    }

    pub fn bulk_insert(&mut self, records: &[(&[Dim], &str)], lsn: LSN) -> Result<Vec<Offset>> {
        let mut offsets = Vec::with_capacity(records.len());

        for (vector, payload) in records.iter() {
            let offset = self.insert(vector, payload, lsn)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    pub fn search(&mut self, offset: Offset) -> Result<Option<Record>> {
        self.file.seek(SeekFrom::Start(offset))?;
        match deserialize_from::<_, Record>(&mut BufReader::new(&self.file)) {
            Ok(record) => {
                record.validate_checksum()?;
                if record.record_header.deleted {
                    return Ok(None);
                }
                Ok(Some(record))
            }
            Err(e) => Err(Error::CannotDeserializeRecord { offset, source: e }),
        }
    }

    pub fn delete(&mut self, offset: Offset, lsn: LSN) -> Result<StorageDeleteResult> {
        if let Some(mut record) = self.search(offset)? {
            record.record_header.lsn = lsn;
            record.record_header.deleted = true;
            record.record_header.checksum = record.calculate_checksum();

            self.file.seek(SeekFrom::Start(offset))?;
            serialize_into(&mut BufWriter::new(&self.file), &record.record_header)?;

            Ok(StorageDeleteResult::Deleted)
        } else {
            Ok(StorageDeleteResult::NotFound)
        }
    }

    pub fn update(
        &mut self,
        offset: Offset,
        vector: Option<&[Dim]>,
        payload: Option<&str>,
        lsn: LSN,
    ) -> Result<StorageUpdateResult> {
        if let Some(mut record) = self.search(offset)? {
            if let Some(vector) = vector {
                self.validate_vector(vector)?;
                record.vector = vector.to_owned();
            }
            if let Some(payload) = payload {
                record.payload = payload.to_owned();
            }

            self.delete(offset, lsn)?;

            let new_offset = self.insert(&record.vector, &record.payload, lsn)?;

            self.update_header()?;

            Ok(StorageUpdateResult::Updated { new_offset })
        } else {
            Ok(StorageUpdateResult::NotFound)
        }
    }

    fn update_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        self.header.checksum = self.header.calculate_checksum();
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;

        self.file.sync_all()?;
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
    use bincode::serialized_size;
    use std::io::Write;

    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn insert_should_store_record_and_return_offset() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        //Act
        let offset = storage.insert(&vector, payload, 1)?;

        //Assert
        let mut file = File::open(storage.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let record: Record = deserialize_from(&mut BufReader::new(&file))?;

        assert_eq!(record.record_header.lsn, 1);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);
        Ok(())
    }

    #[test]
    fn insert_two_records_should_store_two_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        //Act
        let offset = storage.insert(&vector, payload, 1)?;
        let offset2 = storage.insert(&vector2, payload2, 2)?;

        //Assert
        let record = storage.search(offset)?;
        assert!(record.is_some());
        let record = record.unwrap();

        assert!(!record.record_header.deleted);
        assert!(record.record_header.lsn == 1);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);

        let record2 = storage.search(offset2)?;
        assert!(record2.is_some());
        let record2 = record2.unwrap();

        assert!(!record2.record_header.deleted);
        assert!(record2.record_header.lsn == 2);
        assert_eq!(record2.record_header.checksum, record2.calculate_checksum());
        assert_eq!(record2.vector, vector2);
        assert_eq!(record2.payload, payload2);

        Ok(())
    }

    #[test]
    fn bulk_insert_two_records_should_store_two_record() -> Result<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<f32> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        // Act
        let offsets = storage.bulk_insert(
            &[(vector.as_slice(), payload), (vector2.as_slice(), payload2)],
            1,
        )?;

        // Assert
        let record = storage.search(offsets[0])?;
        assert!(record.is_some());
        let record = record.unwrap();

        assert_eq!(record.record_header.lsn, 1);
        assert!(!record.record_header.deleted);
        assert_eq!(record.record_header.checksum, record.calculate_checksum());
        assert_eq!(record.vector, vector);
        assert_eq!(record.payload, payload);

        let record2 = storage.search(offsets[1])?;
        assert!(record2.is_some());
        let record2 = record2.unwrap();

        assert_eq!(record2.record_header.lsn, 1);
        assert!(!record2.record_header.deleted);
        assert_eq!(record2.record_header.checksum, record2.calculate_checksum());
        assert_eq!(record2.vector, vector2);
        assert_eq!(record2.payload, payload2);

        Ok(())
    }

    #[test]
    fn bulk_insert_empty_array_should_return_empty_offsets() -> Result<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;

        // Act
        let offsets = storage.bulk_insert(&[], 1)?;

        // Assert
        assert!(offsets.is_empty());

        Ok(())
    }

    #[test]
    fn search_record_should_return_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let offset = storage.insert(&vector, payload, 1)?;

        //Act
        let record = storage.search(offset)?;

        //Assert
        assert!(record.is_some());
        let record = record.unwrap();
        assert_eq!(record.record_header.lsn, 1);
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

        let _ = storage.insert(&vector, payload, 1)?;
        let offset = storage.insert(&vector, payload, 2)?;

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

        let offset = storage.insert(&vector, payload, 1)?;

        //Act
        storage.delete(offset, 2)?;

        //Assert
        let record = storage.search(offset)?;
        assert!(record.is_none());

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

        let offset = storage.insert(&vector, payload, 1)?;

        //Act
        let update_result = storage.update(offset, Some(&new_vector), Some(new_payload), 1)?;

        //Assert
        let old_record = storage.search(offset)?;

        assert!(old_record.is_none());

        match update_result {
            StorageUpdateResult::Updated { new_offset } => {
                let record = storage.search(new_offset)?;

                assert!(record.is_some());
                let record = record.unwrap();
                assert_eq!(record.record_header.lsn, 1);
                assert!(!record.record_header.deleted);
                assert_eq!(record.record_header.checksum, record.calculate_checksum());
                assert_eq!(record.vector, new_vector);
                assert_eq!(record.payload, new_payload);
                Ok(())
            }
            _ => panic!("Expected 'StorageUpdateResult::Updated'"),
        }
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
        let result = storage.insert(&vector, payload, 1);
        let result2 = storage.insert(&vector2, payload2, 2);

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
        let offset = storage.insert(&vector, payload, 1)?;
        let result = storage.update(offset, Some(&vector2), None, 2);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[ignore = "Not sure if it will be needed."]
    fn load_should_define_header_on_when_header_has_been_corrupted() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path())?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let _ = storage.insert(&vector, payload, 1)?;
        let checksum = storage.header.checksum;

        let mut file = File::open(&storage.path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&file);
        writer.write_all(b"corrupted data")?;

        //Act
        let storage = Storage::load(&storage.path)?;

        //Assert
        assert_eq!(storage.header.modification_lsn, 1); //TODO: Should it somehow get the max lsn? How?
        assert_eq!(storage.header.vector_dim_amount, 3); //TODO: Maybe make it readonly instead of defining it?
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
        assert_eq!(storage.header.modification_lsn, 0);
        assert_eq!(storage.header.vector_dim_amount, 0);
        assert_eq!(storage.header.checksum, checksum);
        Ok(())
    }

    #[test]
    fn validate_checksum_should_return_error_when_checksum_is_incorrect() -> Result<()> {
        //Arrange
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let mut record = Record::new(1, &vector, payload);
        record.payload = "corrupted payload".to_owned();

        //Act
        let result = record.validate_checksum();

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn validate_checksum_should_return_ok_when_checksum_is_correct() -> Result<()> {
        //Arrange
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let record = Record::new(1, &vector, payload);

        //Act
        let result = record.validate_checksum();

        //Assert
        assert!(result.is_ok());
        Ok(())
    }
}
