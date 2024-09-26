use std::{
    fmt::Display,
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::Path,
};

use super::{
    types::{
        StorageCommand, StorageCommandResult, StorageCreationSettings, StorageDeleteResult,
        StorageInterface, StorageQuery, StorageQueryResult, StorageUpdateResult,
    },
    Error, Result,
};
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};

use crate::{
    components::collection::{
        get_file_name_from_path,
        types::{NONE, NOT_SET},
    },
    types::{Dim, Lsn, Offset, STORAGE_FILE},
};

impl StorageInterface for Storage {
    fn perform_command(
        &mut self,
        command: StorageCommand,
        lsn: Lsn,
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
            StorageCommand::Delete { offset } => {
                let strg_delete_result = self.delete(offset, lsn)?;

                match strg_delete_result {
                    StorageDeleteResult::NotFound => return Ok(StorageCommandResult::NotFound),
                    StorageDeleteResult::Deleted => StorageCommandResult::Deleted,
                }
            }
        };

        self.header.modification_lsn = lsn; //TODO:: Modification lsn updated even if no changes were made? Example: Insert [] empty array.
        self.update_header()?;

        Ok(result)
    }

    fn perform_query(&mut self, query: StorageQuery) -> Result<StorageQueryResult> {
        let result = match query {
            StorageQuery::Search { offset } => {
                let record = self.search(offset)?;

                match record {
                    Some(record) => StorageQueryResult::FoundRecord { record },
                    None => StorageQueryResult::NotFound,
                }
            }
        };

        Ok(result)
    }

    fn perform_rollback(&mut self, lsn: Lsn) -> Result<()> {
        if lsn - 1 != self.header.modification_lsn {
            return Err(Error::Unexpected("Index: Cannot rollback - LSN mismatch."));
        }

        self.rollback_last_ud_command()?;

        self.header.modification_lsn -= 1;
        self.update_header()?;

        Ok(())
    }
}

pub struct Storage {
    pub file_name: String,
    file: File,
    header: StorageHeader,
}

//TODO: Offset backup for storing recently deleted record?
#[derive(Serialize, Deserialize, Clone)]
struct StorageHeader {
    modification_lsn: Lsn,
    vector_dim_amount: u16,
    checksum: u64,
    backup_offset: Offset,
}

impl StorageHeader {
    fn new(modification_lsn: u64, vector_dim_amount: u16) -> Self {
        let mut header = Self {
            modification_lsn,
            vector_dim_amount,
            checksum: NONE,
            backup_offset: NONE,
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
        self.backup_offset.hash(state);
    }
}

impl Default for StorageHeader {
    fn default() -> Self {
        let mut header = Self {
            modification_lsn: 0,
            vector_dim_amount: 0,
            checksum: 0,
            backup_offset: NONE,
        };

        header.checksum = header.calculate_checksum();
        header
    }
}

#[cfg_attr(test, derive(PartialEq, Debug))]
#[derive(Serialize, Deserialize, Clone)]
pub struct Record {
    pub record_header: RecordHeader,
    pub vector: Vec<Dim>,
    pub payload: String,
}

impl Record {
    pub fn new(lsn: u64, vector: &[Dim], payload: &str) -> Self {
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

#[cfg_attr(test, derive(PartialEq, Debug))]
#[derive(Serialize, Deserialize, Clone)]
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
    pub fn create(path: &Path, custom_settings: Option<StorageCreationSettings>) -> Result<Self> {
        let (file_name, header) = match custom_settings {
            Some(settings) => {
                let header =
                    StorageHeader::new(settings.modification_lsn, settings.vector_dim_amount);
                (settings.name, header)
            }
            None => {
                let header = StorageHeader::default();
                (STORAGE_FILE.to_owned(), header)
            }
        };

        let file_path = path.join(&file_name);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        let mut storage = Self {
            file_name,
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

        let file_name = get_file_name_from_path(path)?;

        let storage = Self {
            file_name,
            file,
            header,
        };

        Ok(storage)
    }

    fn insert(&mut self, vector: &[Dim], payload: &str, lsn: Lsn) -> Result<Offset> {
        self.validate_vector(vector)?;

        let record_offset = self.file.seek(SeekFrom::End(0))?;

        let record = Record::new(lsn, vector, payload);

        serialize_into(&mut BufWriter::new(&self.file), &record)?;

        Ok(record_offset)
    }

    fn bulk_insert(&mut self, records: &[(&[Dim], &str)], lsn: Lsn) -> Result<Vec<Offset>> {
        let mut offsets = Vec::with_capacity(records.len());

        for (vector, payload) in records.iter() {
            let offset = self.insert(vector, payload, lsn)?;
            offsets.push(offset);
        }

        Ok(offsets)
    }

    fn search(&mut self, offset: Offset) -> Result<Option<Record>> {
        match self.search_with_deleted(offset)? {
            Some(record) => {
                if record.record_header.deleted {
                    return Ok(None);
                }
                Ok(Some(record))
            }
            None => Ok(None),
        }
    }

    fn search_with_deleted(&mut self, offset: Offset) -> Result<Option<Record>> {
        self.file.seek(SeekFrom::Start(offset))?;
        match deserialize_from::<_, Record>(&mut BufReader::new(&self.file)) {
            Ok(record) => {
                record.validate_checksum()?;
                Ok(Some(record))
            }
            Err(e) => Err(Error::CannotDeserializeRecord { offset, source: e }),
        }
    }

    fn delete(&mut self, offset: Offset, lsn: Lsn) -> Result<StorageDeleteResult> {
        if let Some(mut record) = self.search(offset)? {
            record.record_header.lsn = lsn;
            record.record_header.deleted = true;
            record.record_header.checksum = record.calculate_checksum();

            self.file.seek(SeekFrom::Start(offset))?;
            serialize_into(&mut BufWriter::new(&self.file), &record.record_header)?;

            self.header.backup_offset = offset;

            Ok(StorageDeleteResult::Deleted)
        } else {
            Ok(StorageDeleteResult::NotFound)
        }
    }

    fn update(
        &mut self,
        offset: Offset,
        vector: Option<&[Dim]>,
        payload: Option<&str>,
        lsn: Lsn,
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

            self.header.backup_offset = offset;

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

    fn rollback_last_ud_command(&mut self) -> Result<()> {
        if let Some(mut record) = self.search_with_deleted(self.header.backup_offset)? {
            record.record_header.lsn -= 1;
            record.record_header.deleted = false;
            record.record_header.checksum = record.calculate_checksum();

            self.file.seek(SeekFrom::Start(self.header.backup_offset))?;
            serialize_into(&mut BufWriter::new(&self.file), &record.record_header)?;

            Ok(())
        } else {
            Err(Error::RecordNotFoundForRollback {
                offset: self.header.backup_offset,
            })
        }
    }

    pub fn get_creation_settings(&self) -> StorageCreationSettings {
        StorageCreationSettings {
            name: self.file_name.clone(),
            modification_lsn: self.header.modification_lsn,
            vector_dim_amount: self.header.vector_dim_amount,
        }
    }
}

#[cfg(test)]
mod tests {
    use bincode::serialized_size;
    use std::io::Write;

    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[ignore = "Not sure if it will be needed."]
    fn load_should_define_header_on_when_header_has_been_corrupted() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join(STORAGE_FILE);
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let _ = storage.insert(&vector, payload, 1)?;
        let checksum = storage.header.checksum;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&file);
        writer.write_all(b"corrupted data")?;

        //Act
        let storage = Storage::load(&path)?;

        //Assert
        assert_eq!(storage.header.modification_lsn, 1); //TODO: Should it somehow get the max lsn? How?
        assert_eq!(storage.header.vector_dim_amount, 3); //TODO: Maybe make it readonly instead of defining it?
        assert_eq!(storage.header.checksum, checksum);
        Ok(())
    }

    #[test]
    fn create_with_custom_settings_should_create_storage_with_custom_settings() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let settings = StorageCreationSettings {
            name: "custom_storage".to_string(),
            modification_lsn: 1,
            vector_dim_amount: 3,
        };

        //Act
        let storage = Storage::create(temp_dir.path(), Some(settings))?;

        //Assert
        assert_eq!(storage.header.modification_lsn, 1);
        assert_eq!(storage.header.vector_dim_amount, 3);
        Ok(())
    }

    #[test]
    fn load_should_define_header_with_default_values_when_no_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join(STORAGE_FILE);
        let storage = Storage::create(temp_dir.path(), None)?;
        let checksum = storage.header.checksum;

        let mut file = File::open(&path)?;
        file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&file);
        writer.write_all(b"corrupted data")?;

        //Act
        let storage = Storage::load(&path)?;

        //Assert
        assert_eq!(storage.header.modification_lsn, 0);
        assert_eq!(storage.header.vector_dim_amount, 0);
        assert_eq!(storage.header.checksum, checksum);
        Ok(())
    }

    #[test]
    fn insert_should_store_record_and_return_offset() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let expected_record = Record::new(1, &vector, payload);

        //Act
        let result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;

        //Assert
        match result {
            StorageCommandResult::Inserted { offset } => {
                let query_result = storage.perform_query(StorageQuery::Search { offset })?;

                assert_eq!(
                    query_result,
                    StorageQueryResult::FoundRecord {
                        record: expected_record
                    }
                );
            }
            _ => panic!("Expected Inserted result"),
        }
        Ok(())
    }

    #[test]
    fn insert_two_records_should_store_two_records() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        let expected_record = Record::new(1, &vector, payload);
        let expected_record2 = Record::new(2, &vector2, payload2);

        //Act
        let result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let result2 = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector2,
                payload: payload2,
            },
            2,
        )?;

        //Assert
        match (result, result2) {
            (
                StorageCommandResult::Inserted { offset },
                StorageCommandResult::Inserted { offset: offset2 },
            ) => {
                let query_result = storage.perform_query(StorageQuery::Search { offset })?;
                let query_result2 =
                    storage.perform_query(StorageQuery::Search { offset: offset2 })?;

                assert_eq!(
                    query_result,
                    StorageQueryResult::FoundRecord {
                        record: expected_record
                    }
                );
                assert_eq!(
                    query_result2,
                    StorageQueryResult::FoundRecord {
                        record: expected_record2
                    }
                );
            }
            _ => panic!("Expected Inserted result for both commands"),
        }
        Ok(())
    }

    #[test]
    fn inserting_vecs_with_different_dim_amounts_should_return_error() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0, 5.0];
        let payload2 = "test2";

        //Act
        let result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        );
        let result2 = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector2,
                payload: payload2,
            },
            2,
        );

        //Assert
        assert!(result.is_ok());
        assert!(result2.is_err());
        assert_eq!(storage.header.modification_lsn, 1);
        Ok(())
    }

    #[test]
    fn bulk_insert_two_records_should_store_two_record() -> Result<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<f32> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<f32> = vec![2.0, 3.0, 4.0];
        let payload2 = "test2";

        let expected_record = Record::new(1, &vector, payload);
        let expected_record2 = Record::new(1, &vector2, payload2);

        // Act
        let result = storage.perform_command(
            StorageCommand::BulkInsert {
                vectors_and_payloads: &[
                    (vector.as_slice(), payload),
                    (vector2.as_slice(), payload2),
                ],
            },
            1,
        )?;

        // Assert
        match result {
            StorageCommandResult::BulkInserted { offsets } => {
                assert_eq!(offsets.len(), 2);

                let query_result =
                    storage.perform_query(StorageQuery::Search { offset: offsets[0] })?;
                let query_result2 =
                    storage.perform_query(StorageQuery::Search { offset: offsets[1] })?;

                assert_eq!(
                    query_result,
                    StorageQueryResult::FoundRecord {
                        record: expected_record
                    }
                );
                assert_eq!(
                    query_result2,
                    StorageQueryResult::FoundRecord {
                        record: expected_record2
                    }
                );
            }
            _ => panic!("Expected BulkInserted result"),
        }
        Ok(())
    }

    #[test]
    fn bulk_insert_empty_array_should_return_empty_offsets() -> Result<()> {
        // Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;

        // Act
        let result = storage.perform_command(
            StorageCommand::BulkInsert {
                vectors_and_payloads: &[],
            },
            1,
        )?;

        // Assert
        match result {
            StorageCommandResult::BulkInserted { offsets } => {
                assert!(offsets.is_empty());
                assert_eq!(storage.header.modification_lsn, 1);
                Ok(())
            }
            _ => panic!("Expected BulkInserted result"),
        }
    }

    #[test]
    fn search_record_should_return_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let expected_record = Record::new(1, &vector, payload);

        let insert_result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let offset = match insert_result {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let query_result = storage.perform_query(StorageQuery::Search { offset })?;

        //Assert
        assert_eq!(
            query_result,
            StorageQueryResult::FoundRecord {
                record: expected_record
            }
        );
        Ok(())
    }

    #[test]
    fn search_record_should_return_error_when_record_with_given_offset_does_not_exist() -> Result<()>
    {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let record = Record::new(2, &vector, payload);

        let _ = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let insert_result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            2,
        )?;

        let offset = match insert_result {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let invalid_offset = offset - serialized_size(&record)? - 1;
        let result = storage.perform_query(StorageQuery::Search {
            offset: invalid_offset,
        });

        //Assert
        assert!(result.is_err());
        assert_eq!(storage.header.modification_lsn, 2);
        Ok(())
    }

    #[test]
    fn delete_record_should_delete_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let insert_result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let offset = match insert_result {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let delete_result = storage.perform_command(
            StorageCommand::Update {
                offset,
                vector: None,
                payload: None,
            },
            2,
        )?;

        //Assert
        match delete_result {
            StorageCommandResult::Updated { new_offset: _ } => {
                let query_result = storage.perform_query(StorageQuery::Search { offset })?;

                assert_eq!(query_result, StorageQueryResult::NotFound);
                assert_eq!(storage.header.modification_lsn, 2);
            }
            _ => panic!("Expected Updated result"),
        }
        Ok(())
    }

    #[test]
    fn update_record_should_update_record() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let new_vector: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let new_payload = "test2";

        let expected_record = Record::new(2, &new_vector, new_payload);

        let insert_result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let offset = match insert_result {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let update_result = storage.perform_command(
            StorageCommand::Update {
                offset,
                vector: Some(&new_vector),
                payload: Some(new_payload),
            },
            2,
        )?;

        //Assert
        match update_result {
            StorageCommandResult::Updated { new_offset } => {
                let query_result =
                    storage.perform_query(StorageQuery::Search { offset: new_offset })?;

                assert_eq!(
                    query_result,
                    StorageQueryResult::FoundRecord {
                        record: expected_record
                    }
                );
                assert_eq!(storage.header.modification_lsn, 2);
            }
            _ => panic!("Expected Updated result"),
        }
        Ok(())
    }

    #[test]
    fn updating_vec_to_different_dim_amount_should_return_error() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let vector2: Vec<Dim> = vec![2.0, 3.0, 4.0, 5.0];

        let insert_result = storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )?;
        let offset = match insert_result {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let result = storage.perform_command(
            StorageCommand::Update {
                offset,
                vector: Some(&vector2),
                payload: None,
            },
            2,
        );

        //Assert
        assert!(result.is_err());
        assert_eq!(storage.header.modification_lsn, 1);
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
        record.validate_checksum()?;

        //Assert
        Ok(())
    }

    #[test]
    fn rollback_update_should_rollback_update() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let new_vector: Vec<Dim> = vec![2.0, 3.0, 4.0];
        let new_payload = "test2";

        let record_before_update = Record::new(1, &vector, payload);

        let offset = match storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )? {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        match storage.perform_command(
            StorageCommand::Update {
                offset,
                vector: Some(&new_vector),
                payload: Some(new_payload),
            },
            2,
        )? {
            StorageCommandResult::Updated { new_offset: _ } => (),
            _ => panic!("Expected Updated result"),
        };

        //Act
        storage.perform_rollback(3)?;

        //Assert
        let query_result = storage.perform_query(StorageQuery::Search { offset })?;
        assert_eq!(
            query_result,
            StorageQueryResult::FoundRecord {
                record: record_before_update
            }
        );
        Ok(())
    }

    #[test]
    fn rollback_delete_should_rollback_delete() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";

        let record_before_delete = Record::new(1, &vector, payload);

        let offset = match storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            1,
        )? {
            StorageCommandResult::Inserted { offset } => offset,
            _ => panic!("Expected Inserted result"),
        };

        match storage.perform_command(StorageCommand::Delete { offset }, 2)? {
            StorageCommandResult::Deleted => (),
            _ => panic!("Expected Deleted result"),
        };

        //Act
        storage.perform_rollback(3)?;

        //Assert
        let query_result = storage.perform_query(StorageQuery::Search { offset })?;
        assert_eq!(
            query_result,
            StorageQueryResult::FoundRecord {
                record: record_before_delete
            }
        );
        Ok(())
    }

    #[test]
    fn rollback_should_throw_error_when_there_is_no_opeartions_performed() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;

        //Act
        let result = storage.perform_rollback(1);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn rollback_should_throw_error_when_trying_to_rollback_not_last_opeartion() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let mut storage = Storage::create(temp_dir.path(), None)?;
        let vector: Vec<Dim> = vec![1.0, 2.0, 3.0];
        let payload = "test";
        let lsn = 1;

        match storage.perform_command(
            StorageCommand::Insert {
                vector: &vector,
                payload,
            },
            lsn,
        )? {
            StorageCommandResult::Inserted { offset: _ } => (),
            _ => panic!("Expected Inserted result"),
        };

        //Act
        let result = storage.perform_rollback(lsn + 2);

        //Assert
        assert!(matches!(
            result,
            Err(Error::Unexpected("Index: Cannot rollback - LSN mismatch."))
        ));
        Ok(())
    }

    #[test]
    fn get_creation_settings_should_return_creation_settings() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let settings = StorageCreationSettings {
            name: "custom_storage".to_string(),
            modification_lsn: 1,
            vector_dim_amount: 3,
        };

        let storage = Storage::create(temp_dir.path(), Some(settings))?;

        //Act
        let result = storage.get_creation_settings();

        //Assert
        assert_eq!(result.name, "custom_storage");
        assert_eq!(result.modification_lsn, 1);
        assert_eq!(result.vector_dim_amount, 3);
        Ok(())
    }
}
