use bincode::{deserialize_from, serialize_into, serialized_size};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Seek, SeekFrom, Write};

const HEADER_SIZE: u64 = 16;

#[derive(Serialize, Deserialize)]
struct WALEntry {
    lsn: u64,
    committed: bool,
    data_len: u16,
    data: String,
}

#[derive(Serialize, Deserialize)]
struct WALHeader {
    most_recent_offset: u64,
    lsn: u64,
}

pub struct WAL {
    file: File,
    header: WALHeader,
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
                let header = WALHeader {
                    most_recent_offset: HEADER_SIZE,
                    lsn: 0,
                }; // TODO Recreate Existing FIle??
                file.seek(SeekFrom::Start(0))?;
                let mut writer = BufWriter::new(&file);
                serialize_into(&mut writer, &header)?;
                writer.flush()?;
                header
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
        self.header.most_recent_offset = self.file.stream_position()?;

        serialize_into(&mut BufWriter::new(&self.file), &entry)?;

        self.update_header()?;
        Ok(())
    }

    fn commit(&mut self, lsn: u64) -> Result<(), bincode::Error> {
        self.file
            .seek(SeekFrom::Start(self.header.most_recent_offset))?;

        let mut entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        entry.committed = true;

        if entry.lsn == lsn {
            self.file
                .seek(SeekFrom::Start(self.header.most_recent_offset))?;
            let mut writer = BufWriter::new(&self.file); // Declare BufWriter
            serialize_into(&mut writer, &entry)?; // Use BufWriter for serialization
            writer.flush()?; // Flush changes to the file
        }
        Ok(())
    }

    fn get_most_recent(&mut self) -> Result<Option<WALEntry>, bincode::Error> {
        let file_size = self.file.metadata()?.len();

        if file_size <= HEADER_SIZE {
            return Ok(None);
        }

        self.file
            .seek(SeekFrom::Start(self.header.most_recent_offset))?;

        let entry: WALEntry = deserialize_from(&mut BufReader::new(&self.file))?;
        Ok(Some(entry))
    }

    fn update_header(&mut self) -> Result<(), bincode::Error> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut writer = BufWriter::new(&self.file);
        serialize_into(&mut writer, &self.header)?;
        writer.write_all(&[0])?;
        writer.flush()?;
        Ok(())
    }
}

mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_append_and_commit() -> Result<(), bincode::Error> {
        let mut wal = WAL::load("test.wal")?;

        let data = "Hello, World!".to_string();
        wal.append(data.clone())?;

        let entry = wal.get_most_recent().unwrap().unwrap();

        assert_eq!(entry.data, data);

        assert!(!entry.committed);

        wal.commit(entry.lsn)?;

        let entry = wal.get_most_recent().unwrap().unwrap();

        assert!(entry.committed);

        fs::remove_file("test.wal").unwrap();

        Ok(())
    }

    #[test]
    fn test_update_header() -> Result<(), bincode::Error> {
        let mut wal = WAL::load("test.wal")?;

        wal.header.lsn = 10;
        wal.update_header()?;

        let wal = WAL::load("test.wal")?;

        assert_eq!(wal.header.lsn, 10);

        Ok(())
    }
}
