use std::{
    fs::{self, File},
    io::{BufReader, Seek, SeekFrom, Write},
    mem,
    path::{Path, PathBuf},
};

use bincode::deserialize_from;

use super::{WALEntry, WALHeader};
use serde_json;
use std::io::Error;

pub fn wal_to_txt(mut path: PathBuf) -> Result<(), Error> {
    let mut wal = File::open(&path)?;

    path.set_extension("json");
    if path.exists() {
        fs::remove_file(&path)?;
    }

    let mut wal_txt = File::create(path.with_extension("json"))?;

    let _ = match deserialize_from::<_, WALHeader>(&mut BufReader::new(&wal)) {
        Ok(header) => {
            let header_json = serde_json::to_string_pretty(&header)?;
            writeln!(wal_txt, "[\n{}", header_json)?;
            header
        }
        Err(_) => {
            println!("Error: Failed to deserialize WAL header in wal_to_txt util.");
            return Ok(());
        }
    };

    wal.seek(SeekFrom::Start(mem::size_of::<WALHeader>() as u64))?;

    while let Ok(entry) = deserialize_from::<_, WALEntry>(&mut BufReader::new(&wal)) {
        write!(wal_txt, ",")?;
        let entry_json = serde_json::to_string_pretty(&entry)?;
        writeln!(wal_txt, "{}", entry_json)?;
    }

    writeln!(wal_txt, "]")?;

    Ok(())
}
