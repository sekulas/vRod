use std::{
    fs::{self, File},
    io::{BufReader, Seek, SeekFrom, Write},
    mem,
    path::Path,
};

use bincode::deserialize_from;

use super::{WalEntry, WalHeader};
use serde_json;
use std::io::Error;

pub fn wal_to_txt(path: &Path) -> Result<(), Error> {
    let mut wal = File::open(path)?;

    let mut wal_json = path.to_owned();
    wal_json.set_extension("json");
    if wal_json.exists() {
        fs::remove_file(&wal_json)?;
    }

    let mut wal_txt = File::create(wal_json)?;

    let _ = match deserialize_from::<_, WalHeader>(&mut BufReader::new(&wal)) {
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

    wal.seek(SeekFrom::Start(mem::size_of::<WalHeader>() as u64))?;

    while let Ok(entry) = deserialize_from::<_, WalEntry>(&mut BufReader::new(&wal)) {
        write!(wal_txt, ",")?;
        let entry_json = serde_json::to_string_pretty(&entry)?;
        writeln!(wal_txt, "{}", entry_json)?;
    }

    writeln!(wal_txt, "]")?;

    Ok(())
}
