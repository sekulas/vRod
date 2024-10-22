use super::Result;
use std::{
    fs::File,
    io::{BufReader, BufWriter},
    path::Path,
};

use serde::{de::DeserializeOwned, Serialize};

pub fn read_json<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(serde_json::from_reader(BufReader::new(File::open(path)?))?)
}

pub fn read_bin<T: DeserializeOwned>(path: &Path) -> Result<T> {
    Ok(bincode::deserialize_from(BufReader::new(File::open(
        path,
    )?))?)
}

pub fn save_json<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    let file = File::create(path)?;
    let writer = BufWriter::new(file);

    serde_json::to_writer_pretty(writer, object)?;

    Ok(())
}

pub fn save_bin<T: Serialize>(path: &Path, object: &T) -> Result<()> {
    Ok(bincode::serialize_into(
        BufWriter::new(File::create(path)?),
        object,
    )?)
}
