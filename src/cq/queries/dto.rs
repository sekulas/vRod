use std::fmt;
use std::fmt::Display;
use crate::types::Dim;
use crate::{components::collection::Record, types::RecordId};

pub struct RecordDTO<'a>(pub &'a RecordId, pub &'a Record);

impl<'a> fmt::Display for RecordDTO<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{\n   id: {},\n   embedding: [",
            self.0
        )?;

        for (index, dim) in self.1.vector.iter().enumerate() {
            write!(f, "{}", dim)?;
            if index < self.1.vector.len() - 1 {
                write!(f, ",")?;
            }
        }

        write!(f,"],\n   payload: {}\n}}", self.1.payload)
    }
}

pub struct RecordDTOList(pub Vec<(RecordId, Record)>);

impl fmt::Display for RecordDTOList {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "[")?;
        for (index, (id, record)) in self.0.iter().enumerate() {
            let record_dto = RecordDTO(id, record);
            write!(f, "{}", record_dto)?;
            if index < self.0.len() - 1 {
                writeln!(f, ",")?;
            }
        }
        write!(f, "\n]")
    }
}
