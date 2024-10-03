use std::fmt;

use crate::{components::collection::Record, types::RecordId};

pub struct RecordDTO<'a>(pub &'a RecordId, pub &'a Record);

impl<'a> fmt::Display for RecordDTO<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{\n   id: {},\n   embedding: {:?},\n   payload: {}\n}}",
            self.0, self.1.vector, self.1.payload
        )
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
