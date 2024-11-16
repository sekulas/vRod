use hnsw::types::Distance;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

use crate::types::{Dim, RecordId};

use super::{Error, Result};
use memchr;
use std::{
    fs::File,
    io::{BufRead, BufReader},
    num::NonZeroUsize,
    path::Path,
};

const ENTRIES_SEPARATOR: char = ' ';
const VECTOR_DELIMITER: char = ',';
const VECTOR_PAYLOAD_DELIMITER: char = ';';
const IN_ENTRY_DELIMETER: char = ';';

pub const INVALID_VECTOR_LEN_FROM_FILE_ERR_M: &str =
    "invalid vector length in first line of the file - has to be non 0 usize.";
pub const EXPECTED_2_ARG_FORMAT_ERR_M: &str = "expected format: <vector>;<payload>";
pub const NO_EMBEDDING_PROVIDED_ERR_M: &str =
    "no embedding provided. Expected format: <embedding>;<payload>";
pub const NO_PAYLOAD_PROVIDED_ERR_M: &str =
    "no payload provided. Expected format: <embedding>;<payload>";
pub const EXPECTED_3_ARG_FORMAT_ERR_M: &str = "Expected format: <record_id>;[vector];[payload]";
pub const NO_RECORD_ID_PROVIDED_ERR_M: &str =
    "no record id provided. Expected format: <record_id>;[vector];[payload]";
pub const EXPECTED_DISTANCE_AND_VECTORS_ERR_M: &str =
    "expected format: <distance> [vector] [vector] ...";
pub const INVALID_DISTANCE_METRIC_ERR_M: &str =
    "invalid distance metric - expected EUCLID, MANHATTAN, DOT or COSINE";
pub const CANNOT_PARSE_FLOAT_ERR_M: &str = "Cannot parse float from given vector.";

struct FileMetadata {
    vector_len: NonZeroUsize,
}

pub fn parse_vecs_and_payloads_from_file(file_path: &Path) -> Result<Vec<(Vec<Dim>, String)>> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    let metadata = read_metadata(&mut reader)?;

    let lines: Vec<String> = reader.lines().collect::<std::result::Result<_, _>>()?;

    if lines.is_empty() {
        return Err(Error::NoDataInSource);
    }

    let vecs_and_payloads: Result<Vec<(Vec<Dim>, String)>> = lines
        .par_iter()
        .map(|line| {
            parse_vec_n_payload(line, Some(metadata.vector_len))
                .map(|(vec, payload)| (vec, payload.to_string()))
        })
        .collect();

    let vecs_and_payloads = vecs_and_payloads?;

    if vecs_and_payloads.is_empty() {
        return Err(Error::NoDataInSource);
    }

    Ok(vecs_and_payloads)
}

fn read_metadata(reader: &mut BufReader<File>) -> Result<FileMetadata> {
    let mut vector_len_str = String::with_capacity(13);
    reader.read_line(&mut vector_len_str)?;

    let vector_len =
        vector_len_str
            .trim()
            .parse::<NonZeroUsize>()
            .map_err(|_| Error::InvalidDataFormat {
                description: INVALID_VECTOR_LEN_FROM_FILE_ERR_M.to_owned(),
            })?;

    Ok(FileMetadata { vector_len })
}

pub fn parse_vec_n_payload(
    line: &str,
    vector_len: Option<NonZeroUsize>,
) -> Result<(Vec<Dim>, &str)> {
    let (vector_str, payload) =
        line.split_once(VECTOR_PAYLOAD_DELIMITER)
            .ok_or_else(|| Error::InvalidDataFormat {
                description: EXPECTED_2_ARG_FORMAT_ERR_M.to_owned(),
            })?;

    if vector_str.is_empty() {
        return Err(Error::InvalidDataFormat {
            description: NO_EMBEDDING_PROVIDED_ERR_M.to_owned(),
        });
    }

    if payload.is_empty() {
        return Err(Error::InvalidDataFormat {
            description: NO_PAYLOAD_PROVIDED_ERR_M.to_owned(),
        });
    }

    let vector = parse_vector(vector_str, vector_len)?;
    Ok((vector, payload.trim())) //TODO: Check before trim.
}

#[inline(always)]
fn parse_vector(vector_str: &str, vector_len: Option<NonZeroUsize>) -> Result<Vec<Dim>> {
    let mut start = 0;
    let bytes = vector_str.as_bytes();

    let mut buffer;
    if let Some(vector_len) = vector_len {
        buffer = Vec::with_capacity(usize::from(vector_len));
    } else {
        buffer = Vec::new();
    }

    // Use SIMD-optimized memchr for delimiter search
    while let Some(pos) = memchr::memchr(VECTOR_DELIMITER as u8, &bytes[start..]) {
        let end = start + pos;
        if end > start {
            let num_str = std::str::from_utf8(&bytes[start..end])?;
            match num_str.parse() {
                Ok(num) => buffer.push(num),
                Err(_) => {
                    return Err(Error::InvalidDataFormat {
                        description: CANNOT_PARSE_FLOAT_ERR_M.to_owned(),
                    })
                }
            }
        }
        start = end + 1;
    }

    if start < bytes.len() {
        let num_str = std::str::from_utf8(&bytes[start..])?;
        match num_str.parse() {
            Ok(num) => buffer.push(num),
            Err(_) => {
                return Err(Error::InvalidDataFormat {
                    description: CANNOT_PARSE_FLOAT_ERR_M.to_owned(),
                })
            }
        }
    }

    Ok(buffer)
}

//TODO: To optimize?
pub fn parse_vecs_and_payloads_from_string(data: &str) -> Result<Vec<(Vec<Dim>, String)>> {
    if data.is_empty() {
        return Err(Error::NoDataInSource);
    }

    let vecs_and_payloads: Result<Vec<(Vec<Dim>, String)>> = data
        .split_whitespace()
        .map(|s| parse_vec_n_payload(s, None).map(|(vec, payload)| (vec, payload.to_string())))
        .collect();

    vecs_and_payloads
}

pub fn parse_string_from_vector_option(data: Option<&[Dim]>) -> String {
    data.map(|v| {
        v.iter()
            .map(|d| d.to_string())
            .collect::<Vec<String>>()
            .join(",")
    })
    .unwrap_or_default()
}

pub fn parse_id_and_optional_vec_payload(
    data: &str,
) -> Result<(RecordId, Option<Vec<Dim>>, Option<String>)> {
    let splitted_data = data.split(IN_ENTRY_DELIMETER).collect::<Vec<&str>>();

    if splitted_data.len() != 3 {
        return Err(Error::InvalidDataFormat {
            description: EXPECTED_3_ARG_FORMAT_ERR_M.to_owned(),
        });
    }

    if splitted_data[0].is_empty() {
        return Err(Error::InvalidDataFormat {
            description: NO_RECORD_ID_PROVIDED_ERR_M.to_owned(),
        });
    }

    let record_id = splitted_data[0].parse()?;

    let vector = if splitted_data[1].is_empty() {
        None
    } else {
        Some(parse_vector(splitted_data[1], None)?)
    };

    let payload = if splitted_data[2].is_empty() {
        None
    } else {
        Some(splitted_data[2].to_string())
    };

    Ok((record_id, vector, payload))
}

pub fn parse_distance(data: &str) -> Result<Distance> {
    match data.to_uppercase().as_str() {
        "EUCLID" => Ok(Distance::Euclid),
        "MANHATTAN" => Ok(Distance::Manhattan),
        "DOT" => Ok(Distance::Dot),
        "COSINE" => Ok(Distance::Cosine),
        _ => Err(Error::InvalidDataFormat {
            description: INVALID_DISTANCE_METRIC_ERR_M.to_owned(),
        }),
    }
}

pub fn parse_distance_and_vecs(data: &str) -> Result<(Distance, Vec<Vec<Dim>>)> {
    let splitted_data = data.split(ENTRIES_SEPARATOR).collect::<Vec<&str>>();

    if splitted_data.len() < 2 {
        return Err(Error::InvalidDataFormat {
            description: EXPECTED_DISTANCE_AND_VECTORS_ERR_M.to_owned(),
        });
    }

    let distance = parse_distance(splitted_data[0])?;

    let vectors = splitted_data[1..]
        .iter()
        .map(|s| parse_vector(s, None).map_err(Error::from))
        .collect::<Result<Vec<Vec<Dim>>>>()?;

    Ok((distance, vectors))
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;
    #[test]
    fn parse_vec_n_payload_should_return_vector_and_payload() -> Result<()> {
        //Arrange
        let data = "1.0,2.0,3.0;payload".to_string();

        //Act
        let (vector, payload) = parse_vec_n_payload(&data, None)?;

        //Assert
        assert_eq!(vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(payload, "payload");

        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_vec() -> Result<()> {
        //Arrange
        let data = ";payload".to_string();

        //Act
        let result = parse_vec_n_payload(&data, None);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_payload() -> Result<()> {
        //Arrange
        let data = "1.0,2.0,3.0;".to_string();

        //Act
        let result = parse_vec_n_payload(&data, None);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_data_provided() -> Result<()> {
        //Arrange
        let data = ";".to_string();

        //Act
        let result = parse_vec_n_payload(&data, None);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_file_should_return_vec_of_vecs_and_payloads() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path)?;
        file.write_all(b"3\n1.0,2.0,3.35;payload\n4.0,5.0,6.0;another_payload")?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path)?;

        //Assert
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (vec![1.0, 2.0, 3.35], "payload".to_string()));
        assert_eq!(
            result[1],
            (vec![4.0, 5.0, 6.0], "another_payload".to_string())
        );

        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_file_should_return_err_when_file_does_not_exist() -> Result<()>
    {
        //Arrange
        let file_path = Path::new("non_existent_file.txt");

        //Act
        let result = parse_vecs_and_payloads_from_file(file_path);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_file_should_return_error_if_invalid_vec_len_in_file(
    ) -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.txt");
        File::create(&file_path)?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path);

        //Assert
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error
            .to_string()
            .contains(INVALID_VECTOR_LEN_FROM_FILE_ERR_M));
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_file_should_return_error_for_vecs() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path)?;
        file.write_all(b"3\n\n\n\n")?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path);

        //Assert
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains(EXPECTED_2_ARG_FORMAT_ERR_M));
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_file_should_return_error_for_vecs_with_different_dims(
    ) -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.txt");
        let mut file = File::create(&file_path)?;
        file.write_all(b"1.0,2.0,3.0;payload\n4.0,5.0,6.0,7.0;another_payload")?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path);

        //Assert
        assert!(matches!(result, Err(Error::InvalidDataFormat { .. })));
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_string_should_return_vec_of_vecs_and_payloads() -> Result<()> {
        //Arrange
        let data = "1.0,2.0,3.0;payload 4.0,5.0,6.0;another_payload";

        //Act
        let result = parse_vecs_and_payloads_from_string(data)?;

        //Assert
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (vec![1.0, 2.0, 3.0], "payload".to_string()));
        assert_eq!(
            result[1],
            (vec![4.0, 5.0, 6.0], "another_payload".to_string())
        );

        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_string_should_return_error_for_no_data() -> Result<()> {
        //Arrange
        let data = "";

        //Act
        let result = parse_vecs_and_payloads_from_string(data);

        //Assert
        assert!(matches!(result, Err(Error::NoDataInSource)));
        Ok(())
    }

    #[test]
    fn parse_vecs_and_payloads_from_string_should_return_error_invalid_data() -> Result<()> {
        //Arrange
        let data = "s s";

        //Act
        let result = parse_vecs_and_payloads_from_string(data);

        //Assert
        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains(EXPECTED_2_ARG_FORMAT_ERR_M));
        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_record_id_vector_and_payload() -> Result<()>
    {
        //Arrange
        let data = "1;1.0,2.0,3.0;payload".to_string();

        //Act
        let (record_id, vector, payload) = parse_id_and_optional_vec_payload(&data)?;

        //Assert
        assert_eq!(record_id, 1);
        assert_eq!(vector, Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(payload, Some("payload".to_string()));

        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_err_when_no_record_id() -> Result<()> {
        //Arrange
        let data = ";1.0,2.0,3.0;payload".to_string();

        //Act
        let result = parse_id_and_optional_vec_payload(&data);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_passed_2_args() -> Result<()> {
        //Arrange
        let data = "1;;payload".to_string();

        //Act
        let (result_id, result_vec, result_payload) = parse_id_and_optional_vec_payload(&data)?;

        //Assert
        assert_eq!(result_id, 1);
        assert_eq!(result_vec, None);
        assert_eq!(result_payload, Some("payload".to_string()));

        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_err_when_no_data_provided() -> Result<()> {
        //Arrange
        let data = ";".to_string();

        //Act
        let result = parse_id_and_optional_vec_payload(&data);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_string_from_vector_option_should_return_string() {
        //Arrange
        let data: Option<&[Dim]> = Some(&[1.0, 2.0, 3.0]);

        //Act
        let result = parse_string_from_vector_option(data);

        //Assert
        assert_eq!(result, "1,2,3".to_string());
    }

    #[test]
    fn parse_string_from_vector_option_should_return_empty_string() {
        //Arrange
        let data = None;

        //Act
        let result = parse_string_from_vector_option(data);

        //Assert
        assert_eq!(result, "".to_string());
    }

    #[test]
    fn parse_distance_and_vecs_should_return_distance_and_vecs() -> Result<()> {
        //Arrange
        let data = "EUCLID 1.0,2,-3 -4,5.0,6.0";

        //Act
        let (distance, vectors) = parse_distance_and_vecs(data)?;

        //Assert
        assert_eq!(distance, Distance::Euclid);
        assert_eq!(vectors.len(), 2);
        assert_eq!(vectors[0], vec![1.0, 2.0, -3.0]);
        assert_eq!(vectors[1], vec![-4.0, 5.0, 6.0]);

        Ok(())
    }
}
