use crate::types::{Dim, RecordId};

use super::{Error, Result};
use std::{
    fs::File,
    io::{BufRead, BufReader},
    num::ParseFloatError,
    path::Path,
};

pub const EXPECTED_2_ARG_FORMAT_ERR_M: &str = "Expected format: <vector>;<payload>";
pub const NO_EMBEDDING_PROVIDED_ERR_M: &str =
    "No embedding provided. Expected format: <embedding>;<payload>";
pub const NO_PAYLOAD_PROVIDED_ERR_M: &str =
    "No payload provided. Expected format: <embedding>;<payload>";
pub const EXPECTED_3_ARG_FORMAT_ERR_M: &str = "Expected format: <record_id>;[vector];[payload]";
pub const NO_RECORD_ID_PROVIDED_ERR_M: &str =
    "No record id provided. Expected format: <record_id>;[vector];[payload]";
pub const DIFFERENT_DIMENSIONS_ERR_M: &str = "All vectors must have the same number of dimensions.";

pub fn parse_vec_n_payload(data: &str) -> Result<(Vec<f32>, String)> {
    let splitted_data = data.split(';').collect::<Vec<&str>>();

    if splitted_data.len() != 2 {
        return Err(Error::InvalidDataFormat {
            description: EXPECTED_2_ARG_FORMAT_ERR_M.to_owned(),
        });
    }

    if splitted_data[0].is_empty() {
        return Err(Error::InvalidDataFormat {
            description: NO_EMBEDDING_PROVIDED_ERR_M.to_owned(),
        });
    }

    if splitted_data[1].is_empty() {
        return Err(Error::InvalidDataFormat {
            description: NO_PAYLOAD_PROVIDED_ERR_M.to_owned(),
        });
    }

    let vector = parse_vector(splitted_data[0])?;

    Ok((vector, splitted_data[1].to_string()))
}

pub fn parse_vector(data: &str) -> std::result::Result<Vec<Dim>, ParseFloatError> {
    let data = data.replace("\"", "");
    data.split(',').map(|s| s.trim().parse::<Dim>()).collect()
}

pub fn parse_vecs_and_payloads_from_file(file_path: &Path) -> Result<Vec<(Vec<Dim>, String)>> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut vecs_and_payloads = Vec::new();

    for line in reader.lines() {
        let line = line?;
        let (vector, payload) = parse_vec_n_payload(&line)?;
        vecs_and_payloads.push((vector, payload));
    }

    validate_vecs_and_payloads(&vecs_and_payloads)?;

    Ok(vecs_and_payloads)
}

pub fn parse_vecs_and_payloads_from_string(data: &str) -> Result<Vec<(Vec<Dim>, String)>> {
    let vecs_and_payloads: Vec<(Vec<Dim>, String)> = data
        .split_whitespace()
        .map(parse_vec_n_payload)
        .collect::<Result<Vec<(Vec<Dim>, String)>>>()?;

    validate_vecs_and_payloads(&vecs_and_payloads)?;

    Ok(vecs_and_payloads)
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
    let splitted_data = data.split(';').collect::<Vec<&str>>();

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
        Some(parse_vector(splitted_data[1])?)
    };

    let payload = if splitted_data[2].is_empty() {
        None
    } else {
        Some(splitted_data[2].to_string())
    };

    Ok((record_id, vector, payload))
}

fn validate_vecs_and_payloads(vecs_and_payloads: &[(Vec<Dim>, String)]) -> Result<()> {
    if vecs_and_payloads.is_empty() {
        return Err(Error::NoDataInSource);
    }

    let first_vec_len = vecs_and_payloads[0].0.len();

    for (vec, _) in vecs_and_payloads.iter() {
        if vec.len() != first_vec_len {
            return Err(Error::InvalidDataFormat {
                description: DIFFERENT_DIMENSIONS_ERR_M.to_owned(),
            });
        }
    }

    Ok(())
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
        let (vector, payload) = parse_vec_n_payload(&data)?;

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
        let result = parse_vec_n_payload(&data);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_payload() -> Result<()> {
        //Arrange
        let data = "1.0,2.0,3.0;".to_string();

        //Act
        let result = parse_vec_n_payload(&data);

        //Assert
        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_data_provided() -> Result<()> {
        //Arrange
        let data = ";".to_string();

        //Act
        let result = parse_vec_n_payload(&data);

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
        file.write_all(b"1.0,2.0,3.0;payload\n4.0,5.0,6.0;another_payload")?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path)?;

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
    fn parse_vecs_and_payloads_from_file_should_return_error_if_no_data_in_file() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let file_path = temp_dir.path().join("test.txt");
        File::create(&file_path)?;

        //Act
        let result = parse_vecs_and_payloads_from_file(&file_path);

        //Assert
        assert!(matches!(result, Err(Error::NoDataInSource)));
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
    fn parse_vecs_and_payloads_from_string_should_return_error_for_vecs_with_different_dims(
    ) -> Result<()> {
        //Arrange
        let data = "1.0,2.0,3.0;payload 4.0,5.0,6.0,7.0;another_payload";

        //Act
        let result = parse_vecs_and_payloads_from_string(data);

        //Assert
        assert!(matches!(result, Err(Error::InvalidDataFormat { .. })));
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
}
