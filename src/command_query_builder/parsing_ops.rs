use crate::types::{Dim, RecordId};

use super::{Error, Result};
use std::num::ParseFloatError;

pub const EXPECTED_2_ARG_FORMAT_ERR_M: &str = "Expected format: <vector>;<payload>";
pub const NO_EMBEDDING_PROVIDED_ERR_M: &str =
    "No embedding provided. Expected format: <embedding>;<payload>";
pub const NO_PAYLOAD_PROVIDED_ERR_M: &str =
    "No payload provided. Expected format: <embedding>;<payload>";
pub const EXPECTED_3_ARG_FORMAT_ERR_M: &str = "Expected format: <record_id>;[vector];[payload]";
pub const NO_RECORD_ID_PROVIDED_ERR_M: &str =
    "No record id provided. Expected format: <record_id>;[vector];[payload]";

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

fn parse_vector(data: &str) -> std::result::Result<Vec<Dim>, ParseFloatError> {
    data.split(',').map(|s| s.trim().parse::<Dim>()).collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn parse_vec_n_payload_should_return_vector_and_payload() -> Result<()> {
        let data = "1.0,2.0,3.0;payload".to_string();
        let (vector, payload) = parse_vec_n_payload(&data)?;

        assert_eq!(vector, vec![1.0, 2.0, 3.0]);
        assert_eq!(payload, "payload");

        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_vec() -> Result<()> {
        let data = ";payload".to_string();
        let result = parse_vec_n_payload(&data);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_payload() -> Result<()> {
        let data = "1.0,2.0,3.0;".to_string();
        let result = parse_vec_n_payload(&data);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_vec_n_payload_should_return_err_when_no_data_provided() -> Result<()> {
        let data = ";".to_string();
        let result = parse_vec_n_payload(&data);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_record_id_vector_and_payload() -> Result<()>
    {
        let data = "1;1.0,2.0,3.0;payload".to_string();
        let (record_id, vector, payload) = parse_id_and_optional_vec_payload(&data)?;

        assert_eq!(record_id, 1);
        assert_eq!(vector, Some(vec![1.0, 2.0, 3.0]));
        assert_eq!(payload, Some("payload".to_string()));

        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_err_when_no_record_id() -> Result<()> {
        let data = ";1.0,2.0,3.0;payload".to_string();
        let result = parse_id_and_optional_vec_payload(&data);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_passed_2_args() -> Result<()> {
        let data = "1;;payload".to_string();
        let (result_id, result_vec, result_payload) = parse_id_and_optional_vec_payload(&data)?;

        assert_eq!(result_id, 1);
        assert_eq!(result_vec, None);
        assert_eq!(result_payload, Some("payload".to_string()));
        Ok(())
    }

    #[test]
    fn parse_id_and_optional_vec_payload_should_return_err_when_no_data_provided() -> Result<()> {
        let data = ";".to_string();
        let result = parse_id_and_optional_vec_payload(&data);

        assert!(result.is_err());
        Ok(())
    }

    #[test]
    fn parse_string_from_vector_option_should_return_string() {
        let data: Option<&[Dim]> = Some(&[1.0, 2.0, 3.0]);
        let result = parse_string_from_vector_option(data);

        assert_eq!(result, "1,2,3".to_string());
    }

    #[test]
    fn parse_string_from_vector_option_should_return_empty_string() {
        let data = None;
        let result = parse_string_from_vector_option(data);

        assert_eq!(result, "".to_string());
    }
}
