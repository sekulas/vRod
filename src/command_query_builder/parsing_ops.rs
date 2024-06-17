use crate::types::Dim;

use super::{Error, Result};
use std::num::ParseFloatError;

pub fn parse_vec_n_payload(data: &str) -> Result<(Vec<f32>, String)> {
    let splitted_data = data.split(';').collect::<Vec<&str>>();

    if splitted_data.len() != 2 {
        return Err(Error::InvalidDataFormat {
            data: data.to_string(),
        });
    }

    if splitted_data[0].is_empty() {
        return Err(Error::NoVector {
            data: data.to_string(),
        });
    }

    if splitted_data[1].is_empty() {
        return Err(Error::NoPayload {
            data: data.to_string(),
        });
    }

    let vector = parse_vector(splitted_data[0])?;

    Ok((vector, splitted_data[1].to_string()))
}

fn parse_vector(data: &str) -> std::result::Result<Vec<Dim>, ParseFloatError> {
    data.split(',').map(|s| s.trim().parse::<Dim>()).collect()
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
}
