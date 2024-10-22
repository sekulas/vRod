use crate::utils::{Error, Result};
use fastembed::{EmbeddingModel, InitOptions, TextEmbedding};
use std::fs;
use std::io::Write;
use std::mem::size_of;
use std::path::PathBuf;

pub fn process_embeddings(number_of_embeddings: usize, file: PathBuf) -> Result<()> {
    let options = InitOptions {
        model_name: EmbeddingModel::AllMiniLML6V2,
        ..Default::default()
    };
    let model = TextEmbedding::try_new(options)?;

    let content = fs::read_to_string(file).expect("Something went wrong reading the file");

    println!("File content acquired.");

    let words = extract_words(&content, number_of_embeddings);
    let embeddings = generate_embeddings(&model, &words)?;

    print_embeddings_info(&words, &embeddings);
    write_embeddings_to_file(&words, &embeddings)?;

    Ok(())
}

fn extract_words(content: &str, number_of_embeddings: usize) -> Vec<&str> {
    content
        .split_whitespace()
        .take(number_of_embeddings)
        .collect::<Vec<&str>>()
}

fn generate_embeddings(model: &TextEmbedding, words: &[&str]) -> Result<Vec<Vec<f32>>> {
    model
        .embed(words.to_vec(), Some(4))
        .map_err(Error::Embedding)
}

fn print_embeddings_info(words: &[&str], embeddings: &[Vec<f32>]) {
    println!("Embeddings length: {}", embeddings.len());
    println!("Embedding dimension: {}", embeddings[0].len());

    let string_data_size: usize = words.iter().map(|s| s.len() * size_of::<char>()).sum();

    let embeddings_data_size: usize = embeddings.iter().map(|e| e.len() * size_of::<f32>()).sum();

    println!(
        "Size of the vector of strings: {:.2} MB",
        string_data_size as f64 / 1024.0 / 1024.0
    );

    println!(
        "Size of the vector of embeddings: {:.2} MB",
        embeddings_data_size as f64 / 1024.0 / 1024.0
    );
}

fn write_embeddings_to_file(words: &[&str], embeddings: &[Vec<f32>]) -> std::io::Result<()> {
    let mut file = fs::File::create("embeddings.txt")?;

    for (word, embedding) in words.iter().zip(embeddings.iter()) {
        let embedding_str = embedding
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<String>>()
            .join(",");
        file.write_all(format!("{};{}\n", embedding_str, word).as_bytes())?;
    }

    let metadata = fs::metadata("alice_embeddings.txt")?;
    println!(
        "Size of the embeddings file: {:.2} MB",
        metadata.len() as f64 / 1024.0 / 1024.0
    );

    Ok(())
}
