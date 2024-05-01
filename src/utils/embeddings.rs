use crate::utils::{Error, Result};
use fastembed::TextEmbedding;
use std::fs;
use std::io::Write;
use std::mem::size_of_val;

pub fn process_embeddings(number_of_embeddings: usize) -> Result<()> {
    let model = TextEmbedding::try_new(Default::default())?;
    let content = fs::read_to_string("alice_in_wonderland.txt")
        .expect("Something went wrong reading the file");

    println!("Alice acquired.");

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
    model.embed(words.to_vec(), None).map_err(Error::Embedding)
}

fn print_embeddings_info(words: &[&str], embeddings: &[Vec<f32>]) {
    println!("Embeddings length: {}", embeddings.len());
    println!("Embedding dimension: {}", embeddings[0].len());

    let string_data_size: usize = words.iter().map(|s| s.len()).sum();

    println!(
        "Size of the vector of strings: {:.2} MB",
        (size_of_val(&words) + string_data_size) as f64 / 1024.0 / 1024.0
    );

    println!(
        "Size of the vector of embeddings: {:.2} MB",
        (size_of_val(&embeddings) + size_of_val(&embeddings[0]) * embeddings.len()) as f64
            / 1024.0
            / 1024.0
    );
}

fn write_embeddings_to_file(words: &[&str], embeddings: &[Vec<f32>]) -> std::io::Result<()> {
    let mut file = fs::File::create("alice_embeddings.txt")?;

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
