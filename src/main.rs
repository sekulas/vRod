mod utils;
use utils::embeddings::process_embeddings;

fn main() -> anyhow::Result<()> {
    process_embeddings(5)?;

    Ok(())
}
