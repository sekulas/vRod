use super::{
    types::{EMPTY_CHILD_SLOT, EMPTY_KEY_SLOT, M},
    Error, Result,
};
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};

use crate::{
    components::collection::types::NONE,
    types::{Offset, RecordId, INDEX_FILE},
};

use std::{
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::Path,
};

#[derive(Serialize, Deserialize)]
struct BPTreeHeader {
    current_max_id: RecordId,
    checksum: u64,
    root_offset: Offset,
}

impl BPTreeHeader {
    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    fn define_header(file: &mut File) -> Result<Self> {
        //TODO: Find MAX_ID in tree
        let mut header = BPTreeHeader::default();
        let checksum = header.calculate_checksum();
        header.checksum = checksum;

        file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(file), &header)?;

        Ok(header)
    }
}

impl Hash for BPTreeHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.current_max_id.hash(state);
        self.checksum.hash(state);
        self.root_offset.hash(state);
    }
}

impl Default for BPTreeHeader {
    fn default() -> Self {
        let root_offset = mem::size_of::<BPTreeHeader>() as Offset;

        let mut tree_header = Self {
            current_max_id: 0,
            checksum: 0,
            root_offset,
        };

        tree_header.checksum = tree_header.calculate_checksum();

        tree_header
    }
}

pub struct BPTree {
    header: BPTreeHeader,
    file: File,
}

#[derive(Serialize, Deserialize)]
pub struct Node {
    checksum: u64,
    is_leaf: bool,
    parent: Offset,
    keys: Vec<RecordId>,
    values: Vec<Offset>,
    next_leaf: Option<Offset>,
}

impl Node {
    fn new_internal(parent: Offset) -> Self {
        Self {
            checksum: 0,
            is_leaf: false,
            parent,
            keys: vec![EMPTY_KEY_SLOT; M + 1],
            values: vec![EMPTY_CHILD_SLOT; M],
            next_leaf: None,
        }
    }
    fn new_leaf(parent: Offset) -> Self {
        Self {
            checksum: 0,
            is_leaf: true,
            parent,
            keys: vec![EMPTY_KEY_SLOT; M],
            values: vec![EMPTY_CHILD_SLOT; M],
            next_leaf: None,
        }
    }

    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.is_leaf.hash(state);
        self.parent.hash(state);
        self.keys.hash(state);
        self.values.hash(state);
        self.next_leaf.hash(state);
    }
}

impl BPTree {
    //TODO: Is that good to skip ID_OFFSET_STORAGE and work only with index
    pub fn create(path: &Path) -> Result<Self> {
        let file_path = path.join(INDEX_FILE);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        let header = BPTreeHeader::default();

        let mut tree = Self { header, file };

        tree.create_root()?;
        tree.update_header()?;

        Ok(tree)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        let header: BPTreeHeader =
            match deserialize_from::<_, BPTreeHeader>(&mut BufReader::new(&file)) {
                Ok(header) => {
                    if header.checksum != header.calculate_checksum() {
                        println!("Checksum incorrect for 'B+Tree' header - defining header.");
                        BPTreeHeader::define_header(&mut file)?;
                    }

                    header
                }
                Err(_) => {
                    println!("Cannot deserialize header for the 'B+Tree' - defining header.");
                    BPTreeHeader::define_header(&mut file)?
                }
            };

        let tree = Self { header, file };

        Ok(tree)
    }

    fn update_header(&mut self) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        self.header.checksum = self.header.calculate_checksum();
        serialize_into(&mut BufWriter::new(&self.file), &self.header)?;

        self.file.sync_all()?;
        Ok(())
    }

    fn write_node(&mut self, node: &Node, offset: Offset) -> Result<()> {
        self.file.seek(SeekFrom::Start(offset))?;
        serialize_into(&mut BufWriter::new(&self.file), node)?;

        //TODO: file.sync_all()?
        Ok(())
    }

    fn read_node(&mut self, offset: Offset) -> Result<Node> {
        self.file.seek(SeekFrom::Start(offset))?;
        //TODO: BufReader with specified size
        let node: Node = deserialize_from(&mut BufReader::new(&self.file))?;

        match node.checksum == node.calculate_checksum() {
            true => Ok(node),
            false => Err(Error::IncorrectChecksum { offset }),
        }
    }

    fn create_root(&mut self) -> Result<()> {
        let mut root = Node::new_internal(NONE);
        root.checksum = root.calculate_checksum();

        self.write_node(&root, self.header.root_offset)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    type Result<T> = core::result::Result<T, Box<dyn std::error::Error>>;

    #[test]
    fn bptree_create_should_create_root() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();

        //Act
        let mut tree = BPTree::create(path)?;
        let root = tree.read_node(tree.header.root_offset)?;

        //Assert
        assert_eq!(tree.header.current_max_id, 0);
        assert_eq!(
            tree.header.root_offset,
            mem::size_of::<BPTreeHeader>() as Offset
        );
        assert!(!root.is_leaf);
        assert_eq!(root.parent, NONE);
        assert_eq!(root.keys, vec![EMPTY_KEY_SLOT; M + 1]);
        assert_eq!(root.values, vec![EMPTY_CHILD_SLOT; M]);
        assert_eq!(root.next_leaf, None);

        Ok(())
    }
}
