use super::{
    types::{EMPTY_CHILD_SLOT, EMPTY_KEY_SLOT, M, MAX_KEYS, SERIALIZED_NODE_SIZE},
    Error, Result,
};
use bincode::{deserialize_from, serialize_into};
use serde::{Deserialize, Serialize};

use crate::{
    components::collection::types::NONE,
    types::{Offset, RecordId, INDEX_FILE},
};

use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufReader, BufWriter, Seek, SeekFrom},
    mem,
    path::Path,
    vec,
};

#[derive(Serialize, Deserialize)]
struct BPTreeHeader {
    current_max_id: RecordId,
    checksum: u64,
    root_offset: Offset,
    last_root_offset: Offset,
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
        self.last_root_offset.hash(state);
    }
}

impl Default for BPTreeHeader {
    fn default() -> Self {
        let root_offset = mem::size_of::<BPTreeHeader>() as Offset;

        let mut tree_header = Self {
            current_max_id: 0,
            checksum: 0,
            root_offset,
            last_root_offset: root_offset,
        };

        tree_header.checksum = tree_header.calculate_checksum();

        tree_header
    }
}

pub struct BPTree {
    header: BPTreeHeader,
    file: BTreeFile,
    modified_nodes: HashMap<Offset, Node>,
}

#[derive(Serialize, Deserialize)]
pub struct Node {
    checksum: u64,
    is_leaf: bool,
    keys: Vec<RecordId>,
    values: Vec<Offset>,
    parent_offset: Offset,
    next_leaf_offset: Offset,
    free_slots: u16,
}

impl Node {
    fn new(is_leaf: bool) -> Self {
        let mut node = Self {
            checksum: 0,
            is_leaf,
            keys: vec![EMPTY_KEY_SLOT; MAX_KEYS],
            values: vec![EMPTY_CHILD_SLOT; M],
            parent_offset: NONE,
            next_leaf_offset: NONE,
            free_slots: MAX_KEYS as u16,
        };

        node.checksum = node.calculate_checksum();

        node
    }

    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    pub fn is_full(&self) -> bool {
        match self.is_leaf {
            true => self.free_slots == 0,
            false => self.free_slots == 1,
        }
    }

    pub fn insert(&mut self, key: RecordId, value: Offset) -> Result<()> {
        match self.is_leaf {
            true => self.insert_into_leaf(key, value),
            false => self.insert_into_internal(key, value),
        }
    }

    pub fn insert_into_leaf(&mut self, key: RecordId, value: Offset) -> Result<()> {
        let insert_pos = self.free_slots as usize - 1;

        self.keys.insert(insert_pos, key);
        self.values.insert(insert_pos, value);

        self.free_slots -= 1;

        Ok(())
    }

    fn insert_into_internal(&mut self, key: RecordId, value: Offset) -> Result<()> {
        let insert_pos = self.free_slots as usize - 1;

        self.keys.insert(insert_pos, key);
        self.values.insert(insert_pos + 1, value);

        self.free_slots -= 1;

        Ok(())
    }

    pub fn get_highest_subtree_offset(&self) -> Option<Offset> {
        if self.is_leaf {
            return None;
        }

        Some(self.values[self.free_slots as usize])
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.is_leaf.hash(state);
        self.keys.hash(state);
        self.values.hash(state);
        self.parent_offset.hash(state);
        self.next_leaf_offset.hash(state);
        self.free_slots.hash(state);
    }
}

struct BTreeFile {
    file: File,
    next_node_offset: Offset,
}

impl BTreeFile {
    fn new(mut file: File) -> Result<Self> {
        let next_node_offset = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            next_node_offset,
        })
    }

    fn get_next_offset(&mut self) -> Offset {
        let result: Offset = self.next_node_offset;
        self.next_node_offset += SERIALIZED_NODE_SIZE as u64;
        result
    }

    fn write_node(&mut self, node: &Node, offset: &Offset) -> Result<()> {
        self.file.seek(SeekFrom::Start(*offset))?;
        serialize_into(&mut BufWriter::new(&self.file), node)?;

        //TODO: file.sync_all()?
        Ok(())
    }

    fn read_node(&mut self, offset: &Offset) -> Result<Node> {
        self.file.seek(SeekFrom::Start(*offset))?;
        //TODO: BufReader with specified size
        let node: Node = deserialize_from(&mut BufReader::new(&self.file))?;

        match node.checksum == node.calculate_checksum() {
            true => Ok(node),
            false => Err(Error::IncorrectChecksum { offset: *offset }),
        }
    }

    fn update_header(&mut self, header: &BPTreeHeader) -> Result<()> {
        self.file.seek(SeekFrom::Start(0))?;

        serialize_into(&mut BufWriter::new(&self.file), header)?;

        self.file.sync_all()?;
        Ok(())
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

        let file = BTreeFile::new(file)?;

        let mut header = BPTreeHeader::default();

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

        let file = BTreeFile::new(file)?;
        let tree = Self { header, file };

        Ok(tree)
    }

    fn update_header(&mut self) -> Result<()> {
        self.header.checksum = self.header.calculate_checksum();
        self.file.update_header(&self.header)
    }

    fn create_root(&mut self) -> Result<()> {
        let mut root = Node::new(true);
        root.checksum = root.calculate_checksum();

        let root_offset = self.header.root_offset;
        self.file.write_node(&root, &root_offset)?;

        Ok(())
    }

    pub fn insert(&mut self, value: Offset) -> Result<()> {
        let root_offset = self.header.root_offset;
        let mut root: Node = self.file.read_node(&root_offset)?;

        let new_root_offset: Offset;
        let mut new_root: Node;

        let mut offset_node_map: HashMap<Offset, &mut Node> = HashMap::new();

        match root.is_full() {
            true => {
                new_root = Node::new(false);
                new_root_offset = self.file.get_next_offset();
                offset_node_map.insert(new_root_offset, &mut new_root);
                root.parent_offset = new_root_offset;
            }
            false => {}
        }

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

        //Assert
        let root_offset = tree.header.root_offset;
        let root = tree.file.read_node(&root_offset)?;

        assert_eq!(tree.header.current_max_id, 0);
        assert_eq!(
            tree.header.root_offset,
            mem::size_of::<BPTreeHeader>() as Offset
        );
        assert!(!root.is_leaf);
        assert_eq!(root.keys, vec![EMPTY_KEY_SLOT; M + 1]);
        assert_eq!(root.values, vec![EMPTY_CHILD_SLOT; M]);

        Ok(())
    }

    #[test]
    fn insert_into_empty_tree() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let mut tree = BPTree::create(path)?;
        let root_offset = tree.header.root_offset;
        let value = 1;

        //Act
        tree.insert(value)?;

        //Assert
        let root = tree.file.read_node(&root_offset)?;

        Ok(())
    }

    #[test]
    fn serialized_size_vs_mem_test() -> Result<()> {
        //Arrange
        let mut node = Node::new(true);
        let serialized_size = bincode::serialized_size(&node)?;
        let serialized_keys_size = bincode::serialized_size(&node.keys)?;
        let serialized_values_size = bincode::serialized_size(&node.values)?;
        let serialized_parent_offset_size = bincode::serialized_size(&node.parent_offset)?;
        node.parent_offset = 12121212;
        let serialized_parent_offset_size_sec = bincode::serialized_size(&node.parent_offset)?;
        let mem_size = mem::size_of::<Node>();

        //Assert
        assert_eq!(serialized_size, mem_size as u64);

        Ok(())
    }
}
