use super::{
    types::{
        InsertionResult, NodeIdx, DEFAULT_BRANCHING_FACTOR, EMPTY_CHILD_SLOT, EMPTY_KEY_SLOT,
        FIRST_VALUE_SLOT, HIGHEST_KEY_SLOT, MAX_KEYS, SERIALIZED_NODE_SIZE,
    },
    Error, Result,
};
use bincode::{deserialize_from, serialize_into, serialized_size};
use serde::{Deserialize, Serialize};

use crate::{
    components::collection::types::NONE,
    types::{Offset, RecordId, INDEX_FILE},
};

use core::alloc;
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
    branching_factor: u16,
    current_max_id: RecordId,
    checksum: u64,
    root_offset: Offset,
    last_root_offset: Offset,
}

impl BPTreeHeader {
    fn new(branching_factor: u16) -> Self {
        let root_offset = mem::size_of::<BPTreeHeader>() as Offset;

        let tree_header = Self {
            branching_factor,
            current_max_id: 0,
            checksum: 0,
            root_offset,
            last_root_offset: root_offset,
        };

        tree_header
    }

    fn calculate_checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }

    fn define_header(file: &mut File) -> Result<Self> {
        //TODO: Find MAX_ID in tree
        let mut header = BPTreeHeader::new(DEFAULT_BRANCHING_FACTOR);
        let checksum = header.calculate_checksum();
        header.checksum = checksum;

        file.seek(SeekFrom::Start(0))?;
        serialize_into(&mut BufWriter::new(file), &header)?;

        Ok(header)
    }
}

impl Hash for BPTreeHeader {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.branching_factor.hash(state);
        self.current_max_id.hash(state);
        self.root_offset.hash(state);
        self.last_root_offset.hash(state);
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
    next_leaf_offset: Offset,
    recently_taken_key_slot: u16,
}

impl Node {
    fn new(is_leaf: bool, branching_factor: u16) -> Self {
        let mut node = Self {
            checksum: 0,
            is_leaf,
            keys: vec![EMPTY_KEY_SLOT; (branching_factor - 1) as usize],
            values: vec![EMPTY_CHILD_SLOT; branching_factor as usize],
            next_leaf_offset: NONE,
            recently_taken_key_slot: branching_factor - 1,
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
            true => self.recently_taken_key_slot == 0,
            false => self.recently_taken_key_slot == 0,
        }
    }

    pub fn insert(&mut self, key: RecordId, value: Offset) -> Result<()> {
        if self.is_full() {
            return Err(Error::NodeIsFull);
        }

        self.recently_taken_key_slot -= 1;

        self.keys[self.recently_taken_key_slot as usize] = key;
        self.values[self.recently_taken_key_slot as usize] = value;

        Ok(())
    }

    pub fn get_highest_subtree_index(&self) -> Option<NodeIdx> {
        if self.is_leaf {
            return None;
        }

        let branching_factor = self.values.len() as u16;

        Some(match self.recently_taken_key_slot == branching_factor {
            true => self.recently_taken_key_slot,
            false => self.recently_taken_key_slot - 1,
        })
    }

    pub fn get_highest_subtree_offset(&self) -> Option<Offset> {
        self.get_highest_subtree_index()
            .map(|index| self.values[index as usize])
    }

    pub fn update_highest_subtree_offset(&mut self, value: Offset) -> Result<()> {
        match self.get_highest_subtree_index() {
            Some(index) => {
                self.values[index as usize] = value;
                Ok(())
            }
            None => Err(Error::UnexpectedError(
                "BTree: Cannot find highest subtree index.",
            )),
        }
    }
}

impl Hash for Node {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.is_leaf.hash(state);
        self.keys.hash(state);
        self.values.hash(state);
        self.next_leaf_offset.hash(state);
        self.recently_taken_key_slot.hash(state);
    }
}

struct BTreeFile {
    file: File,
    last_node_offset: Offset,
}

impl BTreeFile {
    fn new(mut file: File) -> Result<Self> {
        let last_node_offset = file.seek(SeekFrom::End(0))?;
        Ok(Self {
            file,
            last_node_offset,
        })
    }

    fn get_next_offset(&mut self) -> Offset {
        self.last_node_offset += SERIALIZED_NODE_SIZE as u64;
        self.last_node_offset
    }

    fn write_node(&mut self, node: &Node, offset: &Offset) -> Result<()> {
        self.file.seek(SeekFrom::Start(*offset))?;
        serialize_into(&mut BufWriter::new(&self.file), node)?;

        //TODO: file.sync_all()?
        Ok(())
    }

    pub fn write_nodes(&mut self, nodes: &HashMap<Offset, Node>) -> Result<()> {
        self.alloc_space_for_nodes()?;

        //TODO: Good to iterate over in desc order? Or sort by offset?
        let mut offsets: Vec<&Offset> = nodes.keys().collect();
        offsets.sort();

        for offset in offsets {
            let node = nodes
                .get(offset)
                .ok_or(Error::UnexpectedError("BTree: Cannot get node."))?;
            self.write_node(node, offset)?;
        }

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

    fn alloc_space_for_nodes(&mut self) -> Result<()> {
        let new_file_len = self.last_node_offset + SERIALIZED_NODE_SIZE as u64;
        self.file.set_len(new_file_len)?;
        Ok(())
    }
}

impl BPTree {
    //TODO: Is that good to skip ID_OFFSET_STORAGE and work only with index
    pub fn create(path: &Path, branching_factor: u16) -> Result<Self> {
        let file_path = path.join(INDEX_FILE);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(file_path)?;

        let header = BPTreeHeader::new(branching_factor);

        let file_len = serialized_size(&header)?;
        file.set_len(file_len)?;
        let file = BTreeFile::new(file)?;

        let modified_nodes: HashMap<Offset, Node> = HashMap::new();

        let mut tree = Self {
            header,
            file,
            modified_nodes,
        };

        tree.create_root()?;
        tree.update_header()?;

        Ok(tree)
    }

    pub fn load(path: &Path) -> Result<Self> {
        let file_path = path.join(INDEX_FILE);

        let mut file = OpenOptions::new().read(true).write(true).open(file_path)?;

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
        let modified_nodes: HashMap<Offset, Node> = HashMap::new();

        let tree = Self {
            header,
            file,
            modified_nodes,
        };

        Ok(tree)
    }

    fn update_header(&mut self) -> Result<()> {
        self.header.checksum = self.header.calculate_checksum();
        self.file.update_header(&self.header)
    }

    fn create_root(&mut self) -> Result<()> {
        let mut root = Node::new(true, self.header.branching_factor);
        root.checksum = root.calculate_checksum();

        let root_offset = self.header.root_offset;
        self.file.write_node(&root, &root_offset)?;

        Ok(())
    }

    fn find_leftmost_leaf_path(&mut self) -> Result<Vec<(Offset, Node)>> {
        let mut path = Vec::new();

        let mut offset = self.header.root_offset;
        let mut node = self.file.read_node(&offset)?;

        while let Some(next_offset) = node.get_highest_subtree_offset() {
            path.push((offset, node));
            node = self.file.read_node(&offset)?;
            offset = next_offset;
        }

        path.push((offset, node));

        Ok(path)
    }

    pub fn insert(&mut self, value: Offset) -> Result<()> {
        self.header.current_max_id += 1;
        let next_key = self.header.current_max_id;

        match self.recursive_insert(self.header.root_offset, next_key, value) {
            Ok(InsertionResult::Inserted {
                existing_child_new_offset,
            }) => {
                self.header.last_root_offset = self.header.root_offset;
                self.header.root_offset = existing_child_new_offset;
            }
            Ok(InsertionResult::InsertedAndPromoted {
                promoted_key,
                existing_child_new_offset: old_root_offset,
                new_child_offset,
            }) => {
                let new_root_offset: u64 = self.file.get_next_offset();
                let mut new_root = Node::new(false, self.header.branching_factor);

                new_root.values[FIRST_VALUE_SLOT as usize] = old_root_offset;
                new_root.insert(promoted_key, new_child_offset)?;

                self.modified_nodes.insert(new_root_offset, new_root);

                self.header.last_root_offset = old_root_offset;
                self.header.root_offset = new_root_offset;
            }
            Err(_) => return Err(Error::UnexpectedError("BTree: Cannot insert.")),
        }

        self.flush_modified_nodes()?;
        self.update_header()?;
        Ok(())
    }

    fn recursive_insert(
        &mut self,
        node_offset: Offset,
        key: RecordId,
        value: Offset,
    ) -> Result<InsertionResult> {
        let mut node = self.file.read_node(&node_offset)?;

        match node.is_leaf {
            true => match node.insert(key, value) {
                Ok(()) => {
                    let new_offset = self.file.get_next_offset();
                    self.modified_nodes.insert(new_offset, node);
                    Ok(InsertionResult::Inserted {
                        existing_child_new_offset: new_offset,
                    })
                }
                Err(Error::NodeIsFull) => {
                    let new_offset = self.file.get_next_offset();
                    let new_node_offset = self.file.get_next_offset();

                    node.next_leaf_offset = new_node_offset;

                    let mut new_node = Node::new(true, self.header.branching_factor);
                    new_node.insert(key, value)?;

                    let key_to_promote =
                        *node
                            .keys
                            .get(HIGHEST_KEY_SLOT)
                            .ok_or(Error::UnexpectedError(
                                "BTree: Cannot get value from the highest key slot for leaf node.",
                            ))?;

                    self.modified_nodes.insert(new_offset, node);
                    self.modified_nodes.insert(new_node_offset, new_node);

                    Ok(InsertionResult::InsertedAndPromoted {
                        existing_child_new_offset: new_offset,
                        promoted_key: key_to_promote,
                        new_child_offset: new_node_offset,
                    })
                }
                Err(_) => Err(Error::UnexpectedError("BTree: Cannot insert into leaf.")),
            },
            false => {
                let child_offset =
                    node.get_highest_subtree_offset()
                        .ok_or(Error::UnexpectedError(
                            "BTree: Cannot find highest subtree offset for internal node.",
                        ))?;

                match self.recursive_insert(child_offset, key, value)? {
                    InsertionResult::Inserted {
                        existing_child_new_offset,
                    } => {
                        let new_offset = self.file.get_next_offset();
                        node.update_highest_subtree_offset(existing_child_new_offset)?;

                        self.modified_nodes.insert(new_offset, node);

                        Ok(InsertionResult::Inserted {
                            existing_child_new_offset: new_offset,
                        })
                    }
                    InsertionResult::InsertedAndPromoted {
                        promoted_key,
                        existing_child_new_offset,
                        new_child_offset,
                    } => {
                        let new_offset = self.file.get_next_offset();
                        node.update_highest_subtree_offset(existing_child_new_offset)?;

                        match node.insert(promoted_key, new_child_offset) {
                            Ok(()) => {
                                self.modified_nodes.insert(new_offset, node);
                                Ok(InsertionResult::Inserted {
                                    existing_child_new_offset: new_offset,
                                })
                            }
                            Err(Error::NodeIsFull) => {
                                let new_offset = self.file.get_next_offset();
                                let new_node_offset = self.file.get_next_offset();

                                let mut new_node = Node::new(false, self.header.branching_factor);
                                new_node.values[FIRST_VALUE_SLOT as usize] = new_node_offset;

                                self.modified_nodes.insert(new_offset, node);
                                self.modified_nodes.insert(new_node_offset, new_node);

                                Ok(InsertionResult::InsertedAndPromoted {
                                    promoted_key,
                                    existing_child_new_offset: new_offset,
                                    new_child_offset: new_node_offset,
                                })
                            }
                            Err(_) => Err(Error::UnexpectedError(
                                "BTree: Cannot insert into internal node.",
                            )),
                        }
                    }
                }
            }
        }
    }

    fn flush_modified_nodes(&mut self) -> Result<()> {
        for (_, node) in self.modified_nodes.iter_mut() {
            node.checksum = node.calculate_checksum();
        }
        self.file.write_nodes(&self.modified_nodes)?;
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
        let branching_factor = 3;

        //Act
        let mut tree = BPTree::create(path, branching_factor)?;

        //Assert
        let root_offset = tree.header.root_offset;
        let root = tree.file.read_node(&root_offset)?;

        assert_eq!(tree.header.current_max_id, 0);
        assert_eq!(
            tree.header.root_offset,
            mem::size_of::<BPTreeHeader>() as Offset
        );
        assert!(root.is_leaf);
        assert_eq!(
            root.keys,
            vec![EMPTY_KEY_SLOT; (tree.header.branching_factor - 1) as usize]
        );
        assert_eq!(
            root.values,
            vec![EMPTY_CHILD_SLOT; tree.header.branching_factor as usize]
        );

        Ok(())
    }

    #[test]
    fn insert_into_empty_tree() -> Result<()> {
        //Arrange
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path();
        let branching_factor = 3;
        let mut tree = BPTree::create(path, branching_factor)?;
        let value = 123;

        //Act
        tree.insert(value)?;

        //Assert
        let root = tree.file.read_node(&tree.header.root_offset)?;

        assert!(root.is_leaf);
        assert_eq!(tree.header.current_max_id, 1);
        assert_eq!(
            root.values.get((root.recently_taken_key_slot) as usize),
            Some(&123)
        );

        Ok(())
    }

    #[test]
    fn serialized_size_should_equal_const() -> Result<()> {
        //Arrange
        let node = Node::new(true, DEFAULT_BRANCHING_FACTOR);
        let serialized_size = bincode::serialized_size(&node)?;

        //Assert
        assert_eq!(serialized_size, SERIALIZED_NODE_SIZE as u64);

        Ok(())
    }
}
