use std::fmt::Debug;

use crate::cursor::Cursor;
use crate::node_type::{InternalNode, KeyValuePair, LeafNode, NodeType};
use crate::pager::{HasOffset, Offset};

pub const MAX_INTERNAL_NODES: usize = 511;
pub const MAX_LEAF_NODES: usize = 12;

#[derive(Debug, Clone)]
pub enum InsertResult<K, V> {
    Success,
    DuplicateKey,
    ParentSplit(SplitEntry<K, V>),
}
#[derive(Debug, Clone)]
pub struct SplitEntry<K, V> {
    pub(crate) separator: K,
    pub(crate) tree: Node<K, V>,
}
#[derive(Debug, Clone)]
pub struct Node<K, V> {
    pub(crate) is_root: bool,
    pub(crate) node_type: NodeType<K, V>,
    pub(crate) parent_offset: Option<Offset>,
    pub(crate) num_cells: usize,
    pub(crate) offset: Offset,
}

impl<K: Ord + Clone, V: Debug> Node<K, V> {
    pub fn leaf() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::leaf_new(),
            parent_offset: None,
            num_cells: 0,
            offset: Offset(0),
        }
    }

    pub fn leaf_with_children(children: Vec<KeyValuePair<K, V>>) -> Self {
        let num_cells = children.len();
        Self {
            is_root: false,
            node_type: NodeType::leaf_with_children(children),
            parent_offset: None,
            num_cells,
            offset: Offset(0),
        }
    }

    pub fn internal() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::internal_new(),
            parent_offset: None,
            num_cells: 0,
            offset: Offset(0),
        }
    }

    pub fn internal_with_separators(keys: Vec<K>, children: Vec<Offset>) -> Self {
        Self {
            is_root: false,
            node_type: NodeType::internal_with_separators(keys, children),
            parent_offset: None,
            num_cells: 0,
            offset: Offset(0),
        }
    }

    pub fn get(&self, key: &K) -> Option<&V>
    where
        K: Ord,
    {
        if let NodeType::Leaf(LeafNode { ref children, .. }) = self.node_type {
            return match children.binary_search_by_key(&key, |pair| &pair.key) {
                Ok(index) => children.get(index).map(|kvp| &kvp.value),
                Err(_) => None,
            };
        }
        None
    }

    pub fn insert_leaf(&mut self, key: K, value: V) -> InsertResult<K, V>
    where
        K: Ord + Debug,
        V: Debug,
    {
        if let NodeType::Leaf(LeafNode {
            ref mut children, ..
        }) = self.node_type
        {
            let location = match children.binary_search_by_key(&key, |pair| pair.key.clone()) {
                Ok(_duplicate_index) => return InsertResult::DuplicateKey,
                Err(index) => index,
            };
            children.insert(location, KeyValuePair { key, value });
            self.num_cells += 1;
            return if self.num_cells <= MAX_LEAF_NODES {
                InsertResult::Success
            } else {
                let upper = children.split_off((children.len() / 2) - 1);
                let new_node = Node::leaf_with_children(upper);
                self.num_cells = children.len();
                InsertResult::ParentSplit(SplitEntry {
                    separator: new_node.smallest_key().unwrap(),
                    tree: new_node,
                })
            };
        } else {
            panic!()
        }
    }

    /// Returns a Result<Cursor> pointing to where to operate next. Ok(Cursor) means it found the item
    /// and is pointing at it. Err(Cursor) is where to insert the item
    pub fn find(&self, key: &K) -> Result<Cursor, Cursor>
    where
        K: Debug,
    {
        dbg!(key);
        match &self.node_type {
            NodeType::Leaf(LeafNode {
                children,
                next_leaf,
                ..
            }) => {
                dbg!(children);

                match children.binary_search_by_key(&key, |pair| dbg!(&pair.key)) {
                    Ok(index) => Ok(Cursor::new(
                        self.offset,
                        index,
                        next_leaf.is_none() && index == self.num_cells - 1,
                    )),
                    Err(index) => {
                        if index > MAX_LEAF_NODES && next_leaf.is_some() {
                            Err(Cursor::new(next_leaf.unwrap(), 0, false))
                        } else {
                            Err(Cursor::new(
                                self.offset,
                                index,
                                next_leaf.is_none() && index == self.num_cells,
                            ))
                        }
                    }
                }
            }
            NodeType::Internal(..) => {
                panic!()
            }
        }
    }

    pub fn split(&mut self, new_page: Offset) -> Node<K, V> {
        if let NodeType::Leaf(LeafNode {
            ref mut children, ..
        }) = self.node_type
        {
            let upper = children.split_off(children.len() / 2);
            let mut new_node = Node::leaf_with_children(upper);
            new_node.offset = new_page;
            self.num_cells = children.len();
            return new_node;
        } else {
            panic!()
        }
    }

    pub fn node_type(&self) -> &NodeType<K, V> {
        &self.node_type
    }

    pub fn largest_key(&self) -> Option<&K> {
        if let NodeType::Leaf(LeafNode { ref children, .. }) = self.node_type {
            children.iter().last().map(|pair| &pair.key)
        } else {
            None
        }
    }
    pub fn smallest_key(&self) -> Option<K> {
        if let NodeType::Leaf(LeafNode { ref children, .. }) = self.node_type {
            children.first().map(|pair| pair.key.clone())
        } else {
            None
        }
    }

    //TODO: Return Result<> here and do error handling
    pub fn insert_internal_child(&mut self, key: K, right: Offset) -> bool {
        if let NodeType::Internal(InternalNode {
            ref mut separators,
            ref mut children,
        }) = self.node_type
        {
            match separators.binary_search(&key) {
                Ok(_index) => {
                    panic!("Duplicate key");
                }
                Err(index) => {
                    if index > MAX_INTERNAL_NODES {
                        println!("Error: Trying to insert more internal children than can be stored by one node ({})!", index);
                        panic!();
                    } else {
                        separators.insert(index, key);
                        children.insert(index + 1, right)
                    }
                }
            }
            return true;
        }
        false
    }
    pub fn set_last_leaf(&mut self, last: Option<Offset>) -> Option<Offset> {
        if let NodeType::Leaf(LeafNode { mut last_leaf, .. }) = self.node_type {
            match last {
                Some(o) => last_leaf.replace(o),
                None => last_leaf.take(),
            }
        } else {
            panic!("Called on a non-leaf node!")
        }
    }

    /// Takes a `next` and returns what it replaced
    pub fn set_next_leaf(&mut self, next: Option<Offset>) -> Option<Offset> {
        if let NodeType::Leaf(LeafNode { mut next_leaf, .. }) = self.node_type {
            match next {
                Some(o) => next_leaf.replace(o),
                None => next_leaf.take(),
            }
        } else {
            panic!("Called on a non-leaf node!")
        }
    }

    pub fn get_next_leaf(&mut self) -> Option<Offset> {
        if let NodeType::Leaf(LeafNode { mut next_leaf, .. }) = self.node_type {
            next_leaf.clone()
        } else {
            panic!("Called on a non-leaf node!")
        }
    }

    pub fn get_last_leaf(&mut self) -> Option<Offset> {
        if let NodeType::Leaf(LeafNode { mut last_leaf, .. }) = self.node_type {
            last_leaf.clone()
        } else {
            panic!("Called on a non-leaf node!")
        }
    }
}

impl<K, V> HasOffset for Node<K, V> {
    fn offset(&self) -> Offset {
        self.offset
    }
}

#[cfg(test)]
mod tests {
    use crate::node::{InsertResult, Node, MAX_LEAF_NODES};

    #[test]
    fn test_leaf_inserts() {
        let mut n: Node<usize, usize> = Node::leaf();
        for i in 0..MAX_LEAF_NODES {
            assert!(matches!(n.insert_leaf(i, i), InsertResult::Success));
        }
        assert!(matches!(n.insert_leaf(0, 0), InsertResult::DuplicateKey));
        assert!(matches!(
            n.insert_leaf(MAX_LEAF_NODES + 1, 0),
            InsertResult::ParentSplit(..)
        ));
    }
}
