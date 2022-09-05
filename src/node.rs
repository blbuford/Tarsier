use std::cell::RefCell;
use std::fmt::Debug;

use crate::cursor::Cursor;
use crate::fetchable::Fetchable;
use crate::fetchable::Fetchable::{Fetched, Unfetched};
use crate::node_type::{KeyValuePair, NodeType};

pub const MAX_INTERNAL_NODES: usize = 511;

#[derive(Debug, Clone)]
pub struct Node<K, V> {
    pub(crate) is_root: bool,
    pub(crate) node_type: NodeType<K, V>,
    pub(crate) parent_offset: Option<usize>,
    pub(crate) num_cells: usize,
    pub(crate) page_num: usize,
}

impl<K: Ord + Clone, V: Debug> Node<K, V> {
    pub fn leaf() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::leaf_new(),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn leaf_with_children(children: Vec<KeyValuePair<K, V>>) -> Self {
        let num_cells = children.len();
        Self {
            is_root: false,
            node_type: NodeType::leaf_with_children(children),
            parent_offset: None,
            num_cells,
            page_num: 0,
        }
    }

    pub fn internal() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::internal_new(),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn get(&self, key: &K) -> Option<&V>
    where
        K: Ord,
    {
        if let NodeType::Leaf(ref cells, _) = self.node_type {
            return match cells.binary_search_by_key(&key, |pair| &pair.key) {
                Ok(index) => cells.get(index).map(|kvp| &kvp.value),
                Err(_) => None,
            };
        }
        None
    }

    pub fn insert(&mut self, location: usize, key: K, value: V) -> bool
    where
        K: Ord + Debug,
        V: Debug,
    {
        dbg!(&value);
        dbg!(&self);
        match self.node_type {
            NodeType::Leaf(ref mut cells, _) => {
                if cells.len() >= 12 {
                    panic!();
                }
                cells.insert(location, KeyValuePair { key, value });
                self.num_cells += 1;
                return true;
            }
            _ => panic!(),
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
            NodeType::Leaf(ref cells, next_leaf) => {
                dbg!(cells);
                let next = match next_leaf.borrow().as_ref() {
                    Unfetched(page) => {
                        if page == usize::MAX {
                            None
                        } else {
                            Some(page)
                        }
                    }
                    Fetched(node) => Some(node.page_num),
                };
                match cells.binary_search_by_key(&key, |pair| dbg!(&pair.key)) {
                    Ok(index) => {
                        if next.is_none() {
                            Ok(Cursor::new(
                                self.page_num,
                                index,
                                self.num_cells - 1 == index,
                            ))
                        } else {
                            Ok(Cursor::new(self.page_num, index, false))
                        }
                    }
                    Err(index) => {
                        if next.is_none() {
                            Err(Cursor::new(self.page_num, index, self.num_cells == index))
                        } else {
                            Err(Cursor::new(self.page_num, index, false))
                        }
                    }
                }
            }
            NodeType::Internal(..) => {
                panic!()
            }
        }
    }

    pub fn split(&mut self, new_page_num: usize) -> Node<K, V> {
        if let NodeType::Leaf(ref mut cells, ..) = self.node_type {
            let upper = cells.split_off(cells.len() / 2);
            let mut new_node = Node::leaf_with_children(upper);
            new_node.page_num = new_page_num;
            self.num_cells = cells.len();
            return new_node;
        } else {
            panic!()
        }
    }

    pub fn largest_key(&self) -> Option<&K> {
        if let NodeType::Leaf(ref cells, ..) = self.node_type {
            cells.iter().last().map(|pair| &pair.key)
        } else {
            None
        }
    }

    //TODO: Return Result<> here and do error handling
    pub fn insert_internal_child(
        &mut self,
        key: K,
        left: Option<Fetchable<Node<K, V>>>,
        right: Option<Fetchable<Node<K, V>>>,
    ) -> bool {
        if let NodeType::Internal(ref mut keys, ref mut children) = self.node_type {
            match keys.binary_search(&key) {
                Ok(_index) => {
                    panic!("Duplicate key");
                }
                Err(index) => {
                    if index > MAX_INTERNAL_NODES {
                        println!("Error: Trying to insert more internal children than can be stored by one node ({})!", index);
                        panic!();
                    } else {
                        keys.insert(index, key);
                        if let Some(left) = left {
                            children.insert(index, RefCell::new(left))
                        }
                        if let Some(right) = right {
                            children.insert(index + 1, RefCell::new(right))
                        }
                    }
                }
            }
            return true;
        }
        false
    }
}
