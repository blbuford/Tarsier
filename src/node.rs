use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use crate::cursor::Cursor;
use crate::fetchable::Fetchable::Unfetched;
use crate::node_type::{Child, NodeType};

#[derive(Debug, Clone)]
pub struct Node<K: Ord + Clone, V> {
    pub(crate) is_root: bool,
    pub(crate) node_type: NodeType<K, V>,
    pub(crate) parent_offset: Option<usize>,
    pub(crate) num_cells: usize,
    pub(crate) page_num: usize,
}

impl<K: Ord + Clone, V> Node<K, V> {
    pub fn leaf() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::Leaf(
                BTreeMap::new(),
                Rc::new(RefCell::new(Unfetched(usize::MAX))),
            ),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn leaf_with_children(children: BTreeMap<K, V>) -> Self {
        let num_cells = children.len();
        Self {
            is_root: false,
            node_type: NodeType::Leaf(children, Rc::new(RefCell::new(Unfetched(usize::MAX)))),
            parent_offset: None,
            num_cells,
            page_num: 0,
        }
    }

    pub fn internal() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::Internal(BTreeSet::new()),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn get(&self, key: K) -> Option<&V> {
        if let NodeType::Leaf(ref cells, _) = self.node_type {
            return cells.get(&key);
        }
        None
    }

    pub fn insert(&mut self, key: K, value: V) -> bool {
        match self.node_type {
            NodeType::Leaf(ref mut cells, _) => {
                if cells.len() >= 12 {
                    return false;
                }
                cells.insert(key, value);
                self.num_cells += 1;
                return true;
            }
            _ => panic!(),
        }
    }

    pub fn find(&self, k: K) -> Result<Cursor, Cursor> {
        match &self.node_type {
            NodeType::Leaf(ref cells, next_leaf) => match cells.get(&k) {
                Some(v) => {
                    if let Unfetched(pg) = next_leaf.borrow().as_ref() {
                        Ok(Cursor::new(
                            self.page_num,
                            0,
                            self.num_cells < 12
                                && cells.iter().last().unwrap().0.lt(&k)
                                && pg != usize::MAX,
                        ))
                    } else {
                        Ok(Cursor::new(self.page_num, 0, false))
                    }
                }
                None => Err(Cursor::new(self.page_num, 0, false)),
            },
            NodeType::Internal(_) => {
                panic!()
            }
        }
    }

    pub fn split(&mut self, new_page_num: usize) -> Node<K, V> {
        if let NodeType::Leaf(ref mut cells, ..) = self.node_type {
            let split_key = cells.iter().skip(5).next().map(|(k, _)| k.clone()).unwrap();
            let upper = cells.split_off(&split_key);
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
            cells.iter().last().map(|kvp| kvp.0)
        } else {
            None
        }
    }

    pub fn insert_internal_child(&mut self, record: Child<K>) -> bool {
        if let NodeType::Internal(ref mut children) = self.node_type {
            return children.insert(record);
        }

        false
    }
}
