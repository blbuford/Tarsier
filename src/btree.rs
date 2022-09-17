use std::cell::{Ref, RefCell, RefMut};
use std::rc::Rc;

use crate::cursor::Cursor;
use crate::fetchable::Fetchable;
use crate::fetchable::Fetchable::{Fetched, Unfetched};
use crate::node::{InsertResult, Node, SplitEntry, MAX_INTERNAL_NODES, MAX_LEAF_NODES};
use crate::node_type::{InternalNode, KeyValuePair, LeafNode, NodeType};
use crate::pager::Pager;
use crate::Row;

pub const NODE_SIZE: usize = 4096;
pub const NODE_TYPE_OFFSET: usize = 0;
pub const IS_ROOT_OFFSET: usize = 1;
pub const PARENT_OFFSET: usize = 2;
pub const NUM_CELLS_OFFSET: usize = 6;
pub const CELL_KEY_SIZE: usize = 4;
pub const CELL_VALUE_SIZE: usize = 291;
pub const CELL_OFFSET: usize = 10;
pub const CELL_SIZE: usize = CELL_VALUE_SIZE + CELL_KEY_SIZE;

#[derive(Debug)]
pub struct BTree {
    root: RefCell<Node<usize, Row>>,
    pager: Pager,
}

pub type NodeLink<K, V> = Rc<RefCell<Fetchable<Node<K, V>>>>;

impl BTree {
    pub fn new(mut pager: Pager) -> Self {
        let root = if pager.num_pages() == 0 {
            let mut root_node = pager.get_page(0);
            root_node.is_root = true;
            pager.commit_page(&root_node);
            root_node
        } else {
            pager.get_page(0)
        };

        Self {
            root: RefCell::new(root),
            pager,
        }
    }

    pub fn get(&self, page_num: usize, cell_num: usize) -> Row {
        let node = self.pager.get_page(page_num);
        node.get(&cell_num).unwrap().clone()
    }

    pub fn insert(&mut self, key: usize, value: Row) -> bool {
        let SplitEntry { separator, tree } = match self._insert(key, self.root.borrow_mut(), value)
        {
            InsertResult::Success => return true,
            InsertResult::DuplicateKey => return false,
            InsertResult::ParentSplit(x) => x,
        };
        //infamous root split case
        let grow_tree = if let NodeType::Leaf(_) = self.root.borrow().node_type {
            true
        } else {
            self.root.borrow().num_cells >= MAX_INTERNAL_NODES
        };
        if grow_tree {
            // root is either a leaf node and we're making it an internal
            // or its internal and we're splitting it up
            let mut new_root = Node::internal();
            new_root.is_root = true;
            new_root.page_num = 0;
            let mut old_root = self.root.replace(new_root);
            old_root.page_num = self.pager.get_new_unused_page();
            old_root.is_root = false;
            if let NodeType::Internal(InternalNode {
                ref mut separators,
                ref mut children,
            }) = self.root.borrow_mut().node_type
            {
                separators.push(separator);
                children.push(Rc::new(RefCell::new(Fetched(old_root))));
                children.push(Rc::new(RefCell::new(Fetched(tree))));
            }
        } else {
            if let NodeType::Internal(InternalNode {
                ref mut separators,
                ref mut children,
            }) = self.root.borrow_mut().node_type
            {
                separators.push(separator);
                children.push(Rc::new(RefCell::new(Fetched(tree))));
            }
        }

        true
    }

    pub fn root(&self) -> Ref<Node<usize, Row>> {
        self.root.borrow()
    }

    pub fn close(&mut self) {
        self.pager.close()
    }

    pub fn find(&self, k: usize) -> Result<Cursor, Cursor> {
        dbg!(self._find(k, self.root.borrow()))
    }
    fn _find(&self, k: usize, node: Ref<Node<usize, Row>>) -> Result<Cursor, Cursor> {
        if let NodeType::Internal(InternalNode {
            ref separators,
            ref children,
        }) = node.node_type
        {
            let child = match separators.binary_search(&k) {
                Ok(index) => index + 1,
                Err(index) => index,
            };
            let child = children.get(child).unwrap();
            {
                let n = if let Unfetched(page_num) = *child.borrow() {
                    Some(self.pager.get_page(page_num.clone()))
                } else {
                    None
                };
                if let Some(n) = n {
                    child.replace(Fetched(n));
                }
            }

            self._find(k, Ref::map(child.borrow(), |f| f.as_ref().unwrap()))
        } else {
            node.find(&k)
        }
    }

    fn _insert(
        &mut self,
        k: usize,
        mut node: RefMut<Node<usize, Row>>,
        value: Row,
    ) -> InsertResult<usize, Row> {
        let page_num = node.page_num;
        if let NodeType::Internal(InternalNode {
            ref mut separators,
            ref mut children,
        }) = node.node_type
        {
            // find the child page of the key that we wish to insert on
            let child = match separators.binary_search(&k) {
                Ok(index) => index,
                Err(index) => index,
            };
            let child = children.get(child).unwrap();

            // fetch the page if it hasn't been
            let n = if let Unfetched(page_num) = *child.borrow() {
                Some(self.pager.get_page(page_num.clone()))
            } else {
                None
            };
            if let Some(n) = n {
                child.replace(Fetched(n));
            }

            // Ref magic and recursively call down to the leaf
            let parent = RefMut::map(child.borrow_mut(), |f| f.as_mut().unwrap());

            match self._insert(k, parent, value) {
                InsertResult::ParentSplit(SplitEntry {
                    separator,
                    mut tree,
                }) => {
                    tree.parent_offset = Some(page_num);
                    tree.page_num = self.pager.get_new_unused_page();
                    let location = separators.binary_search(&separator).unwrap_err();
                    separators.insert(location, separator.clone());
                    self.pager.commit_page(&tree);
                    children.insert(location + 1, Rc::new(RefCell::new(Fetched(tree))));

                    return if separators.len() >= MAX_INTERNAL_NODES {
                        //split internal
                        let upper_keys = separators.split_off((separators.len() / 2) - 1);
                        let separator = upper_keys.first().unwrap().clone();
                        let upper_children = children.split_off(separators.len() / 2);
                        let tree = Node::internal_with_separators(upper_keys, upper_children);

                        InsertResult::ParentSplit(SplitEntry { separator, tree })
                    } else {
                        InsertResult::Success
                    };
                }
                result => result,
            }
        } else {
            self.insert_leaf(node, value.id as usize, value)
        }
    }
    pub fn insert_leaf(
        &mut self,
        mut node: RefMut<Node<usize, Row>>,
        key: usize,
        value: Row,
    ) -> InsertResult<usize, Row> {
        if let NodeType::Leaf(LeafNode {
            ref mut children, ..
        }) = node.node_type
        {
            let location = match children.binary_search_by_key(&key, |pair| pair.key.clone()) {
                Ok(_duplicate_index) => return InsertResult::DuplicateKey,
                Err(index) => index,
            };
            children.insert(location, KeyValuePair { key, value });

            return if children.len() <= MAX_LEAF_NODES {
                node.num_cells += 1;
                self.pager.commit_page_ref(node);
                InsertResult::Success
            } else {
                let upper = children.split_off((children.len() / 2) - 1);
                let new_node = Node::leaf_with_children(upper);
                node.num_cells = children.len();
                self.pager.commit_page_ref(node);
                InsertResult::ParentSplit(SplitEntry {
                    separator: new_node.smallest_key().unwrap(),
                    tree: new_node,
                })
            };
        } else {
            panic!()
        }
    }
}

// struct BTIterator {
//     current: Option<NodeLink<usize, Row>>,
//     tree: BTree,
//     last_elem: usize,
// }
// impl Iterator for BTIterator {
//     type Item = Row;
//
//     fn next(&mut self) -> Option<Self::Item> {
//         match &self.current {
//             Some(node) => {
//                 let mut cursor = node.clone();
//                 loop {
//                     cursor = match cursor.borrow().as_ref() {
//                         Fetched(x) => match x.node_type {
//                             NodeType::Internal(InternalNode { ref children, .. }) => {
//                                 children.first().unwrap().clone()
//                             }
//                             NodeType::Leaf(_) => break,
//                         },
//                         Unfetched(i) => match self.tree.pager.get_page(i).node_type {
//                             NodeType::Internal(InternalNode { ref children, .. }) => {
//                                 children.first().unwrap().clone()
//                             }
//                             NodeType::Leaf(_) => break,
//                         },
//                     };
//                 }
//                 todo!()
//             }
//             None => None,
//         }
//     }
// }

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::btree::BTree;
    use crate::pager::Pager;
    use crate::Row;

    fn test_db_file_truncate() {
        let test_db = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open("test.db")
            .expect("test database");
        test_db.sync_all().expect("sync changes to disk");
    }
    #[test]
    fn test_multiple_leaf_splits() {
        test_db_file_truncate();
        let pager = Pager::open("test.db");
        let mut bt = BTree::new(pager);

        for i in 0..15 {
            if i == 14 {
                println!("18");
            }
            assert!(bt.insert(
                i,
                Row {
                    id: i as u32,
                    username: String::from(format!("user{i}")),
                    email: String::from(format!("user{i}@example.com"))
                }
            ));
        }
        dbg!(bt);
    }
}
