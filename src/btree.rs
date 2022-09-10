use std::borrow::{Borrow, BorrowMut};
use std::cell::{Ref, RefCell, RefMut};
use std::cmp::Ordering;

use crate::cursor::Cursor;
use crate::fetchable::Fetchable::{Fetched, Unfetched};
use crate::node::{InsertResult, Node, SplitEntry, MAX_INTERNAL_NODES};
use crate::node_type::NodeType;
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

    pub fn insert(&mut self, cursor: &Cursor, value: Row) -> bool {
        let mut node = self.pager.get_page(cursor.page_num());
        // TODO: Check this node's parent offset for an internal node. If exists, split all of its children to maintain B+-Tree height property
        return if node.num_cells >= 12 {
            let mut new_node = node.split(self.pager.num_pages());
            let mut new_parent: Node<usize, Row> = Node::internal();
            new_parent.page_num = self.pager.num_pages() + 1;
            std::mem::swap(&mut node.page_num, &mut new_parent.page_num);
            std::mem::swap(&mut node.is_root, &mut new_parent.is_root);
            node.parent_offset = Some(new_parent.page_num);
            new_node.parent_offset = Some(new_parent.page_num);
            let lower_largest_key = node.largest_key().unwrap().clone();

            match value.id.cmp(&(lower_largest_key as u32)) {
                Ordering::Less => {
                    let c = node.find(&(value.id as usize)).unwrap_err();
                    node.insert(value.id as usize, value)
                }
                Ordering::Greater => {
                    let c = new_node.find(&(value.id as usize)).unwrap_err();
                    new_node.insert(value.id as usize, value)
                }
                _ => panic!(),
            };

            self.pager.commit_page(&node);
            self.pager.commit_page(&new_node);

            if !new_parent.insert_internal_child(lower_largest_key.clone(), Fetched(new_node)) {
                panic!()
            }
            self.pager.commit_page(&new_parent);

            if new_parent.is_root {
                self.root.replace(new_parent);
            }

            true
        } else {
            if let InsertResult::Success = node.insert(value.id as usize, value) {
                self.pager.commit_page(&node);
                if node.is_root {
                    self.root.replace(node);
                }
                true
            } else {
                false
            }
        };
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
        if let NodeType::Internal(ref keys, ref children) = node.node_type {
            let child = match keys.binary_search(&k) {
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
        if let NodeType::Internal(ref mut keys, ref mut children) = node.node_type {
            // find the child page of the key that we wish to insert on
            let child = match keys.binary_search(&k) {
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
            let node = RefMut::map(child.borrow_mut(), |f| f.as_mut().unwrap());

            match self._insert(k, node, value) {
                InsertResult::ParentSplit(SplitEntry { separator, tree }) => {
                    let location = keys.binary_search(&separator).unwrap_err();
                    keys.insert(location, separator.clone());
                    children.insert(location + 1, RefCell::new(Fetched(tree)));
                    return if keys.len() >= MAX_INTERNAL_NODES {
                        //split internal
                        let upper_keys = keys.split_off((keys.len() / 2) - 1);
                        let separator = upper_keys.first().unwrap().clone();
                        let upper_children = children.split_off(keys.len() / 2);
                        let tree = Node::internal_with_separators(upper_keys, upper_children);
                        InsertResult::ParentSplit(SplitEntry { separator, tree })
                    } else {
                        InsertResult::Success
                    };
                }
                result => result,
            }
        } else {
            node.insert(value.id as usize, value)
        }
    }
}

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
        let mut pager = Pager::open("test.db");
        let mut bt = BTree::new(pager);

        for i in 0..15 {
            if i == 14 {
                println!("18");
            }
            let c = bt.find(i).unwrap_err();
            assert!(bt.insert(
                &c,
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
