use std::cell::{Ref, RefCell};
use std::cmp::Ordering;

use crate::cursor::Cursor;
use crate::fetchable::Fetchable::Unfetched;
use crate::node::Node;
use crate::node_type::{Child, NodeType};
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
            let child_record =
                Child::new(lower_largest_key.clone(), node.page_num, new_node.page_num);

            // TODO: Probably wrong because Cursor was computed pre-split
            match value.id.cmp(&(lower_largest_key as u32)) {
                Ordering::Less => {
                    let c = node.find(&(value.id as usize)).unwrap_err();
                    node.insert(c.cell_num(), value.id as usize, value)
                }
                Ordering::Greater => {
                    let c = new_node.find(&(value.id as usize)).unwrap_err();
                    new_node.insert(c.cell_num(), value.id as usize, value)
                }
                _ => panic!(),
            };

            self.pager.commit_page(&node);
            child_record.set_left(node);
            self.pager.commit_page(&new_node);
            child_record.set_right(new_node);

            if !new_parent.insert_internal_child(child_record) {
                panic!()
            }
            self.pager.commit_page(&new_parent);

            if new_parent.is_root {
                self.root.replace(new_parent);
            }

            true
        } else {
            if node.insert(cursor.cell_num(), value.id as usize, value) {
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
        self._find(k, self.root.borrow())
    }
    fn _find(&self, k: usize, node: Ref<Node<usize, Row>>) -> Result<Cursor, Cursor> {
        if let NodeType::Internal(ref children) = node.node_type {
            let child = match children.binary_search(&Child::new(k, 0, 0)) {
                Ok(index) => index,
                Err(index) => index - 1,
            };
            let child = children.get(child).unwrap();
            let mut uf = usize::MAX;
            let n = match k.cmp(child.key()) {
                Ordering::Greater => {
                    if let Unfetched(page_num) = *child.right() {
                        uf = page_num;
                    }
                    if uf != usize::MAX {
                        child.set_right(self.pager.get_page(uf))
                    }
                    child.right()
                }
                _ => {
                    if let Unfetched(page_num) = *child.left() {
                        uf = page_num;
                    }
                    if uf != usize::MAX {
                        child.set_left(self.pager.get_page(uf))
                    }
                    child.left()
                }
            };

            self._find(k, Ref::map(n, |f| f.as_ref().unwrap()))
        } else {
            node.find(&k)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::btree::BTree;
    use crate::pager::Pager;

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
    }
}
