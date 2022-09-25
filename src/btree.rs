use crate::cursor::Cursor;
use crate::node::{InsertResult, Node, SplitEntry, MAX_INTERNAL_NODES, MAX_LEAF_NODES};
use crate::node_type::{InternalNode, KeyValuePair, LeafNode, NodeType};
use crate::pager::{Offset, Pager};
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
    root: Offset,
    pager: Pager<Node<usize, Row>>,
    is_empty: bool,
}

impl BTree {
    pub fn new(mut pager: Pager<Node<usize, Row>>) -> Self {
        if pager.num_pages() == 0 {
            let mut root_node = Node::leaf();
            root_node.is_root = true;
            pager.commit(root_node);
            Self {
                root: Offset(0),
                pager,
                is_empty: true,
            }
        } else {
            let root = pager.get(&Offset(0));

            Self {
                root: Offset(0),
                pager,
                is_empty: root.num_cells > 0,
            }
        }
    }

    pub fn get(&self, offset: &Offset, cell_num: usize) -> Option<Row> {
        let node = self.pager.get(offset);
        match node.node_type() {
            NodeType::Leaf(LeafNode { children, .. }) => {
                children.get(cell_num).map(|kv| kv.value.clone())
            }
            _ => panic!("Can't retrive a row from an internal node"),
        }
    }

    pub fn insert(&mut self, key: usize, value: Row) -> bool {
        let SplitEntry { separator, tree } = match self._insert(&self.root.clone(), key, value) {
            InsertResult::Success => {
                self.is_empty = false;
                return true;
            }
            InsertResult::DuplicateKey => return false,
            InsertResult::ParentSplit(x) => x,
        };
        //infamous root split case
        let mut root_node = self.pager.get_mut(&self.root);
        let grow_tree = if let NodeType::Leaf(_) = root_node.node_type {
            true
        } else {
            root_node.num_cells >= MAX_INTERNAL_NODES
        };
        if grow_tree {
            // root is either a leaf node and we're making it an internal
            // or its internal and we're splitting it up
            let mut new_root: Node<usize, Row> = Node::internal();
            new_root.is_root = true;
            new_root.offset = Offset(0);
            root_node.offset = self.pager.new_page();
            root_node.is_root = false;
            if let NodeType::Internal(InternalNode {
                ref mut separators,
                ref mut children,
            }) = new_root.node_type
            {
                separators.push(separator);
                children.push(root_node.offset);
                children.push(tree.offset);
            }
            self.pager.commit(new_root);
            self.pager.commit(tree);
        } else {
            if let NodeType::Internal(InternalNode {
                ref mut separators,
                ref mut children,
            }) = root_node.node_type
            {
                separators.push(separator);
                children.push(tree.offset);
            }
        }
        self.is_empty = false;
        true
    }
    pub fn root(&self) -> Offset {
        self.root.clone()
    }

    pub fn is_empty(&self) -> bool {
        self.is_empty
    }

    pub fn advance_cursor(&self, cursor: &mut Cursor) {
        let node: &Node<usize, Row> = self.pager.get(cursor.offset());
        match node.node_type() {
            NodeType::Internal(..) => panic!("Cursors shouldn't point at internal nodes"),
            NodeType::Leaf(LeafNode {
                children,
                next_leaf,
                ..
            }) => {
                if children.len() - 1 > cursor.cell_num() {
                    cursor.cell_num += 1;
                    cursor.end_of_table =
                        next_leaf.is_none() && cursor.cell_num == children.len() - 1
                } else {
                    match next_leaf {
                        Some(next) => {
                            cursor.offset = next.clone();
                            cursor.cell_num = 0;
                            cursor.end_of_table = false;
                        }
                        None => cursor.end_of_table = true,
                    }
                }
            }
        }
    }

    pub fn close(&mut self) {
        self.pager.close()
    }

    pub fn find(&self, k: usize) -> Result<Cursor, Cursor> {
        self._find(k, &self.root)
    }
    fn _find(&self, k: usize, offset: &Offset) -> Result<Cursor, Cursor> {
        let node: &Node<usize, Row> = self.pager.get(offset);
        if let NodeType::Internal(InternalNode {
            ref separators,
            ref children,
        }) = node.node_type
        {
            let child = match separators.binary_search(&k) {
                Ok(index) => index + 1,
                Err(index) => index,
            };
            let child_offset = children.get(child).unwrap();
            self._find(k, child_offset)
        } else {
            node.find(&k)
        }
    }

    fn _insert(&mut self, offset: &Offset, k: usize, value: Row) -> InsertResult<usize, Row> {
        let mut node = self.pager.get_mut(offset);
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
            let child_offset = children.get(child).unwrap();

            match self._insert(child_offset, k, value) {
                InsertResult::ParentSplit(SplitEntry {
                    separator,
                    mut tree,
                }) => {
                    tree.parent_offset = Some(offset.clone());
                    let mut left_child: &mut Node<usize, Row> = self.pager.get_mut(child_offset);
                    tree.set_last_leaf(Some(left_child.offset));

                    // Voodoo to insert tree into the middle of two leaves
                    left_child
                        .set_next_leaf(Some(tree.offset))
                        .map(|right_child_offset| {
                            tree.set_next_leaf(Some(right_child_offset));
                            let mut right_child: &mut Node<usize, Row> =
                                self.pager.get_mut(&right_child_offset);
                            right_child.set_last_leaf(Some(tree.offset));
                        });

                    let location = separators.binary_search(&separator).unwrap_err();
                    separators.insert(location, separator.clone());

                    children.insert(location + 1, tree.offset);
                    self.pager.commit(tree);
                    return if separators.len() >= MAX_INTERNAL_NODES {
                        //split internal
                        let upper_keys = separators.split_off((separators.len() / 2) - 1);
                        let separator = upper_keys.first().unwrap().clone();
                        let upper_children = children.split_off(separators.len() / 2);
                        let mut tree = Node::internal_with_separators(upper_keys, upper_children);
                        tree.offset = self.pager.new_page();
                        InsertResult::ParentSplit(SplitEntry { separator, tree })
                    } else {
                        InsertResult::Success
                    };
                }
                result => result,
            }
        } else {
            self.insert_leaf(&mut node, value.id as usize, value)
        }
    }
    pub fn insert_leaf(
        &mut self,
        node: &mut Node<usize, Row>,
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
                InsertResult::Success
            } else {
                let upper = children.split_off(children.len() / 2);
                let mut new_node = Node::leaf_with_children(upper);
                node.num_cells = children.len();
                new_node.offset = self.pager.new_page();

                InsertResult::ParentSplit(SplitEntry {
                    separator: new_node.smallest_key().unwrap(),
                    tree: new_node,
                })
            };
        } else {
            panic!()
        }
    }

    pub fn cursor_start(&self) -> Cursor {
        let mut cursor = self.root.clone();
        let mut end_of_table = false;
        loop {
            let node: &Node<usize, Row> = self.pager.get(&cursor);
            match node.node_type() {
                NodeType::Internal(InternalNode { children, .. }) => {
                    cursor = children.first().unwrap().clone()
                }
                NodeType::Leaf(LeafNode { children, .. }) => {
                    end_of_table = children.is_empty();
                    break;
                }
            }
        }
        Cursor {
            offset: cursor,
            cell_num: 0,
            end_of_table,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::btree::BTree;
    use crate::cursor::Cursor;
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
        let count = 60;

        for i in 0..count {
            assert!(bt.insert(
                i,
                Row {
                    id: i as u32,
                    username: String::from(format!("user{i}")),
                    email: String::from(format!("user{i}@example.com"))
                }
            ));
        }
        let mut cursor = bt.cursor_start();
        let mut i: u32 = 0;
        while !cursor.is_at_end_of_table() {
            let val = cursor.value(&bt).id;
            assert_eq!(val, i);
            bt.advance_cursor(&mut cursor);
            i += 1;
        }
    }
}
