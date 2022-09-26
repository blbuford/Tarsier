use std::fmt::{Debug, Formatter};
use std::io::Write;

use crate::btree::{
    CELL_KEY_SIZE, CELL_OFFSET, CELL_SIZE, CELL_VALUE_SIZE, IS_ROOT_OFFSET, NODE_TYPE_OFFSET,
    NUM_CELLS_OFFSET, PARENT_OFFSET,
};
use crate::datastore::ROW_SIZE;
use crate::node::Node;
use crate::node_type::{InternalNode, KeyValuePair, LeafNode, NodeType};
use crate::pager::Offset;
use crate::Row;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;
pub const RIGHTMOST_CHILD_OFFSET: usize = 10;
pub const INTERNAL_CHILDREN_OFFSET: usize = RIGHTMOST_CHILD_OFFSET + 4;
pub const INTERNAL_CHILD_SIZE: usize = 12;

pub struct Page(Box<Box<[u8; PAGE_SIZE as usize]>>);

impl Page {
    pub fn new() -> Self {
        Self(Box::new(Box::new([0 as u8; PAGE_SIZE as usize])))
    }

    pub fn load(p: Box<[u8; PAGE_SIZE]>) -> Self {
        Self(Box::new(p))
    }

    pub fn insert(&mut self, row: Row, slot: usize) {
        let min = slot * ROW_SIZE;
        let max = min + ROW_SIZE;
        self.0[min..max].swap_with_slice(&mut *row.serialize());
    }

    pub fn select(&self, slot: usize) -> Row {
        let min = slot * ROW_SIZE;
        let max = min + ROW_SIZE;
        Row::deserialize(&self.0[min..max])
    }

    pub fn write(&self, mut writer: impl Write) -> std::io::Result<usize> {
        writer.write(&self.0[0..PAGE_SIZE])
    }

    pub fn is_root_node(&self) -> bool {
        self.0[IS_ROOT_OFFSET] == 1
    }

    pub fn set_root_node(&mut self, is_root_node: bool) {
        if is_root_node {
            self.0[IS_ROOT_OFFSET] = 1
        } else {
            self.0[IS_ROOT_OFFSET] = 0
        }
    }

    pub fn parent_offset(&self) -> Option<Offset> {
        Some(Offset(
            u32::from_ne_bytes(self.0[PARENT_OFFSET..PARENT_OFFSET + 4].try_into().unwrap())
                as usize,
        ))
    }

    pub fn set_parent_offset(&mut self, parent_offset: Option<Offset>) {
        if let Some(offset) = parent_offset {
            self.0[PARENT_OFFSET..PARENT_OFFSET + 4]
                .swap_with_slice(&mut (offset.0 as u32).to_ne_bytes())
        }
    }

    pub fn num_cells(&self) -> usize {
        u32::from_ne_bytes(
            self.0[NUM_CELLS_OFFSET..NUM_CELLS_OFFSET + 4]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn set_num_cells(&mut self, num_cells: usize) {
        self.0[NUM_CELLS_OFFSET..NUM_CELLS_OFFSET + 4]
            .swap_with_slice(&mut (num_cells as u32).to_ne_bytes());
    }

    pub fn rightmost_child(&self) -> usize {
        u32::from_ne_bytes(
            self.0[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 4]
                .try_into()
                .unwrap(),
        ) as usize
    }

    pub fn set_rightmost_child(&mut self, rightmost_child: usize) {
        self.0[RIGHTMOST_CHILD_OFFSET..RIGHTMOST_CHILD_OFFSET + 4]
            .swap_with_slice(&mut (rightmost_child as u32).to_ne_bytes());
    }

    pub fn set_internal_child(&mut self, slot: usize, key: usize, left: Offset, right: Offset) {
        let child_left = INTERNAL_CHILDREN_OFFSET + (slot * INTERNAL_CHILD_SIZE);
        let child_key = child_left + 4;
        let child_right = child_key + 4;
        self.0[child_left..child_left + 4].swap_with_slice(&mut (left.0 as u32).to_ne_bytes());
        self.0[child_key..child_key + 4].swap_with_slice(&mut (key as u32).to_ne_bytes());
        self.0[child_right..child_right + 4].swap_with_slice(&mut (right.0 as u32).to_ne_bytes());
    }

    pub fn set_cell(&mut self, cell_num: usize, key: usize, value: &Row) {
        let cell_key = CELL_OFFSET + (cell_num * CELL_SIZE);
        let cell_val = cell_key + CELL_KEY_SIZE;
        self.0[cell_key..cell_key + 4].swap_with_slice(&mut (key as u32).to_ne_bytes());
        self.0[cell_val..cell_val + CELL_VALUE_SIZE].swap_with_slice(&mut *value.serialize());
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Page (\n\t0: [*OMITTED*]\n)")
    }
}

impl From<&Page> for Node<usize, Row> {
    fn from(value: &Page) -> Self {
        let mut node = if value.0[NODE_TYPE_OFFSET] == 0 {
            Node::leaf()
        } else {
            Node::internal()
        };
        node.is_root = value.is_root_node();
        if !node.is_root {
            node.parent_offset = value.parent_offset();
        }
        node.num_cells = value.num_cells();

        match node.node_type {
            NodeType::Leaf(LeafNode {
                ref mut children, ..
            }) => {
                for i in 0..12 as usize {
                    if i == node.num_cells {
                        break;
                    }
                    let cell_key = CELL_OFFSET + (i * CELL_SIZE);
                    let cell_val = cell_key + CELL_KEY_SIZE;
                    let key =
                        u32::from_ne_bytes(value.0[cell_key..cell_key + 4].try_into().unwrap())
                            as usize;
                    let value = Row::deserialize(&value.0[cell_val..cell_val + CELL_VALUE_SIZE]);
                    children.push(KeyValuePair { key, value });
                }
            }
            NodeType::Internal(InternalNode {
                ref mut separators,
                ref mut children,
            }) => {
                let rightmost = value.rightmost_child();
                for slot in 0..rightmost {
                    let child_left = INTERNAL_CHILDREN_OFFSET + (slot * INTERNAL_CHILD_SIZE);
                    let child_key = child_left + 4;
                    let child_right = child_key + 4;

                    let left =
                        u32::from_ne_bytes(value.0[child_left..child_left + 4].try_into().unwrap())
                            as usize;

                    let right = u32::from_ne_bytes(
                        value.0[child_right..child_right + 4].try_into().unwrap(),
                    ) as usize;

                    separators.push(child_key);
                    children.insert(slot, Offset(left));
                    children.insert(slot + 1, Offset(right));
                }
            }
        }

        node
    }
}

impl From<Node<usize, Row>> for Page {
    fn from(n: Node<usize, Row>) -> Self {
        Page::from(&n)
    }
}

impl From<&Node<usize, Row>> for Page {
    fn from(value: &Node<usize, Row>) -> Self {
        let mut page = Page::new();
        page.set_root_node(value.is_root);
        page.set_parent_offset(value.parent_offset);
        page.set_num_cells(value.num_cells);

        match value.node_type {
            NodeType::Leaf(LeafNode { ref children, .. }) => {
                page.0[NODE_TYPE_OFFSET] = 0;
                let mut i = 0;
                for KeyValuePair { key, value } in children {
                    page.set_cell(i, *key, value);
                    i += 1;
                }
            }
            NodeType::Internal(InternalNode {
                ref separators,
                ref children,
            }) => {
                page.0[NODE_TYPE_OFFSET] = 1;
                page.set_rightmost_child(children.len() - 1);
                for (slot, ((&key, left), right)) in separators
                    .iter()
                    .zip(children.iter())
                    .zip(children.iter().skip(1))
                    .enumerate()
                {
                    page.set_internal_child(slot, key, left.clone(), right.clone())
                }
            }
        }

        page
    }
}
