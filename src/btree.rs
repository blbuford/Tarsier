use crate::cursor::Cursor;
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

pub struct KeyValuePair<K, V> {
    pub key: K,
    pub value: V,
}

pub enum NodeType<K, V> {
    Internal,
    Leaf(Vec<KeyValuePair<K, V>>),
}

pub struct BTree {
    root: Node<usize, Row>,
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

        Self { root, pager }
    }

    pub fn get(&mut self, page_num: usize, cell_num: usize) -> Row {
        let node = self.pager.get_page(page_num);
        node.get(cell_num).as_ref().unwrap().value.clone()
    }

    pub fn insert(&mut self, page_num: usize, cell_num: usize, value: Row) -> bool {
        let mut node = self.pager.get_page(page_num);
        if node.insert(cell_num, cell_num, value) {
            self.pager.commit_page(&node);
            if node.is_root {
                self.root = node;
            }
            return true;
        }
        return false;
    }

    pub fn root(&self) -> &Node<usize, Row> {
        &self.root
    }

    pub fn close(&mut self) {
        self.pager.close()
    }
}
pub struct Node<K, V> {
    pub(crate) is_root: bool,
    pub(crate) node_type: NodeType<K, V>,
    pub(crate) parent_offset: Option<usize>,
    pub(crate) num_cells: usize,
    pub(crate) page_num: usize,
}

impl<K, V> Node<K, V> {
    pub fn new() -> Self {
        Self {
            is_root: false,
            node_type: NodeType::Leaf(Vec::new()),
            parent_offset: None,
            num_cells: 0,
            page_num: 0,
        }
    }

    pub fn get(&self, cell_num: usize) -> Option<&KeyValuePair<K, V>> {
        if let NodeType::Leaf(ref cells) = self.node_type {
            return cells.get(cell_num);
        }
        None
    }

    pub fn insert(&mut self, cell_num: usize, key: K, value: V) -> bool {
        if let NodeType::Leaf(ref mut cells) = self.node_type {
            if cells.len() >= 12 {
                return false;
            }
            cells.insert(cell_num, KeyValuePair { key, value });
            self.num_cells += 1;
            return true;
        }

        false
    }
}
//
// struct Page {
//     data: Box<[u8; NODE_SIZE]>,
// }
//
// impl Page {
//     pub fn is_root_node(&self) -> bool {
//         self.data[IS_ROOT_OFFSET] == 1
//     }
//     pub fn parent_offset(&self) -> Option<usize> {
//         Some(u32::from_ne_bytes(
//             self.data[PARENT_OFFSET..PARENT_OFFSET + 4]
//                 .try_into()
//                 .unwrap(),
//         ) as usize)
//     }
//     pub fn num_cells(&self) -> usize {
//         u32::from_ne_bytes(
//             self.data[NUM_CELLS_OFFSET..NUM_CELLS_OFFSET + 4]
//                 .try_into()
//                 .unwrap(),
//         ) as usize
//     }
// }
//
// impl TryFrom<Page> for Node<usize, Row> {
//     type Error = ();
//
//     fn try_from(value: Page) -> Result<Self, Self::Error> {
//         let mut node = Node::new();
//         node.is_root = value.is_root_node();
//         if !node.is_root {
//             node.parent_offset = value.parent_offset();
//         }
//         node.num_cells = value.num_cells();
//
//         match node.node_type {
//             NodeType::Leaf(ref mut cells) => {
//                 for i in 0..12 as usize {
//                     if i == node.num_cells {
//                         break;
//                     }
//                     let cell_key = CELL_OFFSET + (i * CELL_SIZE);
//                     let cell_val = cell_key + CELL_KEY_SIZE;
//                     let key =
//                         u32::from_ne_bytes(value.data[cell_key..cell_key + 4].try_into().unwrap())
//                             as usize;
//                     let value = Row::deserialize(&value.data[cell_val..cell_val + CELL_VALUE_SIZE]);
//                     cells.push(KeyValuePair { key, value })
//                 }
//             }
//             _ => todo!(),
//         }
//
//         Ok(node)
//     }
// }
