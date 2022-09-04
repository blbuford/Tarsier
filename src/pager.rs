use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::Path;
use std::process::exit;

use crate::btree::{
    CELL_KEY_SIZE, CELL_OFFSET, CELL_SIZE, CELL_VALUE_SIZE, IS_ROOT_OFFSET, NUM_CELLS_OFFSET,
    PARENT_OFFSET,
};
use crate::datastore::ROW_SIZE;
use crate::node::Node;
use crate::node_type::{Child, NodeType};
use crate::Row;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;
pub const RIGHTMOST_CHILD_OFFSET: usize = 10;
pub const INTERNAL_CHILDREN_OFFSET: usize = RIGHTMOST_CHILD_OFFSET + 4;
pub const INTERNAL_CHILD_SIZE: usize = 12;

#[derive(Debug)]
pub struct Pager {
    file: RefCell<File>,
    num_pages: Cell<usize>,
    page_cache: RefCell<HashMap<usize, Page>>,
}

impl Pager {
    pub fn open(filename: impl AsRef<Path>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        match file {
            Ok(file) => {
                let file_length = file.metadata().expect("Metadata for DB open").len();
                if file_length % PAGE_SIZE as u64 != 0 {
                    println!("DB file is not a whole number of pages. CORRUPT FILE.");
                    panic!();
                }
                let num_pages = Cell::new(file_length as usize / PAGE_SIZE);
                Self {
                    file: RefCell::new(file),
                    num_pages,
                    page_cache: RefCell::new(HashMap::new()),
                }
            }
            Err(why) => {
                println!("Unable to open file: {why}");
                exit(-1);
            }
        }
    }

    pub fn get_page(&self, page_num: usize) -> Node<usize, Row> {
        if self.page_cache.borrow().get(&page_num).is_none() {
            if page_num < self.num_pages.get() {
                self.file
                    .borrow_mut()
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .expect("Unable to seek to location in file.");
                let mut page_raw = Box::new([0 as u8; PAGE_SIZE]);
                match self.file.borrow_mut().read(page_raw.as_mut()) {
                    Ok(_bytes_read) => self
                        .page_cache
                        .borrow_mut()
                        .insert(page_num, Page::load(page_raw)),
                    Err(why) => {
                        println!("Unable to read file: {why}");
                        exit(-1);
                    }
                };
            } else {
                self.page_cache.borrow_mut().insert(page_num, Page::new());
                self.num_pages.set(self.num_pages.get() + 1);
            }
        }

        let mut node = Node::try_from(self.page_cache.borrow().get(&page_num).unwrap()).unwrap();
        node.page_num = page_num;
        node
    }

    pub fn commit_page(&mut self, n: &Node<usize, Row>) {
        match Page::try_from(n) {
            Ok(new_page) => {
                if n.page_num > self.num_pages.get() {
                    self.num_pages.set(n.page_num + 1);
                }
                self.page_cache.borrow_mut().insert(n.page_num, new_page);
            }
            Err(_) => {
                println!("Unable to commit page {}", n.page_num);
                exit(-1);
            }
        }
    }

    pub fn close(&mut self) {
        for i in 0..self.num_pages.get() {
            let map = self.page_cache.get_mut();
            let page = map.get_mut(&i);
            self.file
                .borrow_mut()
                .seek(SeekFrom::Start(0))
                .expect("Seeking start of the file");
            match page.map(|page| page.write(self.file.borrow_mut().deref())) {
                Some(Ok(bytes_written)) => {
                    if i < self.num_pages.get() - 1 {
                        self.file
                            .borrow_mut()
                            .seek(SeekFrom::Current(
                                (PAGE_SIZE as usize - bytes_written) as i64,
                            ))
                            .expect("seeking up to the next page offset");
                    }
                }
                Some(Err(why)) => {
                    println!("Unable to write page to file because: {why}");
                    exit(-1);
                }
                None => {
                    self.file
                        .borrow_mut()
                        .seek(SeekFrom::Current(PAGE_SIZE as i64))
                        .expect("Page size seek forward");
                }
            }
        }
        self.file
            .borrow_mut()
            .flush()
            .expect("Flushing writes to file")
    }

    pub fn num_pages(&self) -> usize {
        self.num_pages.get()
    }
}

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

    pub fn parent_offset(&self) -> Option<usize> {
        Some(
            u32::from_ne_bytes(self.0[PARENT_OFFSET..PARENT_OFFSET + 4].try_into().unwrap())
                as usize,
        )
    }

    pub fn set_parent_offset(&mut self, parent_offset: Option<usize>) {
        if let Some(offset) = parent_offset {
            self.0[PARENT_OFFSET..PARENT_OFFSET + 4]
                .swap_with_slice(&mut (offset as u32).to_ne_bytes())
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

    pub fn set_internal_child(&mut self, child: &Child<usize>, slot: usize) {
        let child_left = INTERNAL_CHILDREN_OFFSET + (slot * INTERNAL_CHILD_SIZE);
        let child_key = child_left + 4;
        let child_right = child_key + 4;
        self.0[child_left..child_left + 4].swap_with_slice(
            &mut (child
                .left()
                .as_ref()
                .unwrap_with_or(|n| n.page_num.clone(), u32::MAX as usize)
                as u32)
                .to_ne_bytes(),
        );
        self.0[child_key..child_key + 4].swap_with_slice(&mut (*child.key() as u32).to_ne_bytes());
        self.0[child_right..child_right + 4].swap_with_slice(
            &mut (child
                .right()
                .as_ref()
                .unwrap_with_or(|n| n.page_num.clone(), u32::MAX as usize)
                as u32)
                .to_ne_bytes(),
        );
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

impl TryFrom<&Page> for Node<usize, Row> {
    type Error = ();

    fn try_from(value: &Page) -> Result<Self, Self::Error> {
        let mut node = Node::leaf();
        node.is_root = value.is_root_node();
        if !node.is_root {
            node.parent_offset = value.parent_offset();
        }
        node.num_cells = value.num_cells();

        match node.node_type {
            NodeType::Leaf(ref mut cells, ..) => {
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
                    cells.insert(key, value);
                }
            }
            NodeType::Internal(ref mut children) => {
                let rightmost = value.rightmost_child();
                for slot in 0..=rightmost {
                    let child_left = INTERNAL_CHILDREN_OFFSET + (slot * INTERNAL_CHILD_SIZE);
                    let child_key = child_left + 4;
                    let child_right = child_key + 4;

                    let left =
                        u32::from_ne_bytes(value.0[child_left..child_left + 4].try_into().unwrap())
                            as usize;

                    let right = u32::from_ne_bytes(
                        value.0[child_right..child_right + 4].try_into().unwrap(),
                    ) as usize;

                    let child = Child::new(
                        u32::from_ne_bytes(value.0[child_key..child_key + 4].try_into().unwrap())
                            as usize,
                        left,
                        right,
                    );
                    children.push(child);
                }
            }
        }

        Ok(node)
    }
}

impl TryFrom<&Node<usize, Row>> for Page {
    type Error = ();

    fn try_from(value: &Node<usize, Row>) -> Result<Self, Self::Error> {
        let mut page = Page::new();
        page.set_root_node(value.is_root);
        page.set_parent_offset(value.parent_offset);
        page.set_num_cells(value.num_cells);

        match value.node_type {
            NodeType::Leaf(ref cells, ..) => {
                let mut i = 0;
                for (&key, value) in cells {
                    page.set_cell(i, key, value);
                    i += 1;
                }
            }
            NodeType::Internal(ref children) => {
                page.set_rightmost_child(children.len() - 1);
                for (slot, child) in children.iter().enumerate() {
                    page.set_internal_child(child, slot)
                }
            }
        }

        Ok(page)
    }
}
