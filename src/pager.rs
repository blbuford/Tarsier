use crate::btree::{
    KeyValuePair, Node, NodeType, CELL_KEY_SIZE, CELL_OFFSET, CELL_SIZE, CELL_VALUE_SIZE,
    IS_ROOT_OFFSET, NUM_CELLS_OFFSET, PARENT_OFFSET,
};
use crate::datastore::ROW_SIZE;
use crate::Row;
use std::borrow::BorrowMut;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

pub const PAGE_SIZE: usize = 4096;
pub const TABLE_MAX_PAGES: usize = 100;

#[derive(Debug)]
pub struct Pager {
    file: File,
    file_length: u64,
    pages: Vec<Option<Page>>,
}

impl Pager {
    pub fn open(filename: impl AsRef<Path>) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(filename);
        let mut pages: Vec<Option<Page>> = Vec::new();

        match file {
            Ok(file) => {
                let file_length = file.metadata().expect("Metadata for DB open").len();
                if file_length % PAGE_SIZE as u64 != 0 {
                    println!("DB file is not a whole number of pages. CORRUPT FILE.");
                    panic!();
                }
                pages.resize_with(file_length as usize / PAGE_SIZE, || None);
                Self {
                    file,
                    file_length,
                    pages,
                }
            }
            Err(why) => {
                println!("Unable to open file: {why}");
                exit(-1);
            }
        }
    }

    pub fn get_page(&mut self, page_num: usize) -> Node<usize, Row> {
        if page_num >= self.pages.len() {
            self.pages.push(None);
        }
        if self.pages[page_num].is_none() {
            let mut num_pages = self.file_length / PAGE_SIZE as u64;
            if self.file_length % (PAGE_SIZE as u64) != 0 {
                num_pages += 1;
            }

            if (page_num as u64) < num_pages {
                self.file
                    .seek(SeekFrom::Start((page_num * PAGE_SIZE) as u64))
                    .expect("Unable to seek to location in file.");
                let mut page_raw = Box::new([0 as u8; PAGE_SIZE]);
                match self.file.read(page_raw.as_mut()) {
                    Ok(_bytes_read) => self.pages[page_num] = Some(Page::load(page_raw)),
                    Err(why) => {
                        println!("Unable to read file: {why}");
                        exit(-1);
                    }
                };
            } else {
                self.pages[page_num] = Some(Page::new());
            }
        }

        let mut node = Node::try_from(self.pages[page_num].as_ref().unwrap()).unwrap();
        node.page_num = page_num;
        node
    }

    pub fn commit_page(&mut self, n: &Node<usize, Row>) {
        if n.page_num < self.pages.len() {
            let new_page = Page::try_from(n).unwrap();
            self.pages[n.page_num] = Some(new_page);
        }
    }

    pub fn close(&mut self) {
        for i in 0..self.pages.len() {
            let page = self.pages[i].as_mut();
            self.file
                .seek(SeekFrom::Start(0))
                .expect("Seeking start of the file");
            match page.map(|page| page.write(self.file.borrow_mut())) {
                Some(Ok(bytes_written)) => {
                    if i < self.pages.len() - 1 {
                        self.file
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
                        .seek(SeekFrom::Current(PAGE_SIZE as i64))
                        .expect("Page size seek forward");
                }
            }
        }
        self.file.flush().expect("Flushing writes to file")
    }

    pub fn file_length(&self) -> usize {
        self.file_length as usize
    }

    pub fn num_pages(&self) -> usize {
        self.pages.len()
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
        let mut node = Node::new();
        node.is_root = value.is_root_node();
        if !node.is_root {
            node.parent_offset = value.parent_offset();
        }
        node.num_cells = value.num_cells();

        match node.node_type {
            NodeType::Leaf(ref mut cells) => {
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
                    cells.push(KeyValuePair { key, value })
                }
            }
            _ => todo!(),
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
            NodeType::Leaf(ref cells) => {
                let mut i = 0;
                for KeyValuePair { key, value } in cells {
                    page.set_cell(i, *key, value);
                    i += 1;
                }
            }
            _ => todo!(),
        }

        Ok(page)
    }
}
