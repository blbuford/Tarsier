use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::Path;
use std::process::exit;

use crate::node::Node;
use crate::page::{Page, PAGE_SIZE};
use crate::Row;

#[derive(Debug, Eq, Hash, PartialEq, PartialOrd, Ord, Copy, Clone)]
pub struct Offset(pub usize);

impl Display for Offset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Offset({})", self.0)
    }
}

#[derive(Debug)]
pub struct Pager<K, V> {
    file: RefCell<File>,
    num_pages: Cell<usize>,
    cache: HashMap<Offset, Node<K, V>>,
    free_pages: RefCell<BinaryHeap<Reverse<Offset>>>,
}

impl<K, V> Pager<K, V> {
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
                    cache: HashMap::new(),
                    free_pages: RefCell::new(BinaryHeap::new()),
                }
            }
            Err(why) => {
                println!("Unable to open file: {why}");
                exit(-1);
            }
        }
    }

    pub fn new_page(&self) -> Offset {
        return if let Ok(mut pq) = self.free_pages.try_borrow_mut() {
            if pq.is_empty() {
                self.num_pages.set(self.num_pages.get() + 1);
                Offset(self.num_pages.get())
            } else {
                pq.pop().unwrap().0
            }
        } else {
            self.num_pages.set(self.num_pages.get() + 1);
            Offset(self.num_pages.get())
        };
    }

    pub fn recycle(&mut self, offset: Offset) {
        self.cache.remove(&offset);
        self.free_pages.borrow_mut().push(Reverse(offset));
    }

    pub fn get(&self, page: &Offset) -> &Node<K, V> {
        match self.cache.get(page) {
            Some(node) => node,
            // TODO: Make this return an option, thus negating the need to panic
            None => panic!("Fetched a non-existent page!"),
        }
    }
    pub fn get_mut(&mut self, page: &Offset) -> &mut Node<K, V> {
        match self.cache.get_mut(page) {
            Some(node) => node,
            // TODO: Make this return an option, thus negating the need to panic
            None => panic!("Fetched a non-existent page!"),
        }
    }
    //     pub fn create_node(&mut self, page: &Offset) -> &Node<K, V> {
    //     if self.cache.get(page).is_none() {
    //         if page.0 < self.num_pages.get() {
    //             self.file
    //                 .borrow_mut()
    //                 .seek(SeekFrom::Start((page.0 * PAGE_SIZE) as u64))
    //                 .expect("Unable to seek to location in file.");
    //             let mut page_raw = Box::new([0 as u8; PAGE_SIZE]);
    //             match self.file.borrow_mut().read(page_raw.as_mut()) {
    //                 Ok(_bytes_read) => self
    //                     .cache
    //                     .insert(page.clone(), Page::load(page_raw)),
    //                 Err(why) => {
    //                     println!("Unable to read file: {why}");
    //                     exit(-1);
    //                 }
    //             };
    //         } else {
    //             self.cache.borrow_mut().insert(page.clone(), Page::new());
    //             self.num_pages.set(self.num_pages.get() + 1);
    //         }
    //     }
    //
    //     let mut node = Node::try_from(self.cache.borrow().get(&page).unwrap()).unwrap();
    //     node.offset = page.clone();
    //     node
    // }

    pub fn commit(&mut self, offset: Offset, node: Node<K, V>) {
        if let Some(_old_node) = self.cache.insert(offset, node) {
            panic!("You just committed over an existing node. Probable corruption!")
        }
    }

    pub fn close(&mut self) {
        for i in 0..self.num_pages.get() {
            let offset = Offset(i);
            let page = self.cache.get(&offset);
            self.file
                .borrow_mut()
                .seek(SeekFrom::Start(0))
                .expect("Seeking start of the file");
            match page
                .map(|node| Page::try_from(node))
                .map(|page| page.map(|p| p.write(self.file.borrow_mut().deref())))
            {
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

pub trait HasOffset {
    fn offset(&self) -> Offset;
}
