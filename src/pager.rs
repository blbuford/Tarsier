use std::cell::{Cell, RefCell};
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap};
use std::fmt::{Debug, Display, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::Deref;
use std::path::Path;
use std::process::exit;
use std::rc::Rc;

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

type Pageable<T> = Rc<RefCell<T>>;
#[derive(Debug)]
pub struct Pager<T> {
    file: RefCell<File>,
    num_pages: Cell<usize>,
    cache: HashMap<Offset, Pageable<T>>,
    free_pages: RefCell<BinaryHeap<Reverse<Offset>>>,
}

impl<T> Pager<T> {
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

    /// Returns the offset of the next free page to be used
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

    /// Moves an entry to a different offset in cache
    pub fn move_entry(&mut self, old: &Offset, new: Offset) {
        let entry = self.cache.remove(old);
        entry.map(|e| self.cache.insert(new, e)).flatten();
    }

    /// Removes an item from the cache, if successfully then adds to the free pages
    pub fn recycle(&mut self, offset: &Offset) -> Option<Pageable<T>>
    where
        T: Debug,
    {
        let r = self.cache.remove(offset);
        if r.is_some() {
            self.free_pages.borrow_mut().push(Reverse(offset.clone()));
        }
        r
    }

    pub fn get(&self, page: &Offset) -> Pageable<T> {
        match self.cache.get(page) {
            Some(node) => node.clone(),
            // TODO: Make this return an option, thus negating the need to panic
            None => panic!("Fetched a non-existent page!"),
        }
    }

    /// given an offset, retrieve that page from the disk, and put the node in the cache.
    pub fn fetch_page(&mut self, page: &Offset)
    where
        T: From<Page>,
    {
        if self.cache.get(page).is_none() {
            if page.0 < self.num_pages.get() {
                self.file
                    .borrow_mut()
                    .seek(SeekFrom::Start((page.0 * PAGE_SIZE) as u64))
                    .expect("Unable to seek to location in file.");
                let mut page_raw = Box::new([0 as u8; PAGE_SIZE]);
                match self.file.borrow_mut().read(page_raw.as_mut()) {
                    Ok(_bytes_read) => self.cache.insert(
                        page.clone(),
                        Rc::new(RefCell::new(T::from(Page::load(page_raw)))),
                    ),
                    Err(why) => {
                        println!("Unable to read file: {why}");
                        exit(-1);
                    }
                };
            } else {
                panic!(
                    "tried to fetch a page greater than the number of pages thought to be on disk!"
                )
            }
        }
    }

    /// commits a new node to the cache to be fsync'd at some point in time... later. Raises the `num_pages` where applicable.
    pub fn commit(&mut self, node: T)
    where
        T: HasOffset,
    {
        if node.offset().0 > self.num_pages() {
            assert_eq!(self.num_pages() + 1, node.offset().0);
            self.num_pages.set(node.offset().0);
        }
        if let Ok(mut pq) = self.free_pages.try_borrow_mut() {
            if !pq.is_empty() && pq.peek().unwrap().0 == node.offset() {
                pq.pop();
            }
        }
        if let Some(_old_node) = self
            .cache
            .insert(node.offset(), Rc::new(RefCell::new(node)))
        {
            panic!("You just committed over an existing node. Probable corruption!")
        }
    }

    /// forcible fsync of all pages and drop them from the cache.
    pub fn close(&mut self)
    where
        Page: From<T>,
        T: Debug,
    {
        for i in 0..self.num_pages.get() {
            let offset = Offset(i);
            let entry = self
                .cache
                .remove(&offset)
                .map(|e| Rc::try_unwrap(e).unwrap().into_inner());

            self.file
                .borrow_mut()
                .seek(SeekFrom::Start(0))
                .expect("Seeking start of the file");
            match entry
                .map(|node| Page::from(node))
                .map(|page| page.write(self.file.borrow_mut().deref()))
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
