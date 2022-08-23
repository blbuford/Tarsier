use crate::datastore::ROW_SIZE;
use crate::Row;
use std::borrow::BorrowMut;
use std::fmt::{Debug, Formatter};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::process::exit;

pub const PAGE_SIZE: u64 = 4096;
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
        pages.resize_with(TABLE_MAX_PAGES, || None);
        match file {
            Ok(file) => {
                let file_length = file.metadata().expect("Metadata for DB open").len();
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

    pub fn get_page(&mut self, page_num: usize) -> &mut Page {
        assert!(page_num <= TABLE_MAX_PAGES);
        if self.pages[page_num].is_none() {
            let mut num_pages = self.file_length / PAGE_SIZE;
            if self.file_length % PAGE_SIZE != 0 {
                num_pages += 1;
            }

            if (page_num as u64) < num_pages {
                self.file
                    .seek(SeekFrom::Start(page_num as u64 * PAGE_SIZE))
                    .expect("Unable to seek to location in file.");
                let mut page_raw = vec![0 as u8; PAGE_SIZE as usize].into_boxed_slice();
                match self.file.read(&mut page_raw) {
                    Ok(bytes_read) => self.pages[page_num] = Some(Page::load(page_raw, bytes_read)),
                    Err(why) => {
                        println!("Unable to read file: {why}");
                        exit(-1);
                    }
                };
            } else {
                self.pages[page_num] = Some(Page::new());
            }
        }

        self.pages[page_num].as_mut().unwrap()
    }

    pub fn close(&mut self) {
        for i in 0..self.pages.len() {
            let page = dbg!(self.pages[i].as_mut());
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
}

pub struct Page {
    length: usize,
    data: Box<Box<[u8]>>,
}
impl Page {
    pub fn new() -> Self {
        Self {
            length: 0,
            data: Box::new(vec![0 as u8; PAGE_SIZE as usize].into_boxed_slice()),
        }
    }

    pub fn load(p: Box<[u8]>, length: usize) -> Self {
        Self {
            length,
            data: Box::new(p),
        }
    }

    pub fn insert(&mut self, row: Row, slot: usize) {
        let min = slot * ROW_SIZE;
        let max = min + ROW_SIZE;
        self.data[min..max].swap_with_slice(&mut *row.serialize());
        if max > self.length {
            self.length = max;
        }
    }

    pub fn select(&self, slot: usize) -> Option<Row> {
        let min = slot * ROW_SIZE;
        let max = min + ROW_SIZE;
        if max <= self.length {
            Some(Row::deserialize(&self.data[min..max]))
        } else {
            None
        }
    }

    pub fn write(&self, mut writer: impl Write) -> std::io::Result<usize> {
        writer.write(&self.data[0..self.length])
    }
}

impl Debug for Page {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Page {{\n\tlength: {},\n\tdata: [*OMITTED*]\n}}",
            self.length
        )
    }
}
