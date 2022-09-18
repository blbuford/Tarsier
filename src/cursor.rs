use crate::btree::BTree;
use crate::pager::Offset;
use crate::Row;

#[derive(Debug)]
pub struct Cursor {
    pub offset: Offset,
    pub cell_num: usize,
    pub end_of_table: bool,
}

impl Cursor {
    pub fn start(tree: &BTree) -> Self {
        let offset = tree.root();
        let end_of_table = tree.is_empty();
        Self {
            offset,
            cell_num: 0,
            end_of_table,
        }
    }

    pub fn new(offset: Offset, cell_num: usize, end_of_table: bool) -> Self {
        Self {
            offset,
            cell_num,
            end_of_table,
        }
    }

    pub fn offset(&self) -> &Offset {
        &self.offset
    }
    pub fn increment_cell_num(&mut self) {
        self.cell_num += 1;
    }
    pub fn cell_num(&self) -> usize {
        self.cell_num
    }

    pub fn value(&self, tree: &BTree) -> Row {
        tree.get(&self.offset, self.cell_num)
    }

    pub fn is_at_end_of_table(&self) -> bool {
        self.end_of_table
    }
}
