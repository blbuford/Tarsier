use crate::btree::BTree;
use crate::Row;

pub struct Cursor {
    page_num: usize,
    cell_num: usize,
    end_of_table: bool,
}

impl Cursor {
    pub fn start(tree: &BTree) -> Self {
        let root = tree.root();
        let page_num = root.page_num;
        let end_of_table = root.num_cells == 0;
        Self {
            page_num,
            cell_num: 0,
            end_of_table,
        }
    }

    pub fn end(tree: &BTree) -> Self {
        let root = tree.root();
        let page_num = root.page_num;
        let cell_num = root.num_cells;
        Self {
            page_num,
            cell_num,
            end_of_table: true,
        }
    }

    pub fn new(page_num: usize, cell_num: usize, end_of_table: bool) -> Self {
        Self {
            page_num,
            cell_num,
            end_of_table,
        }
    }

    pub fn page_num(&self) -> usize {
        self.page_num
    }
    pub fn cell_num(&self) -> usize {
        self.cell_num
    }

    pub fn value(&self, tree: &BTree) -> Row {
        tree.get(self.page_num, self.cell_num)
    }

    // pub fn insert_at(&mut self, row: Row) -> bool {
    //     self.tree.insert(self.page_num, self.cell_num, row)
    // }

    pub fn advance(&mut self, tree: &BTree) {
        self.cell_num += 1;
        let node = tree.root();
        if self.cell_num >= node.num_cells {
            self.end_of_table = true;
        }
    }

    pub fn is_at_end_of_table(&self) -> bool {
        self.end_of_table
    }
}
