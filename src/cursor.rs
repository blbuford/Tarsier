use crate::datastore::ROWS_PER_PAGE;
use crate::pager::Page;
use crate::{Row, Table};

pub struct Cursor<'a> {
    table: &'a mut Table,
    row_num: usize,
    end_of_table: bool,
}

impl<'a> Cursor<'a> {
    pub fn start(table: &'a mut Table) -> Self {
        let end_of_table = table.num_rows() == 0;
        Self {
            table,
            row_num: 0,
            end_of_table,
        }
    }

    pub fn end(table: &'a mut Table) -> Self {
        let row_num = table.num_rows();
        Self {
            table,
            row_num,
            end_of_table: true,
        }
    }

    pub fn row_num(&self) -> usize {
        self.row_num
    }

    pub fn value(&mut self) -> &mut Page {
        let row_num = self.row_num;
        let page_num = row_num / ROWS_PER_PAGE;
        self.table.get_page(page_num)
    }

    pub fn advance(&mut self) {
        self.row_num += 1;
        if self.row_num >= self.table.num_rows() {
            self.end_of_table = true;
        }
    }

    pub fn is_at_end_of_table(&self) -> bool {
        self.end_of_table
    }
}
