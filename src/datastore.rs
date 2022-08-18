use crate::{Statement, StatementType};
use std::borrow::BorrowMut;
use std::fmt::Formatter;

const PAGE_SIZE: usize = 4096;
const TABLE_MAX_PAGES: usize = 100;
const ROW_SIZE: usize = 291;
const ROWS_PER_PAGE: usize = PAGE_SIZE / ROW_SIZE;
const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug, PartialEq)]
pub enum ExecuteResult {
    InsertSuccess,
    SelectSuccess(Vec<Row>),
    TableFull,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub id: u32,
    pub username: String,
    pub email: String,
}

impl std::fmt::Display for Row {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {} ({})", self.id, self.username, self.email)
    }
}

impl Row {
    pub fn serialize(&self) -> Box<[u8]> {
        let mut ser = Vec::new();
        ser.extend(self.id.to_ne_bytes());
        ser.extend(self.username.as_str().as_bytes());
        ser.resize(36, 0);
        ser.extend(self.email.as_str().as_bytes());
        ser.resize(291, 0);

        ser.into_boxed_slice()
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let (id_bytes, rest) = data.split_at(std::mem::size_of::<u32>());
        let id: u32 = u32::from_ne_bytes(id_bytes.try_into().unwrap());
        let (username_bytes, email) = rest.split_at(32);
        let mut username = std::str::from_utf8(username_bytes).unwrap().to_string();
        if let Some((u, _)) = username.split_once("\0") {
            username = u.to_string();
        }
        let mut email = std::str::from_utf8(email).unwrap().to_string();
        if let Some((e, _)) = email.split_once("\0") {
            email = e.to_string();
        }
        Self {
            id,
            username,
            email,
        }
    }
}
#[derive(Debug)]
pub struct Page(Box<Box<[u8]>>);
impl Page {
    pub fn new() -> Self {
        Self(Box::new(vec![0 as u8; PAGE_SIZE].into_boxed_slice()))
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
}

pub struct Table {
    num_rows: u32,
    pages: Vec<Page>,
}

impl Table {
    pub fn new() -> Self {
        Table {
            num_rows: 0,
            pages: Vec::with_capacity(TABLE_MAX_PAGES),
        }
    }
    pub fn execute_statement(&mut self, stmt: Statement) -> ExecuteResult {
        match stmt.statement_type {
            StatementType::Insert => self.execute_insert(stmt.row_to_insert.unwrap()),
            StatementType::Select => self.execute_select(),
        }
    }
    fn execute_insert(&mut self, row: Row) -> ExecuteResult {
        if self.num_rows as usize >= TABLE_MAX_ROWS {
            return ExecuteResult::TableFull;
        }

        let page_slot = self.num_rows as usize % ROWS_PER_PAGE;
        let mut page = self.row_slot(self.num_rows);
        page.insert(row, page_slot);
        self.num_rows += 1;
        ExecuteResult::InsertSuccess
    }

    fn execute_select(&mut self) -> ExecuteResult {
        let mut rows = Vec::with_capacity(self.num_rows as usize);
        for r in 0..self.num_rows as usize {
            let page = self.row_slot(r as u32);
            rows.push(page.select(r % ROWS_PER_PAGE));
        }
        ExecuteResult::SelectSuccess(rows)
    }

    fn row_slot(&mut self, row_num: u32) -> &mut Page {
        let page_num = row_num as usize / ROWS_PER_PAGE;
        assert!(page_num <= TABLE_MAX_PAGES);
        if page_num == self.pages.len() {
            self.pages.push(Page::new());
        }
        self.pages[page_num].borrow_mut()
    }
}

#[cfg(test)]
mod tests {
    use crate::datastore::{Page, TABLE_MAX_ROWS};
    use crate::{ExecuteResult, Row, Statement, StatementType, Table};

    #[test]
    fn serialize_tests() {
        let r = Row {
            id: 0,
            username: String::from("bbuford"),
            email: String::from("bbuford@example.com"),
        };
        let ser = r.serialize();
        let de = Row::deserialize(&*ser);
        assert_eq!(r.id, de.id);
        assert_eq!(r.username, de.username);
        assert_eq!(r.email, de.email);

        let r = Row {
            id: 0,
            username: String::from(""),
            email: String::from(""),
        };
        let ser = r.serialize();
        let de = Row::deserialize(&*ser);
        assert_eq!(r.id, de.id);
        assert_eq!(r.username, de.username);
        assert_eq!(r.email, de.email);

        let r = Row {
            id: 0,
            username: String::from("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"), // 40 char, should be truncated to 32
            email: String::from("bbuford@example.com"),
        };
        let ser = r.serialize();
        let de = Row::deserialize(&*ser);
        assert_eq!(r.id, de.id);
        assert_ne!(r.username, de.username);
        assert_eq!(
            de.username,
            String::from("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA") // 32 char truncation
        );
        assert_eq!(r.email, de.email);
    }

    #[test]
    fn page_insert_tests() {
        let mut p = Page::new();
        let r = Row {
            id: 0,
            username: String::from("bbuford"),
            email: String::from("bbuford@example.com"),
        };
        p.insert(r.clone(), 0);
        p.insert(r.clone(), 1);
        let sel = p.select(0);
        assert_eq!(r.id, sel.id);
        assert_eq!(r.username, sel.username);
        assert_eq!(r.email, sel.email);

        let sel = p.select(1);
        assert_eq!(r.id, sel.id);
        assert_eq!(r.username, sel.username);
        assert_eq!(r.email, sel.email);
    }

    #[test]
    fn table_insert_single_row() {
        let mut table = Table::new();
        let row = Row {
            id: 0,
            username: String::from("bbuford"),
            email: String::from("bbuford@example.com"),
        };

        let statement = Statement {
            statement_type: StatementType::Insert,
            row_to_insert: Some(row),
        };

        assert_eq!(
            table.execute_statement(statement),
            ExecuteResult::InsertSuccess
        );

        let statement = Statement {
            statement_type: StatementType::Select,
            row_to_insert: None,
        };

        let res = table.execute_statement(statement);
        assert!(matches!(res, ExecuteResult::SelectSuccess { .. }));
        match res {
            ExecuteResult::SelectSuccess(rows) => {
                assert_eq!(rows.len(), 1);
                let row = &rows[0];
                assert_eq!(row.id, 0);
                assert_eq!(row.username, String::from("bbuford"));
                assert_eq!(row.email, String::from("bbuford@example.com"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn table_insert_max_rows() {
        let mut table = Table::new();
        for i in 0..TABLE_MAX_ROWS {
            assert_eq!(
                table.execute_statement(Statement {
                    statement_type: StatementType::Insert,
                    row_to_insert: Some(Row {
                        id: i as u32,
                        username: String::from(format!("user{i}")),
                        email: String::from(format!("user{i}@example.com")),
                    }),
                }),
                ExecuteResult::InsertSuccess
            );
        }

        assert_eq!(
            table.execute_statement(Statement {
                statement_type: StatementType::Insert,
                row_to_insert: Some(Row {
                    id: TABLE_MAX_ROWS as u32,
                    username: String::from(format!("user{TABLE_MAX_ROWS}")),
                    email: String::from(format!("user{TABLE_MAX_ROWS}@example.com")),
                }),
            }),
            ExecuteResult::TableFull
        );
    }
}
