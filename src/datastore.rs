use std::fmt::Formatter;
use std::path::Path;

use crate::btree::BTree;
use crate::cursor::Cursor;
use crate::pager::{Pager, PAGE_SIZE, TABLE_MAX_PAGES};
use crate::{Statement, StatementType};

pub const ROW_SIZE: usize = 291;
pub const ROWS_PER_PAGE: usize = PAGE_SIZE as usize / ROW_SIZE;
pub const TABLE_MAX_ROWS: usize = ROWS_PER_PAGE * TABLE_MAX_PAGES;

#[derive(Debug, PartialEq)]
pub enum ExecuteResult {
    InsertSuccess,
    SelectSuccess(Vec<Row>),
    TableFull,
    DuplicateKey,
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

pub struct Table {
    root_page_num: usize,
    btree: BTree,
}

impl Table {
    pub fn open(filename: impl AsRef<Path>) -> Self {
        let pager = Pager::open(filename);
        let btree = BTree::new(pager);

        Table {
            root_page_num: 0,
            btree,
        }
    }

    pub fn execute_statement(&mut self, stmt: Statement) -> ExecuteResult {
        match stmt.statement_type {
            StatementType::Insert => self.execute_insert(stmt.row_to_insert.unwrap()),
            StatementType::Select => self.execute_select(),
        }
    }

    pub fn close(&mut self) {
        self.btree.close()
    }

    pub fn root_page_num(&self) -> usize {
        self.root_page_num
    }

    pub fn find(&self, key: usize) -> Result<Cursor, Cursor> {
        self.btree.find(key)
    }

    pub fn get_root_page_num(&self) -> usize {
        self.root_page_num
    }
    fn execute_insert(&mut self, row: Row) -> ExecuteResult {
        match self.find(row.id as usize) {
            Ok(_duplicate_location) => ExecuteResult::DuplicateKey,
            Err(cursor) => {
                if cursor.page_num() == usize::MAX {
                    return ExecuteResult::TableFull;
                }
                if !self.btree.insert(&cursor, row) {
                    return ExecuteResult::TableFull;
                }
                ExecuteResult::InsertSuccess
            }
        }
    }

    fn execute_select(&self) -> ExecuteResult {
        let mut rows = Vec::new();
        let mut cursor = Cursor::start(&self.btree);
        while !cursor.is_at_end_of_table() {
            let row = cursor.value(&self.btree);
            rows.push(row.clone());
            cursor.advance(&self.btree);
        }
        ExecuteResult::SelectSuccess(rows)
    }
}

#[cfg(test)]
mod tests {
    use std::fs::OpenOptions;

    use crate::datastore::TABLE_MAX_ROWS;
    use crate::pager::Page;
    use crate::{ExecuteResult, Row, Statement, StatementType, Table};

    fn open_test_db() -> Table {
        let test_db = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open("test.db")
            .expect("test database");
        test_db.sync_all().expect("sync changes to disk");
        Table::open("test.db")
    }

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
        let mut table = open_test_db();
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
    fn table_insert_duplicate_keys_throw_error() {
        let mut table = open_test_db();
        let row = Row {
            id: 0,
            username: String::from("bbuford"),
            email: String::from("bbuford@example.com"),
        };

        let statement = Statement {
            statement_type: StatementType::Insert,
            row_to_insert: Some(row.clone()),
        };

        assert_eq!(
            table.execute_statement(statement),
            ExecuteResult::InsertSuccess
        );
        let statement = Statement {
            statement_type: StatementType::Insert,
            row_to_insert: Some(row.clone()),
        };
        assert_eq!(
            table.execute_statement(statement),
            ExecuteResult::DuplicateKey
        );
    }

    #[test]
    fn table_insert_max_rows() {
        let mut table = open_test_db();
        for i in 0..12 {
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
