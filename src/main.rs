extern crate core;

mod cursor;
mod datastore;
mod pager;

use crate::datastore::{ExecuteResult, Row, Table};
use regex::Regex;
use std::io;
use std::io::Write;
use std::process::exit;

enum MetaCommand {
    Success,
    UnrecognizedCommand,
}
enum PrepareResult {
    Success(Statement),
    UnrecognizedStatement,
    SyntaxError,
    NegativeId,
    StringTooLong,
}
pub enum StatementType {
    Insert,
    Select,
}

pub struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

fn main() {
    let mut input = String::new();
    let mut table = Table::open("db.db");
    loop {
        print!("db> ");
        io::stdout().flush().unwrap();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.starts_with(".") {
                    match do_meta_command(&input, &mut table) {
                        MetaCommand::UnrecognizedCommand => {
                            input.pop();
                            println!("Unrecognized command: {}", input);
                        }
                        MetaCommand::Success => {}
                    }
                } else {
                    match prepare_statement(&input) {
                        PrepareResult::Success(stmt) => match table.execute_statement(stmt) {
                            ExecuteResult::InsertSuccess => println!("SUCCESS"),
                            ExecuteResult::SelectSuccess(results) => {
                                for row in results {
                                    println!("{}", row);
                                }
                            }
                            ExecuteResult::TableFull => println!("ERROR: TABLE IS FULL"),
                        },
                        PrepareResult::UnrecognizedStatement => {
                            input.pop();
                            println!("Unrecognized statement: {}", input);
                        }
                        PrepareResult::SyntaxError => {
                            input.pop();
                            println!("SYNTAX ERROR: Could not parse statement");
                        }
                        PrepareResult::StringTooLong => {
                            input.pop();
                            println!("String is too long!");
                        }
                        PrepareResult::NegativeId => {
                            input.pop();
                            println!("ID must be positive!");
                        }
                    }
                }
            }
            Err(why) => println!("Error: {why}"),
        }
        input.clear()
    }
}

fn do_meta_command(command: &String, table: &mut Table) -> MetaCommand {
    if command.starts_with(".exit") {
        table.close();
        exit(0);
    } else {
        MetaCommand::UnrecognizedCommand
    }
}

fn prepare_statement(statement: &String) -> PrepareResult {
    if statement.starts_with("insert") {
        let re = Regex::new(r"^insert (-?\d+) (\w+) ([\w@\.]+)").unwrap();
        match re.captures(statement) {
            Some(cap) => {
                let id: u32 = if let Ok(i) = cap.get(1).unwrap().as_str().parse() {
                    i
                } else {
                    return PrepareResult::NegativeId;
                };

                let username: String =
                    if let Ok(user) = cap.get(2).unwrap().as_str().parse::<String>() {
                        if user.len() > 32 {
                            return PrepareResult::StringTooLong;
                        }
                        user
                    } else {
                        return PrepareResult::SyntaxError;
                    };

                let email: String = if let Ok(e) = cap.get(3).unwrap().as_str().parse::<String>() {
                    if e.len() > 255 {
                        return PrepareResult::StringTooLong;
                    }
                    e
                } else {
                    return PrepareResult::SyntaxError;
                };

                PrepareResult::Success(Statement {
                    statement_type: StatementType::Insert,
                    row_to_insert: Some(Row {
                        id,
                        username,
                        email,
                    }),
                })
            }
            None => PrepareResult::SyntaxError,
        }
    } else if statement.starts_with("select") {
        PrepareResult::Success(Statement {
            statement_type: StatementType::Select,
            row_to_insert: None,
        })
    } else {
        PrepareResult::UnrecognizedStatement
    }
}
