extern crate core;

mod datastore;

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
}
enum StatementType {
    Insert,
    Select,
}

struct Statement {
    statement_type: StatementType,
    row_to_insert: Option<Row>,
}

fn main() {
    let mut input = String::new();
    let mut table = Table::new();
    loop {
        print!("db> ");
        io::stdout().flush().unwrap();
        match io::stdin().read_line(&mut input) {
            Ok(_) => {
                if input.starts_with(".") {
                    match do_meta_command(&input) {
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
                            println!("SYNTAX ERROR: {}", input);
                        }
                    }
                }
            }
            Err(why) => println!("Error: {why}"),
        }
        input.clear()
    }
}

fn do_meta_command(command: &String) -> MetaCommand {
    if command.starts_with(".exit") {
        exit(0);
    } else {
        MetaCommand::UnrecognizedCommand
    }
}

fn prepare_statement(statement: &String) -> PrepareResult {
    if statement.starts_with("insert") {
        let re = Regex::new(r"^insert (\d+) (\w+) ([\w@\.]+)").unwrap();
        match re.captures(statement) {
            Some(cap) => {
                let row = Row {
                    id: cap.get(1).unwrap().as_str().parse().unwrap(),
                    username: cap.get(2).unwrap().as_str().parse().unwrap(),
                    email: cap.get(3).unwrap().as_str().parse().unwrap(),
                };

                PrepareResult::Success(Statement {
                    statement_type: StatementType::Insert,
                    row_to_insert: Some(row),
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
