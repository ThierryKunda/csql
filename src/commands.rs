use crate::{
    errors::SerializeError,
    traits::{Condition, Executable},
};
use sqlparser::ast::Statement;
use std::collections::HashMap;

pub enum Command {
    Select {
        table: String,
        columns: Vec<String>,
        conditions: Option<Vec<Condition>>,
    },
    Update {
        table: String,
        updates: HashMap<String, String>,
        conditions: Option<Vec<Condition>>,
    },
    Delete {
        conditions: Option<Vec<Condition>>,
    },
}

impl Executable for Statement {
    fn deserialize_as_command(&self) -> Result<Command, SerializeError> {
        match self {
            Statement::Query(_) => todo!(),
            Statement::Insert {
                table_name,
                columns,
                source,
                table,
                ..
            } => todo!(),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => todo!(),
            Statement::Delete {
                tables,
                from,
                selection,
                ..
            } => todo!(),
            _ => Err(SerializeError::UselessToImplement),
        }
    }
}
