use crate::{
    errors::SerializeError,
    traits::{Condition, Executable, Columns, Filtering, InsertElement},
};
use crate::utils::Value as Val;
use sqlparser::ast::{Statement, SelectItem, SetExpr, Expr, TableFactor, Value, BinaryOperator};
use std::{collections::HashMap, ops::Deref};

#[derive(Debug)]
pub enum Command {
    Select {
        tables: Vec<String>,
        columns: Columns,
        conditions: Result<Option<Condition>, SerializeError>,
    },
    Update {
        table: String,
        updates: HashMap<String, Val>,
        conditions: Result<Option<Condition>, SerializeError>,
    },
    Insert {
        table: String,
        elements: InsertElement,
    },
    Delete {
        table: String,
        conditions: Result<Option<Condition>, SerializeError>,
    },
}

impl Filtering for Option<Expr> {
    fn deserialize_conditions(&self) -> Result<Option<Condition>, SerializeError> {
        match self {
            Some(Expr::BinaryOp { left, op, right }) => match op {
                BinaryOperator::Eq => match (left.deref(), right.deref()) {
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::SingleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::SingleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::Equal(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::DoubleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::DoubleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::Equal(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::Equal(ident.value.clone(), n2.clone()))),
                    _ => Err(SerializeError::NotImplemented(String::from("Types not compatible for comparison with operator '='"))),
                },
                BinaryOperator::Gt => match (left.deref(), right.deref()) {
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::SingleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::SingleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::GreaterThan(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::DoubleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::DoubleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::GreaterThan(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::GreaterThan(ident.value.clone(), n2.clone()))),
                    _ =>
                        Err(SerializeError::NotImplemented(String::from("Types not compatible for comparison with operator '>'"))),
                },
                BinaryOperator::Lt => match (left.deref(), right.deref()) {
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::SingleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::SingleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::LessThan(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::DoubleQuotedString(s))
                    ) |
                    (
                        Expr::Value(Value::DoubleQuotedString(s)),
                        Expr::Identifier(ident),
                    ) => Ok(Some(Condition::LessThan(ident.value.clone(), s.clone()))),
                    (
                        Expr::Identifier(ident),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::LessThan(ident.value.clone(), n2.clone()))),
                    _ => Err(SerializeError::NotImplemented(String::from(format!("Expression '{:?}' can not be used for conditions", self)))),
                },
                BinaryOperator::And => match (
                    Self::deserialize_conditions(&Some(left.deref().clone()))?,
                    Self::deserialize_conditions(&Some(right.deref().clone()))?
                ) {
                    (Some(left_cond), Some(right_cond)) => Ok(Some(Condition::And(
                        Box::new(left_cond),
                        Box::new(right_cond)
                    ))),
                    (None, Some(cond)) => Ok(Some(cond)),
                    (Some(cond), None) => Ok(Some(cond)),
                    (None, None) => Ok(None),
                }
                BinaryOperator::Or => match (
                    Self::deserialize_conditions(&Some(left.deref().clone()))?,
                    Self::deserialize_conditions(&Some(right.deref().clone()))?
                ) {
                    (Some(left_cond), Some(right_cond)) => Ok(Some(Condition::Or(
                        Box::new(left_cond),
                        Box::new(right_cond)
                    ))),
                    (None, Some(cond)) => Ok(Some(cond)),
                    (Some(cond), None) => Ok(Some(cond)),
                    (None, None) => Ok(None),
                },
                _ => Err(SerializeError::NotImplemented(String::from("Combination of comparisons not compatible with operator 'or'")))
            },
            None => Ok(None),
            _ => Err(SerializeError::NotImplemented(String::from("Types not compatible for comparison"))),
        }
    }
}

impl Executable for Statement {
    fn deserialize_as_command(&self) -> Result<Command, SerializeError> {
        match self {
            Statement::Query(q) => {
                let query = q.deref();
                let body = query.body.deref();
                let mut columns: Vec<String> = vec![];
                let mut all_selected = false;
                let mut tables = vec![];
                // let conditions = 
                let mut _conditions = Ok(None);
                match body {
                    SetExpr::Select(s) => {
                        let select = s.deref();
                        _conditions = s.selection.deserialize_conditions();
                        for proj in select.projection.iter() {
                            match proj {
                                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => columns.push(ident.clone().value),
                                SelectItem::Wildcard(_) => all_selected = true,
                                _ => return Err(SerializeError::NotImplemented(String::from("Only identifiers and aliases are implemented")))
                            }
                        }
                        for t in &select.from {
                            if let TableFactor::Table { name, .. } = &t.relation {
                                let idents = &name.0;
                                idents.iter()
                                .for_each(|ident| tables.push(ident.value.clone()))
                            }
                        }
                    },
                    _ => return Err(SerializeError::UselessToImplement)
                }
                // Ok(Command::Select { table, columns, conditions })
                if all_selected {
                    Ok(Command::Select { tables, columns: Columns::All, conditions: _conditions })
                } else {
                    Ok(Command::Select { tables, columns: Columns::ColumnNames(columns), conditions: _conditions })
                }
            },
            Statement::Insert {
                table_name,
                columns,
                source,
                ..
            } => {
                let _ident = table_name.0.get(0).ok_or(SerializeError::NotImplementable)?;
                let table = _ident.value.clone();
                let mut _elements: InsertElement = InsertElement::PlainValues(vec![]);
                if columns.len() == 0 {
                    let mut plain_values: Vec<Val> = Vec::new();
                    match source.deref().body.deref() {
                        SetExpr::Values(vals) => {
                            let unique_line = vals.rows.first().ok_or(SerializeError::NotImplementable)?;
                            for expr in unique_line.iter() {
                                match expr {
                                    Expr::Value(v) => match v {
                                        Value::Boolean(b) => if *b {
                                            plain_values.push(Some(String::from("true")));
                                        } else {
                                            plain_values.push(Some(String::from("false")));
                                        },
                                        Value::Number(nb, _) => plain_values.push(Some(nb.clone())),
                                        Value::SingleQuotedString(s)
                                        | Value::DoubleQuotedString(s) => plain_values.push(Some(s.clone())),
                                        Value::Null => plain_values.push(None),
                                        _ => return Err(SerializeError::NotImplemented(String::from(format!("Type not implemented for the value : {:?}", v)))),
                                    },
                                    _ => return Err(SerializeError::NotImplementable),
                                }
                            }
                        },
                        _ => return Err(SerializeError::NotImplementable),
                    }
                    _elements = InsertElement::PlainValues(plain_values);       
                } else {
                    let mut mapped_values: HashMap<String, Val> = HashMap::new();
                    match source.deref().body.deref() {
                            SetExpr::Values(vals) => {
                                let unique_line = vals.rows.first().ok_or(SerializeError::NotImplementable)?;
                                for i in 0..unique_line.len() {
                                    match (columns.get(i), unique_line.get(i)) {
                                        (Some(ident), Some(expr)) => match expr {
                                            Expr::Value(v) => match v {
                                                Value::Boolean(b) => if *b {
                                                    mapped_values.insert(ident.value.clone(), Some(String::from("true")));
                                                } else {
                                                    mapped_values.insert(ident.value.clone(), Some(String::from("false")));
                                                },
                                                Value::Number(nb, _) => { mapped_values.insert(ident.value.clone(), Some(nb.clone())); },
                                                Value::SingleQuotedString(s)
                                                | Value::DoubleQuotedString(s) => {mapped_values.insert(ident.value.clone(), Some(s.clone()));},
                                                Value::Null =>{ mapped_values.insert(ident.value.clone(), None); },
                                                _ => return Err(SerializeError::NotImplemented(String::from(format!("Type not implemented for the value : {:?}", v)))),
                                            },
                                            _ => return Err(SerializeError::NotImplementable),
                                        },
                                        _ => return Err(SerializeError::NotImplementable),
                                    }
                                }
                            },
                            _ => return Err(SerializeError::NotImplementable),
                    }
                    _elements = InsertElement::MappedValues(mapped_values);
                }
                Ok(Command::Insert { table, elements: _elements })
            },
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => {
                let table = if let TableFactor::Table { name, .. } = &table.relation {
                    Ok(name.0
                    .get(0)
                    .ok_or(SerializeError::NotImplementable)?
                    .value
                    .clone())
                } else {
                    Err(SerializeError::NotImplementable)
                }?;
                let conditions = selection.deserialize_conditions();
                let mut updates = HashMap::new();
                for ass in assignments.iter() {
                    let attr_name = ass.id.first().ok_or(SerializeError::NotImplementable)?.value.clone();
                    match &ass.value {
                        Expr::Value(v) => match v {
                            Value::Boolean(b) => if *b {
                                updates.insert(attr_name, Some(String::from("true")));
                            } else {
                                updates.insert(attr_name, Some(String::from("false")));
                            },
                            Value::Number(nb, _) => { updates.insert(attr_name, Some(nb.clone())); },
                            Value::SingleQuotedString(s)
                            | Value::DoubleQuotedString(s) => { updates.insert(attr_name, Some(s.clone())); },
                            Value::Null => { updates.insert(attr_name, None); },
                            _ => return Err(SerializeError::NotImplemented(String::from(format!("Type not implemented for the value : {:?}", &ass.value)))),
                        },
                        _ => return Err(SerializeError::NotImplementable),
                    }
                } 
            Ok(Command::Update {table, updates, conditions })
            },
            Statement::Delete {
                from,
                selection,
                ..
            } => {
                let mut _t = String::from("");
                let unique_t = from.first()
                .ok_or(SerializeError::NotImplementable)?;
                if let TableFactor::Table { name, ..} = &unique_t.relation {
                    _t = name.0.first()
                    .ok_or(SerializeError::NotImplementable)?
                    .value.clone();
                }
            let conditions = selection.deserialize_conditions();
            Ok(Command::Delete { table: _t, conditions })
            },
            _ => Err(SerializeError::UselessToImplement),
        }
    }
}
