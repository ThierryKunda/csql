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
        conditions: Result<Option<Condition>, SerializeError>,
    },
}

impl Filtering for Option<Expr> {
    fn deserialize_conditions(&self) -> Result<Option<Condition>, SerializeError> {
        match self {
            Some(Expr::BinaryOp { left, op, right }) => match op {
                BinaryOperator::Eq => match (left.deref(), right.deref()) {
                    (
                        Expr::Value(Value::SingleQuotedString(s1)),
                        Expr::Value(Value::SingleQuotedString(s2))
                    ) => Ok(Some(Condition::Equal(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::DoubleQuotedString(s1)),
                        Expr::Value(Value::DoubleQuotedString(s2)),
                    ) => Ok(Some(Condition::Equal(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::Number(n1, _)),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::Equal(n1.clone(), n2.clone()))),
                    _ => Err(SerializeError::NotImplemented),
                },
                BinaryOperator::Gt => match (left.deref(), right.deref()) {
                    (
                        Expr::Value(Value::SingleQuotedString(s1)),
                        Expr::Value(Value::SingleQuotedString(s2))
                    ) => Ok(Some(Condition::GreaterThan(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::DoubleQuotedString(s1)),
                        Expr::Value(Value::DoubleQuotedString(s2)),
                    ) => Ok(Some(Condition::GreaterThan(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::Number(n1, _)),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::GreaterThan(n1.clone(), n2.clone()))),
                    _ => Err(SerializeError::NotImplemented),
                },
                BinaryOperator::Lt => match (left.deref(), right.deref()) {
                    (
                        Expr::Value(Value::SingleQuotedString(s1)),
                        Expr::Value(Value::SingleQuotedString(s2))
                    ) => Ok(Some(Condition::LessThan(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::DoubleQuotedString(s1)),
                        Expr::Value(Value::DoubleQuotedString(s2)),
                    ) => Ok(Some(Condition::LessThan(s1.clone(), s2.clone()))),
                    (
                        Expr::Value(Value::Number(n1, _)),
                        Expr::Value(Value::Number(n2, _))
                    ) => Ok(Some(Condition::LessThan(n1.clone(), n2.clone()))),
                    _ => Err(SerializeError::NotImplemented),
                },
                BinaryOperator::And => match (
                    Self::deserialize_conditions(&Some(left.deref().clone()))?,
                    Self::deserialize_conditions(&Some(left.deref().clone()))?
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
                    Self::deserialize_conditions(&Some(left.deref().clone()))?
                ) {
                    (Some(left_cond), Some(right_cond)) => Ok(Some(Condition::Or(
                        Box::new(left_cond),
                        Box::new(right_cond)
                    ))),
                    (None, Some(cond)) => Ok(Some(cond)),
                    (Some(cond), None) => Ok(Some(cond)),
                    (None, None) => Ok(None),
                },
                _ => Err(SerializeError::NotImplemented)
            },
            None => Ok(None),
            _ => Err(SerializeError::NotImplemented),
        }
    }
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
