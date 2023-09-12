use std::collections::HashMap;

use crate::errors::QueryError;

pub trait Queryable {
    fn select(&self, attributes_names: Columns, filters: Option<HashMap<(String, usize), Filter>>) -> Result<Vec<Vec<String>>, QueryError>;
    fn delete(&mut self, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError>;
}

#[derive(Clone)]
pub enum Filter {
    Equal(String),
    // GreaterThan(String),
    // LessThan(String),
    // GreaterOrEqualThan(String),
    // LessOrEqualThan(String),
}

pub enum Columns {
    All,
    ColumnNames(Vec<String>)
}