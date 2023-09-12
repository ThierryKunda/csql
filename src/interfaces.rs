use std::collections::HashMap;

use crate::errors::QueryError;

pub trait Queryable {
    fn select(&self, attributes_names: Columns, filters: Option<HashMap<(String, usize), Filter>>) -> Result<Vec<Vec<String>>, QueryError>;
    fn delete(&mut self, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError>;
    fn update(&mut self, column_name: String, new_value: &String, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError>;
    fn insert(&mut self, new_record: Vec<String>) -> Result<(), QueryError>;
}

#[derive(Debug, Clone)]
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