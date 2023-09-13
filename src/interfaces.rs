use std::collections::HashMap;

use crate::errors::QueryError;

pub trait Queryable {
    fn select(&self, attributes_names: Columns, filters: Option<HashMap<(String, usize), Filter>>) -> Result<Vec<Vec<String>>, QueryError>;
    fn delete(&mut self, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError>;
    fn update(&mut self, column_name: String, new_value: &String, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError>;
    fn insert(&mut self, new_record: Vec<String>) -> Result<(), QueryError>;
}

pub trait Recordable {
    fn get_record_as_collection(&self) -> Vec<String>; 
    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError>;
    fn get_attr_value(&self, attr_name: &String) -> Result<String, QueryError> {
        let coll = self.get_record_as_collection();
        let idx = self.get_attr_index_from_name(attr_name)?;
        match coll.get(idx) {
            Some(v) => Ok(v.clone()),
            None => Err(QueryError),
        }
    }
    fn delete_value(&mut self, attr_name: &String) -> Result<(), QueryError>;
    fn update_value(&mut self, attr_name: &String, new_value: &String) -> Result<(), QueryError>;
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