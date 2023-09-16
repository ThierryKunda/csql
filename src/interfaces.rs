use std::collections::HashMap;

use crate::errors::{QueryError, FlushError, LoadingError};

type ColumnName = String;
type Value = Option<String>;
pub trait Queryable<T: Recordable> {
    fn bulk_load_data(&mut self, data: &impl Loadable) -> Result<(), LoadingError>;
    fn select(&self, attributes_names: Columns, conditions: &Option<Condition>) -> Result<Vec<Vec<Option<String>>>, QueryError>;
    fn delete(&mut self, conditions: &Option<Condition>) -> Result<(), QueryError>;
    fn update(&mut self, new_values: HashMap<ColumnName, Value>, conditions: &Option<Condition>) -> Result<(), QueryError>;
    fn insert(&mut self, new_record: InsertElement) -> Result<(), QueryError>;
}

pub trait Recordable {
    fn get_record_as_collection(&self) -> Vec<Option<String>>; 
    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError>;
    fn get_attr_value(&self, attr_name: &String) -> Result<Option<String>, QueryError> {
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

pub enum Condition {
    Equal(String, String),
    GreaterThan(String, String),
    LessThan(String, String),
    Or(Box<Condition>, Box<Condition>),
    And(Box<Condition>, Box<Condition>)
}

pub enum Columns {
    All,
    ColumnNames(Vec<String>)
}
pub trait Loadable {
    fn bulk_load(source_location: String) -> Result<Box<dyn Queryable>, LoadingError>;
    fn flush() -> Result<(), FlushError>;
}