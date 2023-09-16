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

pub trait Recordable: Sized {
    fn get_record_as_collection(&self) -> Vec<Option<String>>; 
    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError>;
    fn get_attr_value(&self, attr_name: &String) -> Result<Option<String>, QueryError>;
    fn get_attr_values(&self, attr_names: &Vec<String>) -> Result<Vec<Option<String>>, QueryError>;
    fn update_values(&mut self, new_values: &HashMap<ColumnName, Value>) -> Result<(), QueryError>;
    fn satisfy_conditions(&self, cond: &Condition) -> Result<bool, QueryError>;
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

pub enum InsertElement {
    PlainValues(Vec<Value>),
    MappedValues(HashMap<ColumnName, Value>),
}


pub trait Loadable: Sized {
    fn bulk_data<'a>(&self) -> Result<Vec<Vec<Option<String>>>, LoadingError>;
    fn flush(&self) -> Result<(), FlushError>;
}