use std::collections::HashMap;

use crate::errors::{QueryError, CommitError, LoadingError, ExportingError};

type ColumnName = String;
type Value = Option<String>;
pub trait Queryable<T: Recordable> {
    fn bulk_load_data(&mut self, data: &Vec<Vec<Option<String>>>) -> Result<(), LoadingError> {
        for r in data {
            if let Err(_) = self.insert(InsertElement::PlainValues(r.clone())) {
                return Err(LoadingError::InvalidRecord(String::new()));
            }
        }
        Ok(())
    }
    fn select(&self, attributes_names: Columns, conditions: &Option<Condition>) -> Result<Vec<Vec<Option<String>>>, QueryError>;
    fn delete(&mut self, conditions: &Option<Condition>) -> Result<(), QueryError>;
    fn update(&mut self, new_values: HashMap<ColumnName, Value>, conditions: &Option<Condition>) -> Result<(), QueryError>;
    fn insert(&mut self, new_record: InsertElement) -> Result<(), QueryError>;
    fn get_records_as_collection(&self) -> Vec<Vec<Option<String>>>;
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


pub trait Loadable<T>: Sized
where T: Recordable {
    fn line_to_vec(line_string: &mut String, columns_amount: usize) -> Result<Vec<Option<String>>, LoadingError>;
    fn collection_to_string(collection: Vec<Vec<Option<String>>>) -> String;
    fn load_from_source(source_path: &str, source_type: SourceType) -> Result<Self, LoadingError>;
    fn bulk_data(&self, columns_amount: usize) -> Result<Vec<Vec<Option<String>>>, LoadingError>;
    fn dump_data(&self, data: Vec<Vec<Option<String>>>) -> Result<(), ExportingError>;
    fn commit(&mut self, query_subject: impl Queryable<T>) -> Result<(), CommitError>;
}

pub enum SourceType {
    LocalFile,
    Http,
}