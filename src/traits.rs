use std::collections::HashMap;

use crate::{errors::{CommitError, ExportError, LoadingError, QueryError, SerializeError}, commands::Command, utils::Value};

pub trait Data: Sized {
    fn bulk_load_data(&mut self, data: &Vec<Vec<Value>>) -> Result<(), LoadingError>;
    fn get_records_as_collection(&self) -> Vec<Vec<Value>>;
}

type ColumnName = String;
pub trait Queryable<T: Recordable> {
    fn select(
        &self,
        _object_names: &Option<Vec<String>>,
        attributes_names: &Columns,
        conditions: &Option<Condition>,
    ) -> Result<Vec<Vec<Value>>, QueryError>;
    fn delete(&mut self, _object_name: &Option<String>, conditions: &Option<Condition>) -> Result<(), QueryError>;
    fn update(
        &mut self,
        _object_name: &Option<String>,
        new_values: HashMap<ColumnName, Value>,
        conditions: &Option<Condition>,
    ) -> Result<(), QueryError>;
    fn insert(&mut self, _object_name: &Option<String>, new_record: InsertElement) -> Result<(), QueryError>;
}

pub trait Recordable: Sized {
    fn get_record_as_collection(&self) -> Vec<Value>;
    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError>;
    fn get_attr_value(&self, attr_name: &String) -> Result<Value, QueryError>;
    fn get_attr_values(&self, attr_names: &Vec<String>) -> Result<Vec<Value>, QueryError>;
    fn update_values(&mut self, new_values: &HashMap<ColumnName, Value>) -> Result<(), QueryError>;
    fn satisfy_conditions(&self, cond: &Condition) -> Result<bool, QueryError>;
}

#[derive(Debug)]
pub enum Condition {
    Equal(String, String),
    GreaterThan(String, String),
    LessThan(String, String),
    Or(Box<Condition>, Box<Condition>),
    And(Box<Condition>, Box<Condition>),
}

#[derive(Debug)]
pub enum Columns {
    All,
    ColumnNames(Vec<String>),
}

#[derive(Debug)]
pub enum InsertElement {
    PlainValues(Vec<Value>),
    MappedValues(HashMap<ColumnName, Value>),
}

pub trait Loadable<T>: Sized
where
    T: Recordable,
{
    fn line_to_vec(
        line_string: &mut String,
        columns_amount: usize,
    ) -> Result<Vec<Value>, LoadingError>;
    fn collection_to_string(collection: Vec<Vec<Value>>) -> String;
    fn load_from_source(source_path: &str, source_type: SourceType) -> Result<Self, LoadingError>;
    fn bulk_data(&self, columns_amount: usize) -> Result<Vec<Vec<Value>>, LoadingError>;
    fn dump_data(&self, data: Vec<Vec<Value>>) -> Result<(), ExportError>;
    fn commit(&mut self, new_data: &impl Data) -> Result<(), CommitError> {
        self.dump_data(new_data.get_records_as_collection())
            .map_err(|_| CommitError)
    }
}

pub enum SourceType {
    LocalFile,
    Http,
}

pub trait Filtering {
    fn deserialize_conditions(&self) -> Result<Option<Condition>, SerializeError>;
}

pub trait Executable {
    fn deserialize_as_command(&self) -> Result<Command, SerializeError>;
}

pub trait Storage {
    fn bulk_data(&self, section_name: &String, columns_amount: usize) -> Result<Vec<Vec<Value>>, LoadingError>;
    fn dump_data(&self, section_name: &String, data: Vec<Vec<Value>>) -> Result<(), ExportError>;
    fn commit(&self, section_name: &String, new_data: &impl Data) -> Result<(), CommitError> {
        self.dump_data(section_name, new_data.get_records_as_collection())
            .map_err(|_| CommitError)
    }
    fn commit_all(&self , new_data: HashMap<String, &impl Data>) -> Result<(), CommitError> {
        for (section_name, d) in new_data {
            self.commit(&section_name, d)?;
        }
        Ok(())
    }
}