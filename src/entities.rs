use crate::{
    errors::{QueryError, TableInitError, LoadingError, ExportError},
    traits::{Queryable, Columns, Recordable, Condition, InsertElement, Data, Storage, Loadable}, file_parsing::Buffer,
};
use crate::utils::Value;
use std::{collections::{HashMap, BTreeMap}, rc::Rc, path::{Path, PathBuf}, fs::{DirEntry, File}, io::{Read, BufReader}};

use serde::{Serialize, Deserialize};

#[derive(Debug, PartialEq, Eq)]
pub struct Table<T>
where T: Recordable {
    name: String,
    columns_names: Rc<Vec<String>>,
    records: Vec<T>,
}

pub struct TableIter<'a> {
    records: &'a Vec<Record>,
    current_record_index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    values: Vec<Option<String>>,
    headers: Rc<Vec<String>>,
}

impl Record {
    pub fn new(values: Vec<Option<String>>, headers: Rc<Vec<String>>) -> Record {
        Record { values, headers }
    }
}

impl Recordable for Record {
    fn get_record_as_collection(&self) -> Vec<Option<String>> {
        self.values.clone()
    }

    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError> {
        match self.headers.iter().position(|n| n == attr_name) {
            Some(idx) => Ok(idx),
            None => Err(QueryError),
        }
    }

    fn update_values(&mut self, new_values: &HashMap<String, Option<String>>) -> Result<(), QueryError> {
        let mut header_iter = self.headers.iter();
        for (attr, val) in new_values {
            match header_iter.position(|n| n == attr) {
                Some(idx) => if let Some(v) = self.values.get_mut(idx) {
                        *v = val.clone();
                    } else {
                        return Err(QueryError)
                    },
                None => return Err(QueryError),
            }
        }
        Ok(())
    }

    fn satisfy_conditions(&self, cond: &Condition) -> Result<bool, QueryError> {
        match cond {
            Condition::Equal(col, v) => Ok(self.get_attr_value(col)? == Some(v.to_string())),
            Condition::GreaterThan(col, v) => Ok(self.get_attr_value(col)? > Some(v.to_string())),
            Condition::LessThan(col, v) => Ok(self.get_attr_value(col)? < Some(v.to_string())),
            Condition::Or(cnd1, cnd2) => Ok(self.satisfy_conditions(cnd1)? || self.satisfy_conditions(cnd2)?),
            Condition::And(cnd1, cnd2) => Ok(self.satisfy_conditions(cnd1)? && self.satisfy_conditions(cnd2)?),
        }
    }

    fn get_attr_value(&self, attr_name: &String) -> Result<Option<String>, QueryError> {
        let attr_index = self.get_attr_index_from_name(attr_name)?;
         self.values.get(attr_index)
        .map(|v| v.clone())
        .ok_or(QueryError)
    }

    fn get_attr_values(&self, attr_names: &Vec<String>) -> Result<Vec<Option<String>>, QueryError> {
        let attr_indexes = attr_names.iter()
        .map(|name| self.get_attr_index_from_name(name))
        .collect::<Result<Vec<usize>, QueryError>>()?;
        Ok(self.values.iter()
        .enumerate()
        .filter(|(idx, _)| attr_indexes.contains(idx))
        .map(|(_, v)| v.clone())
        .collect())
    }
}

impl<'a> Iterator for TableIter<'a> {
    type Item = &'a Record;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.records.get(self.current_record_index);
        self.current_record_index += 1;
        res
    }
}

impl Table<Record> {
    pub fn new(
        name: &str,
        columns_names: &Vec<&str>,
    ) -> Result<Self, TableInitError> {
        // Checks columns_names has at least one column name
        if columns_names.len() == 0 {
            return Err(TableInitError::new(
                "There should be at least one column name",
            ));
        }
        // Checks columns are not empty string
        for col in columns_names.iter().enumerate() {
            if col.1 == &String::from("") {
                return Err(TableInitError::new(
                    format!(
                        "The column n°{} is an empty string, which is illegal. ",
                        col.0
                    )
                    .as_str(),
                ));
            }
        }
        Ok(Self {
            name: name.to_string(),
            columns_names: Rc::new(columns_names.iter()
                .map(|col| col.to_string())
                .collect()),
            records: vec![],
        })
    }

    pub fn iter(&self) -> TableIter {
        TableIter { records: &self.records, current_record_index: 0 }
    }
}

impl Data for Table<Record> {
    fn bulk_load_data(&mut self, data: &Vec<Vec<Value>>) -> Result<(), LoadingError> {
        for r in data {
            if let Err(_) = self.insert(&None, InsertElement::PlainValues(r.clone())) {
                return Err(LoadingError::InvalidRecord(String::new()));
            }
        }
        Ok(())
    }
    
    fn get_records_as_collection(&self) -> Vec<Vec<Option<String>>> {
        self.records.iter()
        .map(|r| r.get_record_as_collection())
        .collect()
    }
}

impl Queryable<Record> for Table<Record> {
    fn select(
        &self,
        _object_names: &Option<Vec<String>>,
        attributes_names: &Columns,
        conditions: &Option<Condition>,
    ) -> Result<Vec<Vec<Option<String>>>, QueryError> {
        match (attributes_names, conditions) {
            (Columns::All, None) => Ok(self.records
            .iter()
            .map(|r| r.get_record_as_collection())
            .collect()),
            (Columns::All, Some(conds)) => {
                let mut res: Vec<Vec<Option<String>>> = Vec::new();
                for el in self.records.iter() {
                    if el.satisfy_conditions(conds)? {
                        res.push(el.get_record_as_collection());
                    }
                }
                Ok(res)
            },
            (Columns::ColumnNames(cols), Some(conds)) => {
                let mut res: Vec<Vec<Option<String>>> = Vec::new();
                for el in self.records.iter() {
                    if el.satisfy_conditions(conds)? {
                        res.push(el.get_attr_values(&cols)?);
                    }
                }
                Ok(res)
            },
            (Columns::ColumnNames(cols), None) => {
                let mut res: Vec<Vec<Option<String>>> = Vec::new();
                for el in self.records.iter() {
                    res.push(el.get_attr_values(&cols)?);
                }
                Ok(res)
            },
            
        }
    }

    fn delete(&mut self, _object_name: &Option<String>, conditions: &Option<Condition>) -> Result<(), QueryError> {
        match conditions {
            None => self.records.clear(),
            Some(conds) => {
                let mut records_left: Vec<Record> = Vec::new();
                    for r in self.iter() {
                    if !r.satisfy_conditions(conds)? {
                        records_left.push(r.clone());
                    }
                }
                self.records = records_left;
        },
        }
        Ok(())
    }

    fn update(&mut self, _object_name: &Option<String>, new_values: HashMap<String, Option<String>>, conditions: &Option<Condition>) -> Result<(), QueryError> {
        match conditions {
            None => for r in self.records.iter_mut() {
                r.update_values(&new_values)?;
            },
            Some(cond) => for r in self.records.iter_mut() {
                if r.satisfy_conditions(cond)? {
                    r.update_values(&new_values)?;
                }
            },
        }
        Ok(())
    }

    fn insert(&mut self, _object_name: &Option<String>, new_record: InsertElement) -> Result<(), QueryError> {
        match new_record {
            InsertElement::PlainValues(values) => if values.len() == self.columns_names.len() {
                self.records.push(Record::new(values, Rc::clone(&self.columns_names)));
                Ok(())
            } else {
                Err(QueryError)
            },
            InsertElement::MappedValues(mappings) => {
                let new_values = self.columns_names.iter()
                .map(|attr| match mappings.get(attr) {
                    Some(v) => v.clone(),
                    None => None,
                }).collect();
                self.records.push(Record::new(new_values, Rc::clone(&self.columns_names)));
                Ok(())
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    data_filepaths: Option<Vec<String>>,
    aliases: HashMap<String, String>
}

pub struct Directory {
    name: String,
    path: PathBuf,
    buffers: HashMap<String, Buffer>,
}

impl Directory {
    pub fn new(path: String) -> Result<Self, LoadingError> {
        let p_obj = Path::new(path.as_str())
        .canonicalize()
        .map_err(|_| LoadingError::FailedFileLoading(std::io::ErrorKind::NotFound))?;
        let name = p_obj.file_name()
        .ok_or(LoadingError::FailedFileLoading(std::io::ErrorKind::NotFound))?
        .to_str()
        .ok_or(LoadingError::FailedFileLoading(std::io::ErrorKind::NotFound))?
        .to_string();
        Ok(Self { name, path: p_obj, buffers: HashMap::new() })
    }

    pub fn list_files(&self) -> Result<Vec<DirEntry>, LoadingError> {
        let mut res = Vec::new();
        for entry in self.path.read_dir()
            .map_err(|_| LoadingError::FailedFileLoading(std::io::ErrorKind::NotFound))? {
            if let Ok(e) = entry {
                res.push(e);
            } else {
                return Err(LoadingError::FailedFileLoading(std::io::ErrorKind::Other));
            }
        }
        Ok(res)
    }

    pub fn list_data_files(&self) -> Result<Vec<DirEntry>, LoadingError> {
        let files = self.list_files()?;
        let res: Vec<DirEntry> = files.into_iter()
        .filter_map(|dir_e| {
            let path = dir_e.path();
            let ext = path.extension()?;
            let ext_as_str = ext.to_str()?;
            if ext_as_str == "csv" {
                Some(dir_e)
            } else {
                None
            }
        })
        .collect();
        Ok(res)
    }

    pub fn load_buffers(&mut self) -> Result<(), LoadingError> {
        for e in self.list_data_files()? {
            let p = e.path();
            let file_name = e.file_name()
            .to_str()
            .ok_or(LoadingError::FailedFileLoading(std::io::ErrorKind::Other))?
            .to_string()
            .replace(".csv", "");
            let source_path = p.to_str()
            .ok_or(LoadingError::FailedFileLoading(std::io::ErrorKind::Other))?;
            let source_type = crate::traits::SourceType::LocalFile;
            self.buffers.insert(file_name, Buffer::load_from_source(source_path, source_type)?);
        }
        Ok(())
    }

    fn get_config_file(&self) -> Result<DirEntry, LoadingError> {
        let files = self.list_files()?;
        for f in files {
            if f.file_name().to_str() == Some("config.json") {
                return Ok(f);
            }
        }
        Err(LoadingError::FailedFileLoading(std::io::ErrorKind::NotFound))
    }

    pub fn get_config(&self) -> Result<Config, LoadingError> {
        let conf_file = self.get_config_file()?;
        let f = File::open(conf_file.path())
        .map_err(|e| LoadingError::FailedFileLoading(e.kind()))?;
        let buf = BufReader::new(f);
        let config: Result<Config, serde_json::Error> = serde_json::from_reader(buf);
        println!("config = {:?}", config);
        config.map_err(|_| LoadingError::FailedFileLoading(std::io::ErrorKind::Other))
    }
}

impl Storage for Directory {
    fn bulk_data(&self, section_name: &String, columns_amount: usize) -> Result<Vec<Vec<Value>>, LoadingError> {
        let buf = self.buffers.get(section_name);
        match buf {
            Some(b) => b.bulk_data(columns_amount),
            None => Err(LoadingError::SourceNotListed),
        }
    }

    fn dump_data(&self, section_name: &String, data: Vec<Vec<Value>>) -> Result<(), ExportError> {
        let buf = self.buffers.get(section_name);
        match buf {
            Some(b) => b.dump_data(data),
            None => Err(ExportError::ResourceNotFound),
        }
    }
}

pub struct DataStore {
    tables: BTreeMap<String, Table<Record>>, 
}

impl DataStore {
    pub fn new(tables: Vec<Table<Record>>) -> Self {
        let mut res = BTreeMap::new();
        for t in tables {
            res.insert(t.name.clone(), t);
        }
        Self { tables: res }
    }
}