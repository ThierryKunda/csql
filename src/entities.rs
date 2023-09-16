use crate::{
    errors::{QueryError, TableInitError, LoadingError},
    interfaces::{Queryable, Columns, Recordable, Condition, InsertElement, Loadable},
};
use std::{collections::HashMap, rc::Rc};

#[derive(Debug, PartialEq, Eq)]
pub struct Table {
    name: Option<String>,
    columns_names: Vec<String>,
    records: Vec<Vec<Option<String>>>,
}

pub struct TableIter<'a> {
    records: &'a Vec<Vec<Option<String>>>,
    current_record_index: usize,
}

pub struct Record<'a> {
    values: Vec<Option<String>>,
    headers: &'a Vec<String>,
}

impl Record<'_> {
    pub fn new(values: Vec<Option<String>>, headers: &Vec<String>) -> Self {
        Self { values, headers }
    }
}

impl Recordable for Record<'_> {
    fn get_record_as_collection(&self) -> Vec<Option<String>> {
        self.values.clone()
    }

    fn get_attr_index_from_name(&self, attr_name: &String) -> Result<usize, QueryError> {
        match self.headers.iter().position(|n| n == attr_name) {
            Some(idx) => Ok(idx),
            None => Err(QueryError),
        }
    }

    fn update_value(&mut self, attr_name: &String, new_value: &String) -> Result<(), QueryError> {
        match self.headers.iter().position(|n| n == attr_name) {
            Some(idx) => match self.values.get_mut(idx) {
                    Some(v) => {
                        *v = None;
                        Ok(())
                    },
                    None => Err(QueryError),
                },
            None => Err(QueryError),
        }
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
        todo!()
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
    type Item = &'a Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.records.get(self.current_record_index);
        self.current_record_index += 1;
        res
    }
}

impl Table<'_> {
    pub fn new(
        name: Option<&str>,
        columns_names: Vec<&str>,
        records: Vec<Vec<Option<String>>>,
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
        // Checking columns and each record have the same size
        let columns_amount = columns_names.len();
        for r in records.iter().enumerate() {
            if r.1.len() != columns_amount {
                return Err(TableInitError::new(
                    format!(
                        "The line n°{} does not have the right amount of columns",
                        r.0
                    )
                    .as_str(),
                ));
            }
        }
        Ok(Self {
            name: name.map(|v| v.to_string()),
            columns_names: columns_names.iter()
                .map(|col| col.to_string())
                .collect(),
            records: records.iter()
            .map(|r|
                Record::new(r.clone(), &columns_names
                .iter()
                .map(|v| v.to_string())
                .collect()))
            .collect(),
        })
    }

    pub fn iter(&self) -> TableIter {
        TableIter { records: &self.records, current_record_index: 0 }
    }

    fn get_column_index(&self, column_name: &String) -> Result<usize, QueryError> {
        let res = self.columns_names.iter().position(|el| el == column_name);
        match res {
            Some(v) => Ok(v),
            None => Err(QueryError),
        }
    }

    fn get_column_indexes(&self, column_names: Columns) -> Result<Vec<usize>, QueryError> {
        match column_names {
            Columns::All => Ok(
                self.columns_names
                .iter()
                .enumerate()
                .map(|pair| pair.0)
                .collect()
            ),
            Columns::ColumnNames(cols) => cols
            .iter()
            .map(|col| self.get_column_index(col))
            .collect(),
        }
    }

    fn get_indexed_filters(
        &self,
        named_filters: HashMap<(String, usize), Filter>,
    ) -> Result<HashMap<(usize, usize), Filter>, QueryError> {
        let mut res: HashMap<(usize, usize), Filter> = HashMap::new();
        for pair in named_filters.iter() {
            let idx = self.get_column_index(&pair.0.0)?;
            res.insert((idx, pair.0.1.clone()), pair.1.clone());
        }
        Ok(res)
    }

    fn get_record_attributes(
        &self,
        record: &Vec<Option<String>>,
        attribute_indexes: &Vec<usize>,
    ) -> Vec<Option<String>> {
        record
            .iter()
            .enumerate()
            .filter(|v| attribute_indexes.contains(&v.0))
            .map(|v| v.1.clone())
            .collect()
    }

}

impl Queryable for Table<'_> {
    fn select(
        &self,
        attributes_names: Columns,
        conditions: &Condition,
    ) -> Result<Vec<Vec<Option<String>>>, QueryError> {
        match attributes_names {
            Columns::All => Ok(self.records.iter()
            .map(|r| r.get_record_as_collection())
            .collect()),
            Columns::ColumnNames(_) => Ok(self.records.iter()
            .filter(|r| r.satisfy_conditions(conditions)?)
            .map(|r| r.get_record_as_collection())
            .collect()),
        }
    }

    fn delete(&mut self, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError> {
        match filters {
            None => self.records = vec![],
            Some(flt) => {
                let indexed_filters = self.get_indexed_filters(flt)?;
                for f in indexed_filters.iter() {
                    self.records = match f.1 {
                        Filter::Equal(s) => self.records
                        .iter()
                        .filter(|v| v.deref().get(f.0.0.clone()) != Some(&Some(s.clone())))
                        .map(|v| v.clone())
                        .collect(),
                    };
                }
            },
        }
        Ok(())
    }

    fn update(&mut self, column_name: String, new_value: &Option<String>, filters: Option<HashMap<(String, usize), Filter>>) -> Result<(), QueryError> {
        let col_index = self.get_column_index(&column_name)?;
        match filters {
            None => for r in self.records.iter_mut() {
                r.remove(col_index);
                r.insert(col_index, new_value.clone());
            },
            Some(flt) => {
                let indexed_filters = self.get_indexed_filters(flt)?;
                for r in self.records.iter_mut() {
                    let mut to_update = true;
                    for ((idx, _), f) in indexed_filters.iter() {
                        match f {
                            Filter::Equal(s) => if Some(&Some(s.clone())) == r.get(idx.clone()) {
                                to_update = to_update && true
                            } else {
                                to_update = to_update && false
                            },
                        }
                    }
                    println!("{}", to_update);
                    if to_update {
                        r.remove(col_index);
                        r.insert(col_index, new_value.clone());
                    }
                }
            },
        }
        Ok(())
    }

    fn insert(&mut self, new_record: Vec<Option<String>>) -> Result<(), QueryError> {
        if self.columns_names.len() == new_record.len() {
            self.records.push(new_record);
            Ok(())
        } else {
            Err(QueryError)
        }
    }
    
}
