use crate::{
    errors::{QueryError, TableInitError},
    interfaces::{Filter, Queryable, Columns},
};
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub struct Table {
    name: Option<String>,
    columns_names: Vec<String>,
    records: Vec<Vec<String>>,
}

impl Table {
    pub fn new(
        name: Option<&str>,
        columns_names: Vec<&str>,
        records: Vec<Vec<String>>,
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
            records,
        })
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
        named_filters: HashMap<String, Filter>,
    ) -> Result<HashMap<usize, Filter>, QueryError> {
        let mut res: HashMap<usize, Filter> = HashMap::new();
        for pair in named_filters.iter() {
            let idx = self.get_column_index(pair.0)?;
            res.insert(idx, pair.1.clone());
        }
        Ok(res)
    }

    fn get_record_attributes(
        &self,
        record: &Vec<String>,
        attribute_indexes: &Vec<usize>,
    ) -> Vec<String> {
        record
            .iter()
            .enumerate()
            .filter(|v| attribute_indexes.contains(&v.0))
            .map(|v| v.1.clone())
            .collect()
    }

    fn get_records_from_filters(
        &self,
        filters: &HashMap<usize, Filter>,
    ) -> Result<Vec<Vec<String>>, QueryError> {
        let mut res = self.records.clone();
        for (idx, f) in filters.iter() {
            match f {
                Filter::Equal(s) => {
                    res = res
                        .into_iter()
                        .filter(|v| v.get(idx.clone()) == Some(s))
                        .collect()
                }
            }
        }
        Ok(res)
    }

    fn get_records(
        &self,
        column_indexes: Vec<usize>,
        indexed_filters: Option<HashMap<usize, Filter>>,
    ) -> Result<Vec<Vec<String>>, QueryError> {
        match indexed_filters {
            None => Ok(self
                .records
                .iter()
                .map(|v| self.get_record_attributes(v, &column_indexes))
                .collect()),
            Some(filters) => Ok(self
                .get_records_from_filters(&filters)?
                .iter()
                .map(|v| self.get_record_attributes(v, &column_indexes))
                .collect()),
        }
    }
}

impl Queryable for Table {
    fn select(
        &self,
        attributes_names: Columns,
        filters: Option<HashMap<String, Filter>>,
    ) -> Result<Vec<Vec<String>>, QueryError> {
        let col_indexes = self.get_column_indexes(attributes_names)?;
        let indexed_filters = match filters {
            Some(map) => Some(self.get_indexed_filters(map)),
            None => None,
        };
        match indexed_filters {
            None => self.get_records(col_indexes, None),
            Some(map) => self.get_records(col_indexes, Some(map?)),
        }
    }
}
