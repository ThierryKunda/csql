use std::{fs::{File, OpenOptions}, io::{BufReader, BufRead, Write, Error}};

use crate::{traits::{Loadable, SourceType, Queryable}, errors::{LoadingError, CommitError, ExportError}, entities::Record};

pub enum Source {
    FilePath(String),
    HttpUri(String)
}

pub struct Buffer {
    source: Source
}

impl Buffer {
    pub fn new(source: Source) -> Self {
        Self { source }
    }

    fn open_read_write_file(path: &String) -> Result<File, Error> {
        OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .open(path.as_str())
    }
}

impl Loadable<Record> for Buffer {
    fn line_to_vec(line_string: &mut String, columns_amount: usize) -> Result<Vec<Option<String>>, LoadingError> {
        if line_string.ends_with(";") {
            line_string.pop();
        }
        let l: Vec<Option<String>> = line_string
        .split(";")
        .map(|attr| match attr {
            "" => None,
        s => Some(s.to_string())
        }).collect();
        if l.len() == columns_amount {
            Ok(l)
        } else {
            Err(LoadingError::InvalidRecord(
                String::from("Number of columns does not match the original one.")
            ))
        }
    }

    fn collection_to_string(collection: Vec<Vec<Option<String>>>) -> String {
        let flatten_vec: Vec<Vec<String>> = collection
        .into_iter()
        .map(|r| r.into_iter().map(|r| match r {
            Some(v) => v,
            None => String::from(""),
        }).collect())
        .collect();
        let res: Vec<String> = flatten_vec.iter()
        .map(|r| r.join(";"))
        .collect();
        res.join(";\n")
    }

    fn load_from_source(source_path: &str, source_type: SourceType) -> Result<Self, LoadingError> {
        match source_type {
            SourceType::LocalFile => Ok(Self {source: Source::FilePath(source_path.to_string()) }),
            SourceType::Http => Err(LoadingError::SourceNotImplemented),
        }
    }

    fn bulk_data(&self, columns_amount: usize) -> Result<Vec<Vec<Option<String>>>, LoadingError> {
        match &self.source {
            Source::FilePath(file) => {
                let f = Self::open_read_write_file(file);
                match f {
                    Ok(file) => {
                        let reader = BufReader::new(file);
                        let lines_iter = reader.lines();
                        let mut records_as_vec: Vec<Vec<Option<String>>> = Vec::new();
                        for l in lines_iter {
                            match l {
                                Ok(mut l) => records_as_vec.push(Self::line_to_vec(&mut l, columns_amount)?),
                                Err(e) => return Err(LoadingError::FailedFileLoading(e.kind())),
                            }
                        }
                        Ok(records_as_vec)
                    },
                    Err(e) => Err(LoadingError::FailedFileLoading(e.kind())),
                }
            },
            Source::HttpUri(_) => Err(LoadingError::SourceNotImplemented),
        }
    }



    fn commit(&mut self, query_subject: impl Queryable<Record>) -> Result<(), CommitError> {
        match &self.source {
            Source::FilePath(s) => {
                let file = Self::open_read_write_file(s);
                match file {
                    Ok(mut f) => {
                        let str_colls = Self::collection_to_string(query_subject.get_records_as_collection());
                        let res = f.write(str_colls.as_bytes());
                        match res {
                            Ok(_) => Ok(()),
                            Err(_) => Err(CommitError),
                        }
                    },
                    Err(_) => Err(CommitError),
                }
            },
            Source::HttpUri(_) => Err(CommitError),
            
        }
    }
}