use std::{fs::{File, OpenOptions}, io::{BufReader, BufRead, Write, Error}};

use crate::{traits::{Loadable, SourceType}, errors::{LoadingError, ExportError}, entities::Record, utils::Value};

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

    fn open_read_write_file(path: &String, truncate: bool) -> Result<File, Error> {
        OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(truncate)
        .create(true)
        .open(path.as_str())
    }

    fn record_to_string(record: &Vec<Option<String>>) -> String {
        let res: Vec<String> = record.iter()
        .map(|val| match val {
            Some(v) => v.clone(),
            None => String::new(),
        })
        .collect();
        res.join(";")
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
        let res: Vec<String> = collection.iter()
        .map(Self::record_to_string)
        .collect();

        res.join("\n")
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
                let f = Self::open_read_write_file(file, false);
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

    fn dump_data(&self, data: Vec<Vec<Value>>) -> Result<(), crate::errors::ExportError> {
        match &self.source {
            Source::FilePath(p) => match Self::open_read_write_file(p, true) {
                Ok(mut f) => {
                    let col = Self::collection_to_string(data);
                    println!("{:?}", col);
                    let res: Result<(), Error> = write!(&mut f, "{}", col);
                    match res {
                        Ok(_) => Ok(()),
                        Err(_) => Err(ExportError::Interrupted),
                    }
                },
                Err(_) => Err(ExportError::ResourceNotFound),
            },
            Source::HttpUri(_) => todo!(),
        }
    }
    
}