use std::error::Error;
use std::fmt::Display;

#[derive(Debug)]
pub struct TokenizeError {
    input: String,
    reason: Option<String>
}

impl TokenizeError {
    pub fn new(input: &str, reason: Option<String>) -> Self {
        Self {
            input: input.to_string(),
            reason
        }
    }
}

impl PartialEq for TokenizeError {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input
    }
}

impl Error for TokenizeError {}

impl Display for TokenizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fmt_text = format!("The input \"{}\" could not be converted to a token.\n", self.input);
        let fmt_reason = if let Some(s) = &self.reason { format!("Description : {}", s) } else { String::from("") };
        write!(f, "{}\n{}", fmt_text, fmt_reason)
    }
}

#[derive(Debug)]
pub struct TableInitError {
    reason: String,
}

impl TableInitError {
    pub fn new(reason: &str) -> Self {
        Self { reason: reason.to_string() }
    }
}

impl Error for TableInitError {}

impl Display for TableInitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "Something went wrong when caching table :\n{}", self.reason)
    }
}

#[derive(Debug, PartialEq)]
pub struct QueryError;

impl Error for QueryError {}

impl Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "Error while querying...")
    }
}

#[derive(Debug)]
pub struct FlushError;

impl Error for FlushError {}

impl Display for FlushError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "Error while saving changes in the physical item.")
    }
}

#[derive(Debug)]
pub struct LoadingError;

impl Error for LoadingError {}

impl Display for LoadingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
      write!(f, "Error while loading data from the physical item.")
    }
}

