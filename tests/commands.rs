use csql::traits::Executable;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::{Parser, ParserError};

#[test]
fn parsing_select() -> Result<(), ParserError>{
    let dialect = GenericDialect {};
    let sql = "
    select * from user, post;
    select name from user where id = 1000;
    select name, email from user where id > 5 and id < 200";
    let statements = Parser::parse_sql(&dialect, sql)?;
    let commands: Result<Vec<_>, _> = statements.iter()
    .map(|st| st.deserialize_as_command())
    .collect();
    if let Ok(comms) = &commands {
        comms.iter().for_each(|c| println!("{:?}", c))
    }
    Ok(())
}

#[test]
fn parsing_update() -> Result<(), ParserError> {
    let dialect = GenericDialect {};
    let sql = "
    update user set name = 'john';
    update user set email = 'john.doe@example.com', password = 'abcd1234' where name = 'john'";
    let statements = Parser::parse_sql(&dialect, sql)?;
    let commands: Result<Vec<_>, _> = statements.iter()
    .map(|st| st.deserialize_as_command())
    .collect();
    if let Ok(comms) = &commands {
        comms.iter().for_each(|c| println!("{:?}", c))
    }
    Ok(())
}

#[test]
fn parsing_insert() {
    
}

#[test]
fn parsing_delete() {
    
}