use csql::entities::Table;

use csql::errors::TableInitError;

use csql::file_parsing::{Buffer, Source};

use csql::traits::{Queryable, Loadable, Columns};

#[test]
fn apply_select() -> Result<(), TableInitError> {
    let buf = Buffer::new(Source::FilePath(String::from("samples/example0.csv")));
    let mut t = Table::new(None, &vec!["one", "two", "three"])?;
    t.bulk_load_data(&buf.bulk_data(3).unwrap()).unwrap();
    let expected: Vec<Vec<Option<String>>> = vec![
        vec![Some(String::from("a")), Some(String::from("c"))],
        vec![Some(String::from("1")), Some(String::from("3"))],
        vec![Some(String::from("x")), Some(String::from("z"))],
    ];
    let query_res = t.select(
        Columns::ColumnNames(vec![String::from("one"), String::from("three")]),
        &None
    ).unwrap();
    assert_eq!(query_res, expected);
    Ok(())
}

#[test]
fn dump_data_test() -> Result<(), TableInitError> {
    let buf = Buffer::new(Source::FilePath(String::from("samples/example0.csv")));
    let mut t = Table::new(None, &vec!["one", "two", "three"])?;
    t.delete(&None).unwrap();
    let new_data = t.get_records_as_collection();
    println!("{:?}", new_data);
    let res = buf.dump_data(new_data);
    assert!(res.is_ok());
    Ok(())
}