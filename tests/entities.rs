use std::collections::HashMap;

use csql::{entities::*, errors::TableInitError, interfaces::{Queryable, Columns, Filter}};

#[test]
fn table_creation_test() {
    let t1 = Table::new(
        None,
        vec!["id", "username"],
        vec![
            vec![String::from("1"), String::from("john.doe123")],
            vec![String::from("2"), String::from("jrogan_$89")],
        ],
    );
    let t2 = Table::new(
        None,
        vec!["id", "username"],
        vec![
            vec![String::from("1")],
            vec![String::from("2"), String::from("jrogan_$89")],
        ],
    );
    assert!(t1.is_ok());
    assert!(t2.is_err());
}

#[test]
fn select_one_column_test() -> Result<(), TableInitError> {
    let t = Table::new(
        None,
        vec!["id", "username", "password"],
        vec![
            vec![
                String::from("1"),
                String::from("john.doe123"),
                String::from("abcd1234"),
            ],
            vec![
                String::from("2"),
                String::from("jrogan_$89"),
                String::from("zzz"),
            ],
        ],
    )?;
    let query_res = t.select(csql::interfaces::Columns::ColumnNames(vec![String::from("id")]), None);
    assert_eq!(query_res, Ok(vec![vec![String::from("1")], vec![String::from("2")]]));
    Ok(())
}

#[test]
fn select_multiple_columns_test() -> Result<(), TableInitError> {
    let t = Table::new(
        None,
        vec!["id", "username", "password"],
        vec![
            vec![
                String::from("1"),
                String::from("john.doe123"),
                String::from("abcd1234"),
            ],
            vec![
                String::from("2"),
                String::from("jrogan_$89"),
                String::from("zzz"),
            ],
        ],
    )?;
    let query_res = t.select(Columns::ColumnNames(vec![String::from("id"), String::from("password")]), None);
    assert_eq!(query_res, Ok(vec![
        vec![String::from("1"), String::from("abcd1234")],
        vec![String::from("2"), String::from("zzz")],
    ]));
    Ok(())
}

#[test]
fn select_filtered_columns_test() -> Result<(), TableInitError> {
    let t = Table::new(
        None,
        vec!["id", "username", "password"],
        vec![
            vec![
                String::from("1"),
                String::from("john.doe123"),
                String::from("abcd1234"),
            ],
            vec![
                String::from("2"),
                String::from("jrogan_$89"),
                String::from("zzz"),
            ],
        ],
    )?;
    let mut filters_1: HashMap<String, Filter> = HashMap::new();
    filters_1.insert(String::from("id"), Filter::Equal(String::from("1")));

    let mut filters_2: HashMap<String, Filter> = HashMap::new();
    filters_2.insert(String::from("id"), Filter::Equal(String::from("1")));
    filters_2.insert(String::from("password"), Filter::Equal(String::from("password_non_existent")));

    let query_res_1 = t.select(Columns::All, Some(filters_1));
    assert_eq!(query_res_1, Ok(vec![
        vec![String::from("1"), String::from("john.doe123"), String::from("abcd1234")],
    ]));
    let query_res_2 = t.select(Columns::All, Some(filters_2));
    assert_eq!(query_res_2, Ok(vec![]));

    Ok(())
}