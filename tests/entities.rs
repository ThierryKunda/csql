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
    let mut filters_1: HashMap<(String, usize), Filter> = HashMap::new();
    filters_1.insert((String::from("id"), 0), Filter::Equal(String::from("1")));

    let mut filters_2: HashMap<(String, usize), Filter> = HashMap::new();
    filters_2.insert((String::from("id"), 0), Filter::Equal(String::from("1")));
    filters_2.insert((String::from("password"), 0), Filter::Equal(String::from("password_non_existent")));

    let query_res_1 = t.select(Columns::All, Some(filters_1));
    assert_eq!(query_res_1, Ok(vec![
        vec![String::from("1"), String::from("john.doe123"), String::from("abcd1234")],
    ]));
    let query_res_2 = t.select(Columns::All, Some(filters_2));
    assert_eq!(query_res_2, Ok(vec![]));

    Ok(())
}

#[test]
fn delete_test() -> Result<(), TableInitError> {
    let mut t1 = Table::new(
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
    let query_res = t1.delete(None);
    assert!(query_res.is_ok());
    assert!(t1.iter().next().is_none());
    Ok(())
}

#[test]
fn delete_filtered_test() -> Result<(), TableInitError> {
    let mut t1 = Table::new(
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
            vec![
                String::from("3"),
                String::from("mickael76"),
                String::from("okokokok"),
            ],
        ],
    )?;
    let mut filters: HashMap<(String, usize), Filter> = HashMap::new();
    filters.insert((String::from("id"), 0), Filter::Equal(String::from("1"))); 
    filters.insert((String::from("id"), 1), Filter::Equal(String::from("2"))); 
    let query_res = t1.delete(Some(filters));
    let mut iter = t1.iter();
    assert!(query_res.is_ok());
    let expected = vec![
        String::from("3"),
        String::from("mickael76"),
        String::from("okokokok"),
    ];
    assert_eq!(Some(&expected), iter.next());
    assert_eq!(None, iter.next());

    Ok(())
}

#[test]
fn update_test() -> Result<(), TableInitError> {
    let mut t1 = Table::new(
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
            vec![
                String::from("3"),
                String::from("mickael76"),
                String::from("okokokok"),
            ],
        ],
    )?;
    let res = t1.update(String::from("username"), &String::from("New name here !"), None);
    assert!(res.is_ok());
    let e1 = vec![
        String::from("1"),
        String::from("New name here !"),
        String::from("abcd1234"),
    ];
    let e2 = vec![
        String::from("2"),
        String::from("New name here !"),
        String::from("zzz"),
    ];
    let e3 = vec![
        String::from("3"),
        String::from("New name here !"),
        String::from("okokokok"),
    ];
    let mut iter = t1.iter();
    assert_eq!(Some(&e1), iter.next());
    assert_eq!(Some(&e2), iter.next());
    assert_eq!(Some(&e3), iter.next());

    Ok(())
}

#[test]
fn update_filtered_test() -> Result<(), TableInitError>{
    let mut t1 = Table::new(
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
            vec![
                String::from("3"),
                String::from("mickael76"),
                String::from("okokokok"),
            ],
        ],
    )?;
    let mut filters: HashMap<(String, usize), Filter> = HashMap::new();
    filters.insert((String::from("id"), 0), Filter::Equal(String::from("1")));
    let res = t1.update(String::from("username"), &String::from("New name here !"), Some(filters));
    assert!(res.is_ok());
    let e1 = vec![
        String::from("1"),
        String::from("New name here !"),
        String::from("abcd1234"),
    ];
    let e2 = vec![
        String::from("2"),
        String::from("jrogan_$89"),
        String::from("zzz"),
    ];
    let e3 = vec![
        String::from("3"),
        String::from("mickael76"),
        String::from("okokokok"),
    ];
    let mut iter = t1.iter();
    assert_eq!(Some(&e1), iter.next());
    assert_eq!(Some(&e2), iter.next());
    assert_eq!(Some(&e3), iter.next());
    
    Ok(())
}

#[test]
fn insert_test() -> Result<(), TableInitError> {
    let mut t1 = Table::new(
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
    let res = t1.insert(vec![
        String::from("3"),
        String::from("mickael76"),
        String::from("okokokok"),
    ]);
    assert!(res.is_ok());
    let mut iter = t1.iter();
    iter.next();
    iter.next();
    let expected = vec![
        String::from("3"),
        String::from("mickael76"),
        String::from("okokokok"),
    ];
    assert_eq!(Some(&expected), iter.next());
    Ok(())
}