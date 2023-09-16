use std::collections::HashMap;

use csql::{entities::*, errors::TableInitError, traits::{Queryable, Columns, InsertElement, Condition, Recordable}};

#[test]
fn table_creation_test() {
    let t1 = Table::new(None,&vec!["id", "username"]);
    let t2 = Table::new(None, &vec!["id", ""]);
    let t3 = Table::new(None, &vec![]);
    assert!(t1.is_ok());
    assert!(t2.is_err());
    assert!(t3.is_err());
}

#[test]
fn insert_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
        vec![
            Some(String::from("3")),
            Some(String::from("mickael76")),
            Some(String::from("okokokok")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    };
    let res = t.insert(InsertElement::PlainValues(vec![
        Some(String::from("3")),
        Some(String::from("mickael76")),
        Some(String::from("okokokok")),
    ]));
    assert!(res.is_ok());
    let mut iter = t.iter().map(|r| r.get_record_as_collection());
    iter.next();
    iter.next();
    let expected = vec![
        Some(String::from("3")),
        Some(String::from("mickael76")),
        Some(String::from("okokokok")),
    ];
    assert_eq!(iter.next(), Some(expected));
    Ok(())
}

#[test]
fn select_one_column_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    }
    let query_res = t.select(Columns::ColumnNames(vec![String::from("id")]),& None);
    assert_eq!(query_res, Ok(vec![vec![Some(String::from("1"))], vec![Some(String::from("2"))]]));
    Ok(())
}

#[test]
fn select_multiple_columns_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    }
    let query_res = t.select(Columns::ColumnNames(vec![String::from("id"), String::from("password")]), &None);
    assert_eq!(query_res, Ok(vec![
        vec![Some(String::from("1")), Some(String::from("abcd1234"))],
        vec![Some(String::from("2")), Some(String::from("zzz"))],
    ]));
    Ok(())
}

#[test]
fn select_filtered_columns_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    }

    let conditions_1 = Condition::Equal(String::from("id"), String::from("1"));
    let conditions_2 = Condition::And(
        Box::new(Condition::Equal(String::from("id"), String::from("1"))),
        Box::new(Condition::Equal(String::from("password"), String::from("password_non_existent"))),
    );
    let query_res_1 = t.select(Columns::All, &Some(conditions_1));
    let query_res_2 = t.select(Columns::All, &Some(conditions_2));
    
    assert_eq!(query_res_1, Ok(vec![
        vec![Some(String::from("1")), Some(String::from("john.doe123")), Some(String::from("abcd1234"))],
    ]));
    assert_eq!(query_res_2, Ok(vec![]));

    Ok(())
}

#[test]
fn delete_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    }
    let query_res = t.delete(&None);
    assert!(query_res.is_ok());
    assert!(t.iter().next().is_none());
    Ok(())
}

#[test]
fn delete_filtered_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
        vec![
            Some(String::from("3")),
            Some(String::from("mickael76")),
            Some(String::from("okokokok")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    };
    let conditions = Condition::LessThan(String::from("id"), String::from("3"));
    let query_res = t.delete(&Some(conditions));
    let mut iter = t.iter().map(|r| r.get_record_as_collection());
    assert!(query_res.is_ok());
    let expected = vec![
        Some(String::from("3")),
        Some(String::from("mickael76")),
        Some(String::from("okokokok")),
    ];
    
    assert_eq!(iter.next(), Some(expected));
    assert_eq!(iter.next(), None);
    Ok(())
}

#[test]
fn update_test() -> Result<(), TableInitError> {
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
        vec![
            Some(String::from("3")),
            Some(String::from("mickael76")),
            Some(String::from("okokokok")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    };
    let updated_elements: HashMap<String, Option<String>> = HashMap::from([(
        String::from("username"),
        Some(String::from("New name here !"))
    )]);
    let res = t.update(updated_elements, &None);
    assert!(res.is_ok());
    let e1 = vec![
        Some(String::from("1")),
        Some(String::from("New name here !")),
        Some(String::from("abcd1234")),
    ];
    let e2 = vec![
        Some(String::from("2")),
        Some(String::from("New name here !")),
        Some(String::from("zzz")),
    ];
    let e3 = vec![
        Some(String::from("3")),
        Some(String::from("New name here !")),
        Some(String::from("okokokok")),
    ];
    let mut iter = t.iter().map(|r| r.get_record_as_collection());
    assert_eq!(iter.next(), Some(e1));
    assert_eq!(iter.next(), Some(e2));
    assert_eq!(iter.next(), Some(e3));

    Ok(())
}

#[test]
fn update_filtered_test() -> Result<(), TableInitError>{
    let mut t = Table::new(None, &vec!["id", "username", "password"])?;
    let lines = vec![
        vec![
            Some(String::from("1")),
            Some(String::from("john.doe123")),
            Some(String::from("abcd1234")),
        ],
        vec![
            Some(String::from("2")),
            Some(String::from("jrogan_$89")),
            Some(String::from("zzz")),
        ],
        vec![
            Some(String::from("3")),
            Some(String::from("mickael76")),
            Some(String::from("okokokok")),
        ],
    ];
    for l in lines {
        let r = t.insert(InsertElement::PlainValues(l));
        if let Err(_) = r {
            return r.map_err(|_| TableInitError::new("Error while inserting element..."));
        }
    };

    let updated_elements: HashMap<String, Option<String>> = HashMap::from([(
        String::from("username"),
        Some(String::from("New name here !"))
    )]);
    let conditions = Condition::Equal(String::from("id"), String::from("1"));
    let res = t.update(updated_elements, &Some(conditions));
    
    assert!(res.is_ok());
    let e1 = vec![
        Some(String::from("1")),
        Some(String::from("New name here !")),
        Some(String::from("abcd1234")),
    ];
    let e2 = vec![
        Some(String::from("2")),
        Some(String::from("jrogan_$89")),
        Some(String::from("zzz")),
    ];
    let e3 = vec![
        Some(String::from("3")),
        Some(String::from("mickael76")),
        Some(String::from("okokokok")),
    ];
    let mut iter = t.iter().map(|r| r.get_record_as_collection());
    assert_eq!(iter.next(), Some(e1));
    assert_eq!(iter.next(), Some(e2));
    assert_eq!(iter.next(), Some(e3));
    
    Ok(())
}