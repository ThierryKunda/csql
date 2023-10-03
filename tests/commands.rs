use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn feature() {
    let dialect = GenericDialect {};
    let sql = "INSERT INTO t VALUES (1,2,3)";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    let first = ast.first().unwrap();
    if let Statement::Query(q) = first {
        let s1 = &**q;
        // println!("{:?}", s1.order_by);
        let s2 = &**(&s1.body);
        if let sqlparser::ast::SetExpr::Select(s) = s2 {
            let select_body = &**s;
            let from = select_body.from.first().unwrap();
            let group_by = &select_body.group_by;
            // println!("From : {:?}", from);
            // println!("Group by : {:?}", group_by);
            // println!("Order by : {:?}", order_by);
        }
    }
    println!("{:#?}", first);
}