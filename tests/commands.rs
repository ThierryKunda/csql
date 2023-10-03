use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[test]
fn feature() {
    let dialect = GenericDialect {};
    let sql = "update t set a = 1, b = 2 where id > 3";
    let ast = Parser::parse_sql(&dialect, sql).unwrap();
    println!("{:#?}", ast);
}