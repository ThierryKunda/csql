pub fn main() {
    let col_name = ".";
    let split_res: Vec<&str> = col_name.split(".").collect();
    println!("{:?}", split_res);
}