use std::fs::File;
use std::io::BufWriter;
use std::io::Write;

use mysql::prelude::Queryable;
use mysql::Pool;


#[derive(Debug)]
struct Item {
    id: u64,
    keyword: String,
}


fn main() {
    let manticore_pool = Pool::new("mysql://root:@172.21.162.118:9306/Manticore?reset_connection=false").unwrap();

    let mut conn = manticore_pool.get_conn().unwrap();

    let file = File::create("mobile.txt").unwrap();
    let mut writer = BufWriter::new(file);

    loop {
        let mut scroll:String="".to_string();
        if let Some(val) = conn.query_first::<String,&str>("SHOW SCROLL;").unwrap(){
            println!("{}",&val);
            scroll=val;
        }

        let mut sql:String="select id,keyword from req_log where sp_id='200002' and req_time>=1751299200 and req_time<1753977600 ".to_string();
        if scroll.len()>0{
            sql.push_str(format!("limit 1000 OPTION scroll='{}' ",scroll.as_str()).as_str());
        }else{
            sql.push_str("order by id asc limit 1000");
        }
         println!("sql:{}",&sql);

        let items = conn.query_map(sql, |(id, keyword)| {
            Item {
                id,
                keyword
            }
        }).unwrap();

        if items.len()==0{
            print!("end.");
            break;
        }

        for item in items{
            let line=format!("{},{}\n",item.id,item.keyword);
            writer.write_all(line.as_bytes()).unwrap();
        }
    }
}
