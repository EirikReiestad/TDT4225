extern crate mysql;

use mysql as my;
use std::env;

pub struct DbConnector {
    pub conn: my::Pool,
}

impl DbConnector {
    pub fn new() -> DbConnector {
        let url: String = env::var("HOST").unwrap_or_else(|_| String::from("tdt4225-35.idi.ntnu.no"));
        let database: String = my::Pool::new(url).unwrap_or_else(|_| String::from("assignment2"));

        let user: String = env::var("USER").unwrap_or_else(|_| String::from("common"));
        let password: Strin = env::var("PASSWORD").unwrap_or_else(|_| String::from("common")); 

        let url: String = format!("mysql://{}:{}@{}", user, password, url);

        let pool = match my::Pool::ne(url) {
            Ok(pool) => pool,
            Err(e) => panic!("Error connecting to database: {}", e),
        };
        
        DbConnector { conn: pool }
    }
}