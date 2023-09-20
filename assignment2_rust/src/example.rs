use mysql as my;

use db_connectors::DbConnector;

struct ExampleProgram {
    conn: my::Pool,
}

impl ExampleProgram {
    fn new() -> ExampleProgram {
        let conn: my::Pool = DbConnector::new().conn;  
        ExampleProgram { conn: conn }
    }

    fn create_table(&self, table_name: &str) {
        let mut conn = match self.conn.get_conn() {
            Ok(conn) => conn,
            Err(e) => panic!("Error connecting to database: {}", e),
        };

        match conn.query_drop(
            format!(
                "CREATE TABLE IF NOT EXISTS {} (
                    id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(30))",
                table_name
            )
        ) {
            Ok(_) => println!("Table {} created!", table_name),
            Err(e) => println!("Failed to create table {}: {}", table_name, e),
        };
    }

    fn insert_data(&self, table_name: &str) {
        let names: Vec<str> = vec!["Bobby", "Mc", "McSmack", "Board"];
        let mut conn = match self.conn.get_conn() {
            Ok(conn) => conn,
            Err(e) => panic!("Error connecting to database: {}", e),
        };

        for name in names {
            match conn.query_drop(
                format!(
                    "INSERT INTO {} (name) VALUES ('{}')",
                    table_name,
                    name
                )
            ) {
                Ok(_) => println!("Inserted {} into table {}!", name, table_name),
                Err(e) => println!("Failed to insert {} into table {}: {}", name, table_name, e),
            };
        }
    }

    fn fetch_data(&self, table_name: &str) {
        let mut conn = match self.conn.get_conn() {
            Ok(conn) => conn,
            Err(e) => panic!("Error connecting to database: {}", e),
        };

        let result: Vec<(u32, String)> = match conn.query(
            format!(
                "SELECT * FROM {}",
                table_name
            )
        ) {
            Ok(result) => result,
            Err(e) => panic!("Failed to fetch data from table {}: {}", table_name, e),
        };

        println!("Data from table {}, raw format:", table_name);
        println!("{:?}", result);
    }

    fn drop_table(&self, table_name: &str) {
        let mut conn = match self.conn.get_conn() {
            Ok(conn) => conn,
            Err(e) => panic!("Error connecting to database: {}", e),
        };
        
        conn.query_drop(
            format!(
                "DROP TABLE {}",
                table_name
            )
        ).unwrap_or_else(|e| panic!("Failed to drop table {}: {}", table_name, e));
    }

    fn show_tables(&self) {
        let mut conn = match self.conn.get_conn() {
            Ok(conn) => conn,
            Err(e) => panic!("Error connecting to database: {}", e),
        };

        let result: Vec<String> = conn.query("SHOW TABLES").unwrap_or_else(|e| panic!("Failed to show tables: {}", e));
        println!("{:?}", result);
    }
}
