"""Example.py"""

from tabulate import tabulate
from DbConnector import DbConnector


class ExampleProgram:
    """Example program that uses the DbConnector class"""

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_table(self, table_name):
        """Create a table"""
        query = """CREATE TABLE IF NOT EXISTS %s (
                   id INT AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   name VARCHAR(30))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query % table_name)
        self.db_connection.commit()

    def insert_data(self, table_name):
        """Insert data into table"""
        names = ["Bobby", "Mc", "McSmack", "Board"]
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        """Fetch data from table"""
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print(f"Data from table {table_name}, raw format:")
        print(rows)
        # Using tabulate to show the table in a nice way
        print(f"Data from table {table_name}, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        """Drop table"""
        print(f"Dropping table {table_name}...")
        query = "DROP TABLE %s"
        self.cursor.execute(query % table_name)

    def show_tables(self):
        """Show all tables"""
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    """Main function"""
    program = None
    try:
        program = ExampleProgram()
        program.create_table(table_name="Person")
        program.insert_data(table_name="Person")
        _ = program.fetch_data(table_name="Person")
        program.drop_table(table_name="Person")
        # Check that the table is dropped
        program.show_tables()
    except ConnectionError as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
