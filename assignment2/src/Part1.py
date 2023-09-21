from DbConnector import DbConnector
from tabulate import tabulate


class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):

        # Drop table if already exists
        self.drop_tables()

        # CREATE USER TABLE
        query = """CREATE TABLE IF NOT EXISTS User (
                   id VARCHAR(255) NOT NULL PRIMARY KEY,
                   has_labels BOOLEAN)
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

        # CREATE ACTIVITY TABLE

        query = """CREATE TABLE IF NOT EXISTS Activity (
                    id INTEGER AUTO_INCREMENT PRIMARY KEY,
                    user_id VARCHAR(255) NOT NULL,
                    transportation_mode VARCHAR(30),
                    start_date_time DATETIME,
                    end_date_time DATETIME,
                    FOREIGN KEY (user_id) REFERENCES User(id)
                )
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

        query = """CREATE TABLE IF NOT EXISTS TrackPoint (
                   id INTEGER AUTO_INCREMENT NOT NULL PRIMARY KEY,
                   activity_id INTEGER NOT NULL,
                   lat DOUBLE ,
                   lon DOUBLE,
                   altitude INTEGER,
                   date_days DOUBLE,
                   date_time DATETIME,
                   CONSTRAINT FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_data(self, table_name):

        # Insert from labeled_ids.txt the user who has labels
        
        # Loop through all folders
        # Each folder name add as a user

        # if they are in labeled_ids, has_label=True else False
        # also then import labels.txt
        # loop through all and add to Activity

        # GO into trajectory folder and loop through all files
        # loop through each row of each file add into TrackPoint

        # COlumn, 1 (lat), 2(lon), 4(altitude), 5(datedays), 6+7(here need to merge into format YYYY-MM-DD HH:MM:SS)

        # If user has label:
        # Chek if datetime is within an Activity if yes add FK else add NULL
        # MATCH EXACT TIME in start and end

        # BULK INSERT INSTEAD (EXAMPLE BELOW)
        # INSERT INTO employees (id, name, age, department)
        # VALUES
        # (1, 'Peter Parker', 19, 'IT'),
        # (2, 'Bruce Wayne', 29, 'Automobile'),
        # (3, 'Tony Stark', 45, 'Finance'),
        # (4, 'Clark Kent', 50, 'Media');

        names = ['Bobby', 'Mc', 'McSmack', 'Board']
        for name in names:
            # Take note that the name is wrapped in '' --> '%s' because it is a string,
            # while an int would be %s etc
            query = "INSERT INTO %s (name) VALUES ('%s')"
            self.cursor.execute(query % (table_name, name))
        self.db_connection.commit()

    def fetch_data(self, table_name):
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("Data from table %s, raw format:" % table_name)
        print(rows)
        # Using tabulate to show the table in a nice way
        print("Data from table %s, tabulated:" % table_name)
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        print("Dropping table %s..." % table_name)
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def drop_tables(self):
        self.drop_table("TrackPoint")
        self.drop_table("Activity")
        self.drop_table("User")

    def show_tables(self):
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_tables()
        # Check that the table is dropped
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
