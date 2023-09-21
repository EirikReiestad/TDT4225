import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import os

class ExampleProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        '''
        DOCSTRING
        '''
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
                    FOREIGN KEY (activity_id) REFERENCES Activity(id))
                """
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()
    
    def insert_data(self):

        # Insert from labeled_ids.txt the user who has labels
        user_label = []
        with open('./dataset/labeled_ids.txt') as labeled_ids:

            for line in labeled_ids:
                user_label.append(line.strip())
        
        # Loop through all folders
        # Each folder name add as a user

        directory_path = './dataset/Data'
        folders = os.listdir(directory_path)

        for user_id in folders:
            if user_id in user_label:

                # Insert into user

                query = "INSERT INTO User (id, has_labels) VALUES ('%s', TRUE)"
                self.cursor.execute(query % (user_id))

                # Read every line
                with open('./dataset/Data/' + user_id + '/labels.txt') as labels:
                    values = []
                    labels.readline()
                    for line in labels:
                        line = line.strip().split('\t')
                        start_time = line[0].replace('/', '-')
                        end_time = line[1].replace('/', '-')
                        transportation_mode = line[2]
                        values.append((user_id, transportation_mode, start_time, end_time))

                activity_query = "INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) VALUES (%s, %s, %s, %s)"
                self.cursor.executemany(activity_query, values)

                # Insert into TrackPoint
                trajectory_path = './dataset/Data/' + user_id + '/Trajectory/'
                files = os.listdir(trajectory_path)
                
                for file in files:
                    with open(trajectory_path + file, 'r') as f:
                        lines = f.readlines()

                    if len(lines) > 2500 + 6:
                        continue
                    
                    start_time = lines[6].split(',')[5] + ' ' + lines[6].split(',')[6]
                    end_time = lines[-1].split(',')[5] + ' ' + lines[-1].split(',')[6]

                    activity_id_query = "SELECT id FROM Activity WHERE user_id = %s AND start_date_time = %s AND end_date_time = %s"
                    self.cursor.execute(activity_id_query, (user_id, start_time, end_time))
                    activity_id = self.cursor.fetchall()
                    if not activity_id:
                        continue
                    # Fetchone is not used because it may for some reason return multiple ids
                    activity_id = activity_id[0][0]

                    values = []
                    # Skip first 6 lines
                    for line in lines[6:]:
                        line = line.strip().split(',')
                        lat = line[0]
                        lon = line[1]
                        altitude = line[3]
                        date_days = line[4]
                        date_time = line[5] + ' ' + line[6]
                        values.append((activity_id, lat, lon, altitude, date_days, date_time))

                    trackpoint_query = "INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) VALUES (%s, %s, %s, %s, %s, %s)"
                    self.cursor.executemany(trackpoint_query, values)
            else:
                query = "INSERT INTO User (id, has_labels) VALUES ('%s', FALSE)"
                self.cursor.execute(query % (user_id))
 
            self.db_connection.commit()

        # if they are in labeled_ids, has_label=True else False
        # also then import labels.txt
        # loop through all and add to Activity

        # GO into trajectory folder and loop through all files
        # loop through each row of each file add into TrackPoint

        # Column, 1 (lat), 2(lon), 4(altitude), 5(datedays), 6+7(here need to merge into format YYYY-MM-DD HH:MM:SS)

        # If user has label else skip user with no label:
        # Chek if datetime is within an Activity if yes add FK else add NULL
        # MATCH EXACT TIME in start and end
        # check plt files name and last row to check if start and end time equals
        # if equal, then check if plt has less than 2500 if not drop insertin activity

        # BULK INSERT INSTEAD (EXAMPLE BELOW)
        # INSERT INTO employees (id, name, age, department)
        # VALUES
        # (1, 'Peter Parker', 19, 'IT'),
        # (2, 'Bruce Wayne', 29, 'Automobile'),
        # (3, 'Tony Stark', 45, 'Finance'),
        # (4, 'Clark Kent', 50, 'Media');

        # names = ['Bobby', 'Mc', 'McSmack', 'Board']
        # for name in names:
        #     # Take note that the name is wrapped in '' --> '%s' because it is a string,
        #     # while an int would be %s etc
        #     query = "INSERT INTO %s (name) VALUES ('%s')"
        #     self.cursor.execute(query % (table_name, name))
        # self.db_connection.commit()

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
        program.insert_data()
        program.show_tables()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
