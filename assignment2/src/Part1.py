"""This is the code for Part1"""
import os
from tabulate import tabulate
from DbConnector import DbConnector


class ExampleProgram:
    """Part 1"""

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def create_tables(self):
        """Create tables"""
        # Drop table if already exists
        self.drop_tables()

        # CREATE USER TABLE
        query = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN
        )"""
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

        # CREATE ACTIVITY TABLE

        query = """
        CREATE TABLE IF NOT EXISTS Activity (
            id INTEGER AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            transportation_mode VARCHAR(30),
            start_date_time DATETIME,
            end_date_time DATETIME,
            FOREIGN KEY (user_id) REFERENCES User(id)
        )"""
        # This adds table_name to the %s variable and executes the query
        self.cursor.execute(query)
        self.db_connection.commit()

        query = """
        CREATE TABLE IF NOT EXISTS TrackPoint (
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

    # pylint: disable=R0914, R0915
    def insert_data(self):
        """Insert data into table"""
        # Insert from labeled_ids.txt the user who has labels
        user_label = []
        with open("./dataset/labeled_ids.txt", "r", encoding="UTF-8") as labeled_ids:
            for line in labeled_ids:
                user_label.append(line.strip())

        # Loop through all folders
        # Each folder name add as a user

        directory_path = "./dataset/Data"
        folders = os.listdir(directory_path)

        for user_id in folders:
            if user_id in user_label:
                # Insert into user

                query = """
                INSERT INTO User (id, has_labels) 
                VALUES ('%s', TRUE)
                """
                self.cursor.execute(query % (user_id))

                # Read every line
                with open(
                    "./dataset/Data/" + user_id + "/labels.txt", "r", encoding="UTF-8"
                ) as labels:
                    values = []
                    labels.readline()
                    for line in labels:
                        line = line.strip().split("\t")
                        start_time = line[0].replace("/", "-")
                        end_time = line[1].replace("/", "-")
                        transportation_mode = line[2]
                        values.append(
                            (user_id, transportation_mode, start_time, end_time)
                        )

                activity_query = """
                INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES (%s, %s, %s, %s)
                """
                self.cursor.executemany(activity_query, values)
            else:
                query = """
                INSERT INTO User (id, has_labels) 
                VALUES ('%s', FALSE)
                """
                self.cursor.execute(query % (user_id))

            # Insert into TrackPoint
            trajectory_path = "./dataset/Data/" + user_id + "/Trajectory/"
            files = os.listdir(trajectory_path)

            for file in files:
                with open(trajectory_path + file, "r", encoding="UTF-8") as f:
                    lines = f.readlines()

                if len(lines) > 2500 + 6:
                    continue

                start_time = lines[6].split(",")[5] + " " + lines[6].split(",")[6]
                end_time = lines[-1].split(",")[5] + " " + lines[-1].split(",")[6]

                if user_id in user_label:
                    activity_id_query = """
                    SELECT id FROM Activity 
                    WHERE user_id = %s 
                    AND start_date_time = %s 
                    AND end_date_time = %s 
                    ORDER BY id DESC
                    """
                    self.cursor.execute(
                        activity_id_query, (user_id, start_time, end_time)
                    )
                    activity_id = self.cursor.fetchall()
                    if not activity_id:
                        continue
                        # Fetchone is not used because there exists user who has multiple activity at the same time
                        # We choose to add the trackpoints to the last activity added because for example user
                        # can drive the car slowly that might get mistaken in the beginning for walk
                    activity_id = activity_id[0][0]
                else:
                    activity_id_query = """
                    INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                    VALUES (%s, %s, %s, %s)
                    """
                    self.cursor.execute(
                        activity_id_query, (user_id, None, start_time, end_time)
                    )
                    activity_id = self.cursor.lastrowid

                values = []
                # Skip first 6 lines
                for line in lines[6:]:
                    line = line.strip().split(",")
                    lat = line[0]
                    lon = line[1]
                    altitude = line[3]
                    date_days = line[4]
                    date_time = line[5] + " " + line[6]
                    values.append(
                        (activity_id, lat, lon, altitude, date_days, date_time)
                    )

                trackpoint_query = """
                INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                self.cursor.executemany(trackpoint_query, values)
            self.db_connection.commit()

    def fetch_data(self, table_name):
        """Fetch data"""
        query = "SELECT * FROM %s"
        self.cursor.execute(query % table_name)
        rows = self.cursor.fetchall()
        print("fData from table {table_name}, raw format:")
        print(rows)
        # Using tabulate to show the table in a nice way
        print(f"Data from table {table_name}, tabulated:")
        print(tabulate(rows, headers=self.cursor.column_names))
        return rows

    def drop_table(self, table_name):
        "Drop table"
        print(f"Dropping table {table_name}...")
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def drop_tables(self):
        "Drop tables"
        self.drop_table("TrackPoint")
        self.drop_table("Activity")
        self.drop_table("User")

    def show_tables(self):
        "Show tables"
        self.cursor.execute("SHOW TABLES")
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    """Main function"""
    program = None
    try:
        program = ExampleProgram()
        program.create_tables()
        program.insert_data()
        program.show_tables()
    # pylint: disable=W0718
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
