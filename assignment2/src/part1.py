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
        # Drop table if the tables already exists
        self.drop_tables()

        # CREATE USER TABLE
        query = """
        CREATE TABLE IF NOT EXISTS User (
            id VARCHAR(255) NOT NULL PRIMARY KEY,
            has_labels BOOLEAN
        )"""
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
        self.cursor.execute(query)
        self.db_connection.commit()

        # CREATE TrackPoint table
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
        self.cursor.execute(query)
        self.db_connection.commit()

    # pylint: disable=R0914, R0915
    def insert_data(self):
        """Clean and insert data into table"""
        # Read from labeled_ids.txt the user who has labels
        user_label = []
        with open("./dataset/labeled_ids.txt", "r", encoding="UTF-8") as labeled_ids:
            for line in labeled_ids:
                user_label.append(line.strip())

        # Loop through all the folders containing data for each user
        # As each folder correspond to a user, add the user to the User table
        directory_path = "./dataset/Data"
        folders = os.listdir(directory_path)
        for user_id in folders:

            # If user saves transportation mode
            if user_id in user_label:
                # Insert into User table with has_labels=TRUE
                query = """
                INSERT INTO User (id, has_labels) 
                VALUES ('%s', TRUE)
                """
                self.cursor.execute(query % (user_id))

                # Insert the user's labeled activities
                with open(
                    "./dataset/Data/" + user_id + "/labels.txt", "r", encoding="UTF-8"
                ) as labels:
                    values = []
                    labels.readline()
                    for line in labels:
                        # Process each line based on labels.txt format
                        line = line.strip().split("\t")
                        start_time = line[0].replace("/", "-")
                        end_time = line[1].replace("/", "-")
                        transportation_mode = line[2]
                        values.append(
                            (user_id, transportation_mode, start_time, end_time)
                        )

                # Insert all labeled activites at once
                activity_query = """
                INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                VALUES (%s, %s, %s, %s)
                """
                self.cursor.executemany(activity_query, values)
            else:
                # If user does not save transportation mode, set has_labels=FALSE
                query = """
                INSERT INTO User (id, has_labels) 
                VALUES ('%s', FALSE)
                """
                self.cursor.execute(query % (user_id))

            # Retrieve files in current user's Trajectory folder
            # Each file corresponds to an activity
            trajectory_path = "./dataset/Data/" + user_id + "/Trajectory/"
            files = os.listdir(trajectory_path)

            for file in files:

                # Read the file
                with open(trajectory_path + file, "r", encoding="UTF-8") as f:
                    lines = f.readlines()

                # Process the file to retrieve start and end time
                # Start time: Date of the first trackpoint
                # End time: Date of the last trackpoint
                start_time = lines[6].split(",")[5] + " " + lines[6].split(",")[6]
                end_time = lines[-1].split(",")[5] + " " + lines[-1].split(",")[6]

                # If user saves transportation mode, then retrieve the activity in
                # Activity table which has exact matches on starttime and end time
                if user_id in user_label:
                    # Retrieve the activity with the exact match if exists
                    # If more than one exists, then order by DESC for later use case
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

                    # If there is no match, then this file of trackpoint is skipped
                    # Because user who saves transportation mode is not allowed
                    # To have transportation mode equals to NULL
                    # Thus, we cannot add a new Activity row for it
                    if not activity_id:
                        continue

                # If file includes more than 2500 (+6 to count for headers) trackpoints
                if len(lines) > 2500 + 6:
                    # If user saves transportation mode, delete the activity that this
                    # file belongs to as it will not have any trackpoints, thus is not relevant
                    if user_id in user_label:
                        activity_drop_query = """
                        DELETE FROM Activity WHERE id IN (%s)
                        """
                        self.cursor.execute(
                            activity_drop_query, ([i[0] for i in activity_id])
                        )
                    # Thereafter, skip this file
                    continue

                # If the user saves transportation mode, all activities by the user has already been added
                # We choose the most recent added activity by using the ORDER clause above with DESC
                # This is for example if a user starts driving the car, but drives slow in the beginning
                # it might have been perceived as walking in the beginning and added to the Activity list
                # after the user speeds up, a new activity is added with the same endtime and starttime,
                # but with another transportation mode
                if user_id in user_label:
                    activity_id = activity_id[0][0]
                else:
                    # If user does not save transportation, it means no activity has been added
                    # and therefore we add a new activity with transportation_mode = None (NULL)
                    activity_id_query = """
                    INSERT INTO Activity (user_id, transportation_mode, start_date_time, end_date_time) 
                    VALUES (%s, %s, %s, %s)
                    """
                    self.cursor.execute(
                        activity_id_query, (user_id, None, start_time, end_time)
                    )
                    activity_id = self.cursor.lastrowid

                values = []
                # Then process each line of the activity file, but skip the headers (first 6 lines)
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

                # insert the trackpoints as a batch to be more efficient
                trackpoint_query = """
                INSERT INTO TrackPoint (activity_id, lat, lon, altitude, date_days, date_time) 
                VALUES (%s, %s, %s, %s, %s, %s)
                """
                self.cursor.executemany(trackpoint_query, values)
            self.db_connection.commit()
        # Then we clean up the activities which do not have any trackpoints
        # As there could exists trackpoints in the labels.txt for each user that does not have any
        # corresponding file in the user's Trajectory folder
        query = """
        DELETE FROM Activity WHERE NOT EXISTS(SELECT 1 FROM TrackPoint WHERE Activity.id = TrackPoint.activity_id)
        """
        self.cursor.execute(query)
        self.db_connection.commit()

    def fetch_data(self, table_name):
        """Fetch data"""
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
        "Drop table based on a table name"
        print(f"Dropping table {table_name}...")
        query = "DROP TABLE IF EXISTS %s"
        self.cursor.execute(query % table_name)

    def drop_tables(self):
        "Drop tables after the order TrackPoint, Activity, User because of cascading rules"
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
