import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import numpy as np
import time


class DbExecutor:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def get_table_size(self, table_name: str) -> (int, bool):
        """
        Returns
        number of users: int
        error: bool
        """
        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        result = self.cursor.fetchone()
        if result is None or len(result) == 0:
            return (0, False)
        return (result, True)

    def get_user_taken_bus(self) -> (list[str], bool):
        """
        Return:
        list of user_id: list[str]
        error: bool
        """
        query = """
            SELECT DISTINCT user_id 
            FROM User u
            INNER JOIN Activity a
            ON u.id = a.user_id
            WHERE transportation_mode = 'bus'
            """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        if users is None or len(users) == 0:
            return (0, False)
        return (users, True)

    def get_user_activity_over_a_day(self) -> (list[(str, str, datetime.datetime)], bool):
        """
        Return:
        list of (user_id, transportation_mode, duration)
        error: bool
        """
        query = """
            SELECT user_id, a.start_date_time, a.end_date_time, transportation_mode
            FROM User u
            INNER JOIN Activity a
            ON u.id = a.user_id
        """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        if users is None or len(users) == 0:
            return (0, False)

        result = []
        for user in users:
            start_day = user[1] + datetime.timedelta(days=1)
            end_day = user[2]

            if start_day.day == end_day.day:
                result.append((user[0], user[3], user[2] - user[1]))
        return (result, True)

    def get_activity_distance(self):
        """
        Return:
        """
        query = """
            SELECT a.transportation_mode 
            FROM Activity a
            INNER JOIN TrackPoint tp
            WHERE 
        """

    # 2. Find the average, minimum and maximum number of activities per user.
    def findMinTrackpoints(self) -> list[(str, int, bool)]:
        query = """
            Select user_id, MIN(trackpoints) 
            FROM (SELECT Activity.user_id , count(t.id) AS trackpoints 
            FROM TrackPoint t
            INNER JOIN Activity ON t.activity_id = Activity.id 
            GROUP BY Activity.id) 
            as Trackpoints 
            GROUP BY user_id
            """

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))

    def findAvgTrackpoints(self) -> list[(str, float, bool)]:
        query = """Select user_id, AVG(trackpoints) FROM (SELECT Activity.user_id , count(t.id) AS trackpoints FROM TrackPoint t INNER JOIN Activity ON t.activity_id = Activity.id GROUP BY Activity.id) as Trackpoints GROUP BY user_id"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    def findMaxTrackpoints(self) -> list[(str, int, bool)]:
        query = """Select user_id, MAX(trackpoints) FROM (SELECT Activity.user_id , count(t.id) AS trackpoints FROM TrackPoint t INNER JOIN Activity ON t.activity_id = Activity.id GROUP BY Activity.id) as Trackpoints GROUP BY user_id"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    # 5. Find the top 10 users with most unique transportation modes
    def findTop10TransportationsUsers(self):
        query = """SELECT user_id, COUNT(DISTINCT(transportation_mode)) as DifferentTransportation FROM Activity GROUP BY user_id ORDER BY DifferentTransportation DESC LIMIT 10;"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


    # 8. Find the number of users which have been close to each other in time and space.
    # Close is defined as the same space (50 meters) and for the same half minute (30
    # seconds)
    def findCloseUsers(self):


        # Using the formula to convert from lat/lon to meters
        # https://sciencing.com/convert-distances-degrees-meters-7858322.html
        # L = (2*pi*r*A)/360 Where L is the length, r is the radius of the earth, and A is the angle in degrees.
        # Came up with 0.00045 as the distance in lat/lon that is 50 meters

        # Sorting the trackpoints by date_time to make it fast to find the datepoints within 30 seconds
        query = """SELECT a.user_id, date_time, lat,lon FROM trackpoint t
        INNER JOIN activity a
        ON a.id = t.activity_id
        ORDER BY date_time ASC"""

        # Looping through all the squares
        users = []
        self.cursor.execute(query)
        rows = self.cursor.fetchall()


        for i in range(len(rows)-1):
            for j in range(i,len(rows)):
                # Skip the rest of the loop if the time difference is more than 30 seconds and they are sorted
                if rows[j][1] - rows[i][1] > datetime.timedelta(seconds=30):
                    break
                # Dont do anything if the same user
                if rows[i][0] == rows[j][0]:
                    continue
                # Checking if the trackpoints are within 30 seconds of each other
                if rows[j][1] - rows[i][1] <= datetime.timedelta(seconds=30):

                    # Checking if the trackpoints are within 50 meters of each other
                    if (rows[i][2] - rows[j][2])**2 + (rows[i][3] - rows[j][3])**2 < 0.00045**2:
                        # Adding the users to the list
                        users.append(rows[i][0])
                        users.append(rows[j][0])

        # Removes duplicates
        users = np.unique(users)

        print(f"{len(users)} have been close to each other in time and space")
        len_users = [len(users),]
        print(tabulate(len_users, headers=["Count(Users)"]))



    # 11. Find all users who have invalid activities, and the number of invalid activities per user
    def findInvalidActivities(self):
        query = """SELECT a.user_id, count(*) 
                FROM Activity a 
                JOIN TrackPoint t1 
                ON a.id = t1.activity_id 
                JOIN TrackPoint t2 
                ON t1.id = t2.id-1 
                AND t1.activity_id = t2.activity_id 
                WHERE t2.date_time > t1.date_time + INTERVAL 5 MINUTE
                AND t1.id != t2.id 
                GROUP BY a.user_id 
                ORDER BY a.user_id ASC"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(tabulate(rows, headers=self.cursor.column_names))


def main():
    program = None
    try:
        program = DbExecutor()
        # program.findMinTrackpoints()
        # program.findAvgTrackpoints()
        # program.findMaxTrackpoints()
        #users, _ = program.findInvalidActivities()
        start = time.time()
        program.findCloseUsers()
        end = time.time()
        print("Time taken: ", end - start)

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
