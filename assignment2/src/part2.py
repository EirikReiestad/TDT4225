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
        return rows

    def findAvgTrackpoints(self) -> list[(str, float, bool)]:
        query = """Select user_id, AVG(trackpoints) FROM (SELECT Activity.user_id , count(t.id) AS trackpoints FROM TrackPoint t INNER JOIN Activity ON t.activity_id = Activity.id GROUP BY Activity.id) as Trackpoints GROUP BY user_id"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    def findMaxTrackpoints(self) -> list[(str, int, bool)]:
        query = """Select user_id, MAX(trackpoints) FROM (SELECT Activity.user_id , count(t.id) AS trackpoints FROM TrackPoint t INNER JOIN Activity ON t.activity_id = Activity.id GROUP BY Activity.id) as Trackpoints GROUP BY user_id"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows

    # 5. Find the top 10 users with most unique transportation modes
    def findTop10TransportationsUsers(self):
        query = """SELECT user_id, COUNT(DISTINCT(transportation_mode)) as DifferentTransportation FROM Activity GROUP BY user_id ORDER BY DifferentTransportation DESC LIMIT 10;"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return (rows, True)

    # 8. Find the number of users which have been close to each other in time and space.
    # Close is defined as the same space (50 meters) and for the same half minute (30
    # seconds)
    def findCloseUsers(self):
        #Finding min,max of lon and lat
        min_max_lat_lon_query = """SELECT min(lat),max(lat),min(lon),max(lat) 
            FROM trackpoint"""

        self.cursor.execute(min_max_lat_lon_query)
        min_max = self.cursor.fetchall()

        min_lat = min_max[0][0] - 0.1
        max_lat = min_max[0][1] + 0.1
        min_lon = min_max[0][2] - 0.1
        max_lon = min_max[0][3] + 0.1


        lat = np.linspace(min_lat,max_lat, 5)
        lon = np.linspace(min_lon,max_lon, 5)



        # Divinding trackpoints into smaller areas
        query = """WITH limitedTrackPoints AS 
            (SELECT * 
            FROM trackpoint 
            WHERE lat > %s 
            AND lat < %s 
            AND lon > %s 
            AND lon < %s)
            
            SELECT DISTINCT *
            FROM Activity a WHERE EXISTS ( 
            SELECT * FROM limitedTrackPoints t1 WHERE EXISTS(
                SELECT * FROM limitedTrackPoints t2 WHERE
                    t2.lon < t1.lon + 0.0040
                    AND t2.lat < t1.lat + 0.0040
                    AND t1.date_time BETWEEN t2.date_time - INTERVAL '30' SECOND AND t2.date_time + INTERVAL '30' SECOND
                    )
            )GROUP BY a.user_id
            """

        start = time.time()
        users = []
        for i in range(len(lat)-1):
            for j in range(len(lon)-1):
                #print(str((lat[i], lat[i + 1], lon[j], lon[j + 1])))
                self.cursor.execute(query, (lat[i], lat[i + 1], lon[j], lon[j + 1]))
                rows = self.cursor.fetchall()

                # Checks if there is content in output
                if len(rows) > 0:
                    # Adds userid to list
                    for row in rows:
                        users.append(row[1])
        # Removes duplicates
        users = np.unique(users)
        end = time.time()

        print(end-start)
        return (users, True)

    # 11. Find all users who have invalid activities, and the number of invalid activities per user
    def findInvalidActivities(self) -> list[(str, int, bool)]:
        query = """SELECT a.user_id, count(*) FROM Activity a JOIN TrackPoint t1 on a.id = t1.activity_id JOIN TrackPoint t2 on t1.id = t2.id-1 AND t1.activity_id = t2.activity_id WHERE t2.date_time > t1.date_time + INTERVAL 5 MINUTE GROUP BY a.user_id"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()

        return (rows, True)


def main():
    program = None
    try:
        program = DbExecutor()
        # program.findMinTrackpoints()
        # program.findAvgTrackpoints()
        # program.findMaxTrackpoints()
        #users, _ = program.findInvalidActivities()
        users, _ = program.findCloseUsers()
        print(users)
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == "__main__":
    main()
