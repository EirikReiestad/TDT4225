"""part 2"""
from datetime import datetime
from DbConnector import DbConnector
from haversine import haversine, Unit
import numpy as np


class Database:
    """Database class"""

    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def get_table_size(self, table_name: str) -> int:
        """
        Get the size of the table

        Parameters
        ----------
        table_name : str
            Name of the table
        """

        query = "SELECT COUNT(*) FROM %s"
        self.cursor.execute(query % table_name)
        result = self.cursor.fetchone()
        return result

    def get_user_taken_bus(self) -> list[str]:
        """
        Get the user who has taken bus

        Return
        ------
        list[str]
            List of user_id
        """

        query = """
        SELECT DISTINCT user_id
        FROM Activity
        WHERE transportation_mode = 'bus'
        """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        columns = self.cursor.column_names
        return users, columns

    def get_num_user_activity_over_a_day(self) -> int:
        """
        Get the number of users who have an activity starting one day, and end the next day

        Return
        ------
        int
            Number of users
        """

        query = """
        SELECT COUNT(DISTINCT user_id) 
        FROM Activity 
        WHERE DATEDIFF(end_date_time, start_date_time) > 0;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchone()
        return result

    def get_user_activity_over_a_day(
        self,
    ) -> list[(str, str, datetime)]:
        """
        Get the users who have an activity starting one day, and end the next day
        Source of inspiration: https://stackoverflow.com/questions/6929328/t-sql-duration-in-hoursminutesseconds
            To get the proper duration format

        Return
        ------
        list[(str, str, datetime)]
            List of user_id, transportation_mode, and duration
        """

        query = """
        SELECT 
            user_id, 
            transportation_mode, 
            SEC_TO_TIME(TIMESTAMPDIFF(SECOND, start_date_time, end_date_time)) as duration
        FROM Activity 
        WHERE DATEDIFF(end_date_time, start_date_time) > 0
        AND transportation_mode IS NOT NULL;
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        columns = self.cursor.column_names
        return result, columns

    # pylint: disable=R0914
    def get_user_with_max_distance(self) -> (int, int, int):
        """
        Get the user_id for user with the longest activity distance for each transportation mode

        Return
        ------
        (int, int, int)
            user_id, transportation_mode, distance
        """
        query = """
        SELECT a.id, tp.lat, tp.lon, a.transportation_mode, a.user_id
        FROM Activity a
        INNER JOIN TrackPoint tp ON a.id = tp.activity_id 
        WHERE (TIMESTAMPDIFF(SECOND, a.start_date_time, a.end_date_time)) < 60 * 60 * 24
        AND a.transportation_mode IS NOT NULL;
        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        activity = {}
        last_activity_position = {}
        transportation = {}
        activity_user = {}

        for res in result:
            activity_id = res[0]
            current_position = (res[1], res[2])
            last_position = last_activity_position.get(activity_id, current_position)
            last_activity_position[activity_id] = current_position
            distance = haversine(current_position, last_position, unit=Unit.KILOMETERS)
            activity[activity_id] = activity.get(activity_id, 0) + distance
            transportation[activity_id] = res[3]
            activity_user[activity_id] = res[4]

        longest_activity = {}
        for act in activity.items():
            transportation_mode = transportation[act[0]]
            longest_activity[transportation_mode] = (
                act[0],
                max(act[1], longest_activity.get(transportation_mode, (0, 0))[1]),
            )

        result = []
        for longest in longest_activity.items():
            activity_id = longest[1][0]
            transportation_mode = longest[0]
            user_id = activity_user[activity_id]
            distance = activity[activity_id]
            result.append((user_id, transportation_mode, distance))

        return result

    def get_user_highest_number_activities(self) -> list[str]:
        """
        Get the user with the highest number of activities

        Return
        ------
        list[str]
            List of user_id
        """
        # Task 3: Top 15 users with the highest number of activities
        # query = "
        # SELECT RANK() OVER (
        #   ORDER BY COUNT(*) DESC) AS Ranking, user_id,
        #   COUNT(*) as number_of_activities
        #   FROM Activity
        #   GROUP BY user_id
        #   ORDER BY COUNT(*)
        #   DESC LIMIT 15"
        query = """
        SELECT user_id FROM Activity 
        GROUP BY user_id 
        ORDER BY COUNT(*) DESC LIMIT 15
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        rows = [i[0] for i in rows]
        print("Top 15 users with the highest number of activities:\n", rows)
        return rows

    def get_multiple_registrated_activities(self) -> list[str]:
        """
        Get the activities that are registered multiple times

        Return
        ------
        list[str]
            List of activity_id
        """
        # Task 6: Find activities that are registered multiple times
        # Interpret it as find activities that have same user_id, transportation_mode, start_date_time, end_date_time

        # Here find rows which are inserted multiple times
        query = """
        SELECT * FROM Activity AS a 
        WHERE EXISTS (
            SELECT b.id FROM Activity AS b 
            WHERE a.user_id = b.user_id 
            AND a.transportation_mode = b.transportation_mode 
            AND a.start_date_time = b.start_date_time 
            AND a.end_date_time = b.end_date_time 
            AND a.id != b.id
        )
        """
        # Here find the set of information that are inserted multiple time
        # SELECT user_id, transportation_mode, start_date_time, end_date_time,
        # COUNT(*) AS amount
        # FROM Activity
        # GROUP BY user_id, transportation_mode, start_date_time, end_date_time
        # HAVING COUNT(*) > 1;
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Find activities that are registered multiple times:\n", rows)

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

        for i in range(len(rows) - 1):
            for j in range(i, len(rows)):
                # Skip the rest of the loop if the time difference is more than 30 seconds and they are sorted
                if rows[j][1] - rows[i][1] > datetime.timedelta(seconds=30):
                    break
                # Dont do anything if the same user
                if rows[i][0] == rows[j][0]:
                    continue
                # Checking if the trackpoints are within 30 seconds of each other
                if rows[j][1] - rows[i][1] <= datetime.timedelta(seconds=30):

                    # Checking if the trackpoints are within 50 meters of each other
                    if (rows[i][2] - rows[j][2]) ** 2 + (rows[i][3] - rows[j][3]) ** 2 < 0.00045 ** 2:
                        # Adding the users to the list
                        users.append(rows[i][0])
                        users.append(rows[j][0])

        # Removes duplicates
        users = np.unique(users)

        print(f"{len(users)} have been close to each other in time and space")
        len_users = [len(users), ]

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

    def get_user_most_altitude(self) -> list[str]:
        """
        Get the user who has gained the most altitude

        Return
        ------
        list[str]
            List of user_id
        """
        # Task 9: Find the top 15 users who have gained the most altitude meters

        query = """
        SELECT user_id, sum(activity_altitude)*0.304 as altitude_in_meters 
        FROM Activity 
        JOIN (SELECT activity_id, SUM(difference) 
        AS activity_altitude 
        FROM (
            SELECT activity_id, altitude - LAG(altitude) 
            OVER (PARTITION 
                BY activity_id 
                ORDER BY date_time) 
            AS difference 
            FROM TrackPoint
        ) AS altitude_difference 
        WHERE difference > 0 
        GROUP BY activity_id) 
        AS difference_table 
        ON Activity.id = difference_table.activity_id 
        GROUP BY user_id ORDER 
        BY altitude_in_meters DESC LIMIT 15
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Find the top 15 users who have gained the most altitude meters")
        for i in rows:
            print(f"User: {i[0]}, Altitude in meters: {i[1]}")

    def get_user_most_used_transportation(self) -> list[str]:
        """
        Get the users who have registered transportation_mode and their most used transportation_mode

        Return
        ------
        list[str]
            List of user_id
        """
        # Task 12: Find all the users who have registered transportation_mode and their most used transportation_mode
        # If transportation_mode has same count then order the transportation mode in alphabetical order
        query = """
        SELECT user_id, transportation_mode 
        FROM ( SELECT user_id, transportation_mode, ROW_NUMBER() 
            OVER (
                PARTITION BY user_id 
                ORDER BY COUNT(*) DESC, transportation_mode ASC) 
            AS rownum FROM Activity 
            WHERE transportation_mode IS NOT NULL 
            GROUP BY user_id, transportation_mode) 
        AS activity_grouped WHERE rownum=1 ORDER BY user_id
        """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(
            "Find all the users who have registered transportation_mode and their most used transportation_mode"
        )
        for i in rows:
            print(f"User: {i[0]}, Transportation Mode: {i[1]}")
    
    # 2. Find the average, minimum and maximum number of activities per user.
    def find_min_trackpoints(self) -> list[(str, int, bool)]:
        '''
        Find minimum trackpoints per user
        '''
        query = """
            SELECT User.id, COALESCE(minimum, 0) AS minimum FROM User LEFT JOIN (SELECT user_id, MIN(trackpoints) AS minimum
            FROM (SELECT Activity.user_id , COUNT(t.id) AS trackpoints 
            FROM TrackPoint t
            INNER JOIN Activity ON t.activity_id = Activity.id 
            GROUP BY Activity.id) 
            AS Trackpoints 
            GROUP BY user_id) a ON a.user_id = User.id
            """
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows, self.cursor.column_names

    def find_avg_trackpoints(self) -> list[(str, float, bool)]:
        '''
        Find average trackpoints per user
        '''
        query = """SELECT User.id, COALESCE(average, 0) AS average 
                FROM User LEFT JOIN (SELECT user_id, AVG(trackpoints) average 
                FROM (SELECT Activity.user_id , COUNT(t.id) AS trackpoints 
                FROM TrackPoint t JOIN Activity ON t.activity_id = Activity.id 
                JOIN User ON Activity.user_id = User.id GROUP BY Activity.id) 
                AS Trackpoints GROUP BY user_id) a ON a.user_id = User.id"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows, self.cursor.column_names

    def find_max_trackpoints(self) -> list[(str, int, bool)]:
        '''
        Find maximum trackpoints per user
        '''
        query = """SELECT User.id, COALESCE(maximum, 0) AS maximum 
                FROM User LEFT JOIN (SELECT user_id, MAX(trackpoints) AS maximum 
                FROM (SELECT Activity.user_id , COUNT(t.id) AS trackpoints FROM TrackPoint t 
                INNER JOIN Activity ON t.activity_id = Activity.id GROUP BY Activity.id) 
                AS Trackpoints GROUP BY user_id) a ON a.user_id = User.id"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows, self.cursor.column_names
    
    def find_top10_transportations_users(self):
        """
        5. Find the top 10 users with most unique transportation modes
        """
        query = """SELECT  RANK() OVER (
        ORDER BY COUNT(DISTINCT(transportation_mode)) DESC
        ) AS Top, user_id, COUNT(DISTINCT(transportation_mode)) as DifferentTransportation 
                    FROM Activity GROUP BY user_id ORDER BY DifferentTransportation DESC LIMIT 10;"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows, self.cursor.column_names

    def find_close_users(self):
        '''
        Finding users who have been close to each other based on
        - time and space
        '''
        #Finding min,max of lon and lat
        min_max_lat_lon_query = """SELECT MIN(lat), MAX(lat), MIN(lon), MAX(lat)
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

        # Build a table
        return users, self.cursor.column_names
    
    def find_invalid_activities(self) -> list[(str, int, bool)]:
        '''
        Find activities that are invalid
        '''
        query = """SELECT a.user_id, count(*) FROM Activity a JOIN TrackPoint t1 on a.id = t1.activity_id 
        JOIN TrackPoint t2 on t1.id = t2.id-1 AND t1.activity_id = t2.activity_id 
        WHERE t2.date_time > t1.date_time + INTERVAL 5 MINUTE GROUP BY a.user_id"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        return rows, self.cursor.column_names


if __name__ == "__main__":
    pass
