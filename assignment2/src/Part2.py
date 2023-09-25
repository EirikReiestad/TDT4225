"""Part 2"""

from datetime import datetime
import math
from DbConnector import DbConnector


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
        FROM User u
        INNER JOIN Activity a
        ON u.id = a.user_id
        WHERE transportation_mode = 'bus'
        """
        self.cursor.execute(query)
        users = self.cursor.fetchall()
        return users

    def get_user_activity_over_a_day(
        self,
    ) -> list[(str, str, datetime.datetime)]:
        """
        Get the users who have an activity starting one day, and end the next day

        Return
        ------
        list[(str, str, datetime.datetime)]
            List of user_id, transportation_mode, and duration
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
        return result

    def get_activity_distance(self) -> (int, int):
        """
        Get the user_id for user with the longest activity distance for each transportation mode

        Return
        ------
        (int, int)
            user_id and transportation_mode
        """
        # First, get the longest distance
        # This is porly written and should be revised
        query = """
        SELECT a.id, tp.lat, tp.lon, a.transportation_mode, u.id
        FROM Activity a
        INNER JOIN TrackPoint tp ON a.id = tp.activity_id 
        INNER JOIN User u ON u.id = a.user_id
        WHERE tp.date_time >= a.start_date_time
        AND tp.date_time <= a.end_date_time
        """

        self.cursor.execute(query)
        result = self.cursor.fetchall()

        activity = {}
        last_activity_position = {}
        transportation = {}
        activity_user = {}

        for res in result:
            activity_id = res[0]
            lat = res[1]
            lon = res[2]

            last_lat, last_lon = last_activity_position.get(activity_id, (lat, lon))
            last_activity_position[activity_id] = (lat, lon)
            distance = math.sqrt((lat - last_lat) ** 2 + (lon - last_lon) ** 2)
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
            result.append((user_id, transportation_mode))

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
        # query = "SELECT RANK() OVER (ORDER BY COUNT(*) DESC) AS Ranking, user_id, COUNT(*) as number_of_activities FROM Activity GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 15"
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
        # SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) AS amount FROM Activity GROUP BY user_id, transportation_mode, start_date_time, end_date_time HAVING COUNT(*) > 1;
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Find activities that are registered multiple times:\n", rows)

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
        query = "SELECT user_id, transportation_mode FROM ( SELECT user_id, transportation_mode, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC, transportation_mode ASC) AS rownum FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode) AS activity_grouped WHERE rownum=1 ORDER BY user_id"
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print(
            "Find all the users who have registered transportation_mode and their most used transportation_mode"
        )
        for i in rows:
            print(f"User: {i[0]}, Transportation Mode: {i[1]}")


if __name__ == "__main__":
    pass
