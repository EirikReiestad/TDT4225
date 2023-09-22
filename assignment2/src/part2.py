"""This is the code for Part2"""
import datetime
import math
from DbConnector import DbConnector

class DbExecutor:
    """Execute Db instructions"""
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
                result.append((user[0], user[3], user[2]-user[1]))
        return (result, True)
    
    def get_activity_distance(self) -> (int, int):
        """
        Return:
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
            distance = math.sqrt((lat - last_lat)**2 + (lon - last_lon)**2)
            activity[activity_id] = activity.get(activity_id, 0) + distance
            transportation[activity_id] = res[3]
            activity_user[activity_id] = res[4]

        longest_activity = {}
        for act in activity.items():
            transportation_mode = transportation[act[0]]
            longest_activity[transportation_mode] = (
                act[0],
                max(act[1], longest_activity.get(transportation_mode, (0, 0))[1]))

        result = []
        for longest in longest_activity.items():
            activity_id = longest[1][0]
            transportation_mode = longest[0]
            user_id = activity_user[activity_id] 
            result.append((user_id, transportation_mode))

        return result


if "__main__" == __name__:
    connector = DbConnector()
    executor = DbExecutor()

    res = executor.get_activity_distance()
    print(res)

    connector.close_connection()
