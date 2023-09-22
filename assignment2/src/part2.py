"""This is the code for Part2"""
import datetime
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


if "__main__" == __name__:
    connector = DbConnector()
    executor = DbExecutor()


    connector.close_connection()
