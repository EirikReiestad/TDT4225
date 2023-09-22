"""This is the code for Part2"""
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
            FROM User 
            INNER JOIN Activity
            ON user_id
            WHERE transportation_mode = 'bus'
            """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is None or len(result) == 0:
            return (0, False)
        return (result, True)

    def get_user_activity_over_a_day(self) -> (list[str], bool):
        """
        Return:
        list of user_id: list[str] 
        error: bool
        """
        query = """
            SELECT DISTINCT user_id
            FROM User
            INNER JOIN Activity a
            ON user_id
        """
        self.cursor.execute(query)
        result = self.cursor.fetchall()
        if result is None or len(result) == 0:
            return (0, False)
        return (result, True)
        
