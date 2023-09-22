"""This is the code for Part2"""
from DbConnector import DbConnector

class DbExecuter:
    """Execute Db instructions"""
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor

    def get_num_users(self) -> int:
        """
        Returns the number of users
        """
        query = "SELECT COUNT(*) FROM "
