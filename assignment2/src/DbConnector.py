"""This is the DBConnector file"""
import mysql.connector as mysql


# pylint: disable=R0903
class DbConnector:
    """
    Connects to the MySQL server on the Ubuntu virtual machine.
    Connector needs HOST, DATABASE, USER and PASSWORD to connect,
    while PORT is optional and should be 3306.

    Example:
    HOST = "tdt4225-00.idi.ntnu.no" // Your server IP address/domain name
    DATABASE = "testdb" // Database name, if you just want to connect to MySQL server, leave it empty
    USER = "testuser" // This is the user you created and added privileges for
    PASSWORD = "test123" // The password you set for said user
    """

    # pylint: disable=C0103
    def __init__(
        self,
        HOST="tdt4225-35.idi.ntnu.no",
        DATABASE="assignment2",
        USER="common",
        PASSWORD="common",
        # HOST="localhost",
        # DATABASE="assignment2",
        # USER="root",
        # PASSWORD="banhmi",
    ):
        # Connect to the database
        try:
            self.db_connection = mysql.connect(
                host=HOST, database=DATABASE, user=USER, password=PASSWORD, port=3306
            )
        # pylint: disable=W0703
        except Exception as error:
            print("ERROR: Failed to connect to db:", error)

        # Get the db cursor
        self.cursor = self.db_connection.cursor()

        print("Using user: ", self.db_connection.user)
        print("Connected to:", self.db_connection.get_server_info())
        # get database information
        self.cursor.execute("select database();")
        database_name = self.cursor.fetchone()
        print("You are connected to the database:", database_name)
        print("-----------------------------------------------\n")

    def close_connection(self):
        """
        Close the connection
        """
        # close the cursor
        self.cursor.close()
        # close the DB connection
        self.db_connection.close()
        print("\n-----------------------------------------------")
        print(f"Connection to {self.db_connection.get_server_info()} is closed")
