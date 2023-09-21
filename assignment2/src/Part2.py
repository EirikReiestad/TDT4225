from DbConnector import DbConnector
from tabulate import tabulate

class Part2:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


    # 2. Find the average, minimum and maximum number of activities per user.
    def findMinTrackpoints(self):
        query = """SELECT MIN(trackpoints) FROM (SELECT COUNT(*) AS trackpoints, user_id FROM TrackPoint INNER JOIN activity ON trackpoint.activity_id = activity.id GROUP BY activity.user_id) AS trackpoints"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Minimum number of trackpoins: ")
        print(tabulate(rows, headers=self.cursor.column_names))

    def findAvgTrackpoints(self):

        query = """SELECT AVG(trackpoints) FROM (SELECT COUNT(*) AS trackpoints FROM TrackPoint INNER JOIN Activity ON Trackpoint.activity_id = Activity.id GROUP BY Activity.user_id) AS trackpoints"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Average number of trackpoins: ")
        print(tabulate(rows, headers=self.cursor.column_names))


    def findMaxTrackpoints(self):
        query = """SELECT Max(trackpoints) FROM (SELECT COUNT(*) AS trackpoints, user_id FROM TrackPoint INNER JOIN activity ON trackpoint.activity_id = activity.id GROUP BY activity.user_id) AS trackpoints"""

        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Maximum number of trackpoins: ")
        print(tabulate(rows, headers=self.cursor.column_names))

    # 5. Find the top 10 users with most unique transportation modes
    def findTop10TransportationsUsers(self):
        query = """SELECT user_id, COUNT(DISTINCT(transportation_mode)) as DifferentTransportation FROM test_db.activity GROUP BY user_id ORDER BY DifferentTransportation DESC LIMIT 10;"""
        self.cursor.execute(query)
        rows = self.cursor.fetchall()
        print("Top 10 users with most unique transportation modes: ")
        print(tabulate(rows, headers=self.cursor.column_names))

def main():
    program = None
    try:
        program = Part2()
        #program.findMinTrackpoints()
        #program.findAvgTrackpoints()
        #program.findMaxTrackpoints()
        program.findTop10TransportationsUsers()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == "__main__":
    main()