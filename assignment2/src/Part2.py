import datetime
from DbConnector import DbConnector
from tabulate import tabulate
import os


class Database:
    def __init__(self):
        self.connection = DbConnector()
        self.db_connection = self.connection.db_connection
        self.cursor = self.connection.cursor


def main():
    db = None
    try:
        db = Database()

        # Task 3: Top 15 users with the highest number of activities
        # query = "SELECT RANK() OVER (ORDER BY COUNT(*) DESC) AS Ranking, user_id, COUNT(*) as number_of_activities FROM Activity GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 15"
        query = "SELECT user_id FROM Activity GROUP BY user_id ORDER BY COUNT(*) DESC LIMIT 15"
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        rows = [i[0] for i in rows]
        print("Top 15 users with the highest number of activities:\n", rows)

        # Task 6: Find activities that are registered multiple times
        # Interpret it as find activities that have same user_id, transportation_mode, start_date_time, end_date_time
        
        # Here find rows which are inserted multiple times
        query = "SELECT * FROM Activity AS a WHERE EXISTS (SELECT b.id FROM Activity AS b WHERE a.user_id = b.user_id AND a.transportation_mode = b.transportation_mode AND a.start_date_time = b.start_date_time AND a.end_date_time = b.end_date_time AND a.id != b.id)"
        # Here find the set of information that are inserted multiple time
        # SELECT user_id, transportation_mode, start_date_time, end_date_time, COUNT(*) AS amount FROM Activity GROUP BY user_id, transportation_mode, start_date_time, end_date_time HAVING COUNT(*) > 1;
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        print("Find activities that are registered multiple times:\n", rows)

        # Task 9: Find the top 15 users who have gained the most altitude meters

        query = "SELECT user_id, sum(activity_altitude)*0.304 as altitude_in_meters FROM Activity JOIN (SELECT activity_id, SUM(difference) AS activity_altitude FROM (SELECT activity_id, altitude - LAG(altitude) OVER (PARTITION BY activity_id ORDER BY date_time) AS difference FROM TrackPoint) AS altitude_difference WHERE difference > 0 GROUP BY activity_id) AS difference_table ON Activity.id = difference_table.activity_id GROUP BY user_id ORDER BY altitude_in_meters DESC LIMIT 15"
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        print("Find the top 15 users who have gained the most altitude meters")
        for i in rows:
            print(f"User: {i[0]}, Altitude in meters: {i[1]}")

        # Task 12: Find all the users who have registered transportation_mode and their most used transportation_mode
        # If transportation_mode has same count then order the transportation mode in alphabetical order
        query = "SELECT user_id, transportation_mode FROM ( SELECT user_id, transportation_mode, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY COUNT(*) DESC, transportation_mode ASC) AS rownum FROM Activity WHERE transportation_mode IS NOT NULL GROUP BY user_id, transportation_mode) AS activity_grouped WHERE rownum=1 ORDER BY user_id"
        db.cursor.execute(query)
        rows = db.cursor.fetchall()
        print("Find all the users who have registered transportation_mode and their most used transportation_mode")
        for i in rows:
            print(f"User: {i[0]}, Transportation Mode: {i[1]}")

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if db:
            db.connection.close_connection()


if __name__ == "__main__":
    main()
