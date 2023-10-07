import os

from DbConnector import DbConnector

class Assignement3:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_User(self):
        collection = self.db.create_collection("User")
        print('Created collection: ', collection)

    def create_Activity(self):
        collection = self.db.create_collection("Activity")
        print('Created collection: ', collection)

    def create_TrackPoint(self):
        collection = self.db.create_collection("TrackPoint")
        print('Created collection: ', collection)

    def drop_User(self):
        collection = self.db["User"]
        collection.drop()

    def drop_Activity(self):
        collection = self.db["Activity"]
        collection.drop()

    def drop_TrackPoint(self):
        collection = self.db["TrackPoint"]
        collection.drop()

    def insert_User(self, id: str, hasLabels: bool):
        collection = self.db["User"]
        collection.insert_one({"_id": id, "hasLabels": hasLabels})

    def insert_ActivityMany(self, activities: list):
        collection = self.db["Activity"]
        collection.insert_many(activities)

    def insertData(self):
        self.drop_User()
        self.drop_Activity()
        self.drop_TrackPoint()

        self.create_User()
        self.create_Activity()
        self.create_TrackPoint()

        """Clean and insert data into table"""
        # Read from labeled_ids.txt the user who has labels
        user_label = []
        with open("./dataset/labeled_ids.txt", "r", encoding="UTF-8") as labeled_ids:
            for line in labeled_ids:
                user_label.append(line.strip())

        # Loop through all the folders containing data for each user
        # As each folder correspond to a user, add the user to the User table
        directory_path = "./dataset/Data"
        folders = os.listdir(directory_path)

        for userId in folders:
            if userId in user_label:

                # Read from labels.txt and insert into Activity table
                with open(
                    "./dataset/Data/" + userId + "/labels.txt", "r", encoding="UTF-8"
                ) as labels:
                    values = []
                    labels.readline()
                    for line in labels:
                        # Process each line based on labels.txt format
                        line = line.strip().split("\t")
                        start_time = line[0].replace("/", "-")
                        end_time = line[1].replace("/", "-")
                        transportation_mode = line[2]
                        values.append({"user_id": userId, "transportation_mode": transportation_mode, "start_time": start_time, "end_time": end_time})
                    self.insert_ActivityMany(values)

                self.insert_User(userId, True)



            else:
                print(userId)
                self.insert_User(userId, False)

def main():
    assignment3 = Assignement3()
    assignment3.insertData()

if __name__ == '__main__':
    main()




