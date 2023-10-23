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

    def insertData(self):
        self.drop_User()
        self.drop_Activity()
        self.drop_TrackPoint()

        self.create_User()
        self.create_Activity()
        self.create_TrackPoint()

        activity_id_counter = 0
        trackpoint_id_counter = 0

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
                # Insert the user into User table
                collection = self.db["User"]
                collection.insert_one({"_id": userId, "hasLabels": True})

                # Insert the activitues with labels into Activity table
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
                        values.append(
                            ({"_id": activity_id_counter,"user_id": userId,"transportation_mode": transportation_mode, "start_time": start_time, "end_time": end_time})
                        )
                        activity_id_counter += 1

                    # Insert all labeled activites at once
                    collection = self.db["Activity"]
                    collection.insert_many(values)

                # Retrieve the activity with the exact match if exists
                # If more than one exists, then order by DESC for later use case
                collection = self.db["Activity"]
                activity_id = collection.find({"user_id": userId, "start_time": start_time, "end_time": end_time})

                # Extracts the id from the result
                #activity_id = activity_id[:]["_id"]


                # If there is no match, then this file of trackpoint is skipped
                # Because user who saves transportation mode is not allowed
                # To have transportation mode equals to NULL
                # Thus, we cannot add a new Activity row for it
                #if not activity_id:
                 #   continue

            else:

                print("User", userId)
                collection = self.db["User"]
                collection.insert_one({"_id": userId, "hasLabels": False})


            # Retrieve files in current user's Trajectory folder
            # Each file corresponds to an activity
            trajectory_path = "./dataset/Data/" + userId + "/Trajectory/"
            files = os.listdir(trajectory_path)

            for file in files:
                # Read the file
                with open(trajectory_path + file, "r", encoding="UTF-8") as f:
                    lines = f.readlines()

                # Process the file to retrieve start and end time
                # Start time: Date of the first trackpoint
                # End time: Date of the last trackpoint
                start_time = lines[6].split(",")[5] + " " + lines[6].split(",")[6]
                end_time = lines[-1].split(",")[5] + " " + lines[-1].split(",")[6]

                # If user saves transportation mode, then retrieve the activity in
                # Activity table which has exact matches on starttime and end time
            # If file includes more than 2500 (+6 to count for headers) trackpoints
            if len(lines) > 2500 + 6:
                # If user saves transportation mode, delete the activity that this
                # file belongs to as it will not have any trackpoints, thus is not relevant
                if userId in user_label:

                    for a in activity_id:
                        collection = self.db["Activity"]
                        collection.delete_one({"_id": a["_id"]})

                # Thereafter, skip this file
                continue


            # If the user saves transportation mode, all activities by the user has already been added
            # We choose the most recent added activity by using the ORDER clause above with DESC
            # This is for example if a user starts driving the car, but drives slow in the beginning
            # it might have been perceived as walking in the beginning and added to the Activity list
            # after the user speeds up, a new activity is added with the same endtime and starttime,
            # but with another transportation mode
            if userId in user_label:
                activity_id = activity_id
            else:
                # If user does not save transportation, it means no activity has been added
                # and therefore we add a new activity with transportation_mode = None (NULL)
                activity_id = self.db["Activity"].insert_one({"_id": activity_id_counter,"user_id": userId, "transportation_mode": None, "start_date_time": start_time, "end_date_time": end_time}).inserted_id
                activity_id_counter += 1
            values = []
            # Then process each line of the activity file, but skip the headers (first 6 lines)
            for line in lines[6:]:
                line = line.strip().split(",")
                lat = line[0]
                lon = line[1]
                altitude = line[3]
                date_days = line[4]
                date_time = line[5] + " " + line[6]
                values.append(
                    {"_id": trackpoint_id_counter, "activity_id": activity_id, "lat": lat, "lon": lon, "altitude": altitude, "date_dats": date_days,"date_time": date_time}
                )

                trackpoint_id_counter += 1

            # insert the trackpoints as a batch to be more efficient
            collection = self.db["TrackPoint"]
            ids = collection.insert_many(values)

            # Insert the ids of the activities into the user
            collection = self.db["User"]
            collection.update_one({"_id": userId}, {"$set": {"activities": ids.inserted_ids}})

            # Insert the ids of the trackpoints into the activity
            collection = self.db["Activity"]
            collection.update_one({"_id": activity_id}, {"$set": {"trackpoints": ids.inserted_ids}})




def main():
    assignment3 = Assignement3()
    assignment3.insertData()

if __name__ == '__main__':
    main()




