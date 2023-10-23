import argparse
import time
from datetime import datetime
from insertData import insert_data

import os
from tabulate import tabulate
    

def init_db():
    """
    Initialize the database
    """
    # Format the current time
    FMT = '%H:%M:%S'
    start_datetime = time.strftime(FMT)
    insert_data()
    end_datetime = time.strftime(FMT)
    # Calculate the time difference
    total_datetime = datetime.strptime(end_datetime, FMT) - datetime.strptime(start_datetime, FMT)
    print(f"Started: {start_datetime}\nFinished: {end_datetime}\nTotal: {total_datetime}")

def dataset_is_present() -> bool:
    if os.path.exists("../assignment3/dataset"):
        return True
    else:
        return False


def main(should_init_db=False):

    if should_init_db:
        # Testing if dataset is in the correct folder
        if dataset_is_present():
            init_db()
        else:
            print("Dataset not found. Add 'dataset' to the root of the project folder")
            return

   

if __name__ == "__main__":
    # Enables flag to initialize database
    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--init_database", action="store_true", help="Initialize the database")
    args = parser.parse_args()

    main(args.init_database)