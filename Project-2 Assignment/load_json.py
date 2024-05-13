import glob
import os
import pandas as pd
from proj_subscriber import validate_initial_data, transform_vehicle_data, validate_transformed_data, upload_data_to_database

loc = "/home/jithendrabojedla9999/new_vehicle_data_2024-05-11"

for file_path in glob.glob(loc + "*.json"):
    print(f"Loading data from file: {file_path}")
    try:
        df = pd.read_json(file_path, orient='records', lines=True)
        if validate_initial_data(df):
            print("Initial data validation passed.")
            transformed_df = transform_vehicle_data(df)
            transformed_df["route_id"] = -1
            transformed_df["direction"] = "Out"
            if validate_transformed_data(transformed_df):
                print(f"uploading to database {file_path}")
                upload_data_to_database(transformed_df)

    except ValueError as e:
        print(f"Error processing file {file_path}: {e}")
    except Exception as e:
        print(f"An unexpected error occurred with file {file_path}: {e}")

