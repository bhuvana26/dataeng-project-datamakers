import json
import os
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import pubsub_v1

# GCP Configuration
project_id = 'scientific-pad-420219'
subscription_id = 'project_sub' 

# PostgreSQL Configuration
pg_host = 'localhost'
pg_port = '5432'
pg_database = 'postgres'
pg_user = 'postgres'
pg_password = '165833'

# Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Database connection
conn = psycopg2.connect(
    host=pg_host,
    port=pg_port,
    database=pg_database,
    user=pg_user,
    password=pg_password
)

today = datetime.now().strftime('%Y-%m-%d')
# File to store the records
file_path = f"new_pubsub_records_{today}.json"

def callback(message):
    try:
        # Decode the message data from bytes to a string and parse into JSON
        data = json.loads(message.data.decode('utf-8'))
        
        # Extract relevant data fields from the message
        tstamp = data['tstamp']
        latitude = data['latitude']
        longitude = data['longitude']
        speed = data['speed']
        trip_id = data['trip_id']

        # Insert data into the PostgreSQL database
        with conn.cursor() as cursor:
            cursor.execute("""
                INSERT INTO BreadCrumb (tstamp, latitude, longitude, speed, trip_id)
                VALUES (%s, %s, %s, %s, %s);
            """, (tstamp, latitude, longitude, speed, trip_id))

        # Commit the transaction
        conn.commit()

        # Append the data to the file
        with open(file_path, 'a') as file:
            file.write(json.dumps(data) + '\n')

        # Acknowledge the message so it won't be sent again
        message.ack()
    except Exception as e:
        print(f"An error occurred: {e}")

def data_validate1(df):
    """
    Perform initial data validation on raw data.
    """
    print("Performing Assertions on Raw Data... ")
    assertion1 = "The vehicle_id for every bus is a positive number"
    assert (df["VEHICLE_ID"] > 0).all(), f"{assertion1} NOT MET"
    print(f"\t{assertion1} MET")

    assertion2 = "For all records, there exists an EVENT_NO_TRIP which is a positive nine-digit number"
    assert df["EVENT_NO_TRIP"].between(100000000, 999999999).all(), f"{assertion2} NOT MET"
    print(f"\t{assertion2} MET")

    assertion3 = "For all records, there exists an EVENT_NO_STOP which is a positive nine-digit number"
    assert df["EVENT_NO_STOP"].between(100000000, 999999999).all(), f"{assertion3} NOT MET"
    print(f"\t{assertion3} MET")

    assertion4 = "The ACT_TIME column exists and represents seconds elapsed from midnight to the next day"
    assert "ACT_TIME" in df.columns, f"{assertion4} NOT MET"
    print(f"\t{assertion4} MET")

    assertion5 = "OPD_DATE format is either DD-MMM-YY or DDMMMYY:00:00:00"
    assert df["OPD_DATE"].str.match(r'\d{2}-[A-Za-z]{3}-\d{2}|\d{2}[A-Za-z]{3}\d{4}:\d{2}:\d{2}:\d{2}').all(), f"{assertion5} NOT MET"
    print(f"\t{assertion5} MET")


def data_transform(df):
    # Apply transformations
    filtered_df = df.copy()
    filtered_df.rename(columns={"EVENT_NO_TRIP": "trip_id", "OPD_DATE": "tstamp", "GPS_LATITUDE": "latitude", "GPS_LONGITUDE": "longitude"}, inplace=True)
    filtered_df.columns = filtered_df.columns.str.lower()

    filtered_df["tstamp"] = pd.to_datetime(filtered_df["tstamp"], errors='coerce')
    filtered_df["act_time"] = pd.to_numeric(filtered_df["act_time"], errors='coerce')
    filtered_df["tstamp"] = filtered_df["tstamp"] + pd.to_timedelta(filtered_df["act_time"], unit='s')
    filtered_df = filtered_df.sort_values(["trip_id", "tstamp"])
    filtered_df["dmeters"] = filtered_df.groupby(["trip_id"])["meters"].diff()
    filtered_df["dtimestamp"] = filtered_df.groupby(["trip_id"])["tstamp"].diff()
    filtered_df["speed"] = (filtered_df["dmeters"] / filtered_df["dtimestamp"].dt.total_seconds()).round(2)
    
    # Handle anomalies and negative speeds
    filtered_df.loc[filtered_df["speed"] < 0, "speed"] = pd.NA  # Convert negative speeds to NaN
    filtered_df["speed"] = filtered_df.groupby(["trip_id"])["speed"].fillna(method="bfill")  # Fill NaN values with backward fill
    # Perform additional transformations as needed
    return filtered_df

def data_validate2(transformed_df):
    """
    Data Validation of Transformed Trimet Data.
    """
    print("Performing Assertions on Transformed Data...")
    assertion6 = "In all records, the speed value is a non-negative number"
    #Remove records with NaN speeds
    transformed_df = transformed_df.dropna(subset=["speed"])

    if not transformed_df.empty:
        min_speed = transformed_df["speed"].min()
        assert min_speed >= 0, f"{assertion6}: NOT MET - Minimum speed: {min_speed}"
        print(f"\t{assertion6} MET")
    else:
        print(f"\t{assertion6} NOT MET - No valid speed values found")

    assertion7 = "The speed of a TriMet bus should not exceed 100 miles per hour"
    assert (transformed_df["speed"] * 2.23694 <= 100).all(), f"{assertion7}: NOT MET"
    print(f"\t{assertion7} MET")

    assertion8 = "If a latitude coordinate is present for every trip, then a corresponding longitude coordinate should also be present"
    assert transformed_df["latitude"].notnull().equals(transformed_df["longitude"].notnull()), f"{assertion8}: NOT MET"
    print(f"\t{assertion8} MET")

    assertion9 = "gps_satellites exists and is a positive number"
    assert "gps_satellites" in transformed_df.columns and (transformed_df["gps_satellites"] >= 0).all(), f"{assertion9}: NOT MET"
    print(f"\t{assertion9} MET")

    assertion10 = "gps_hdop exists and is a positive number"
    assert "gps_hdop" in transformed_df.columns and (transformed_df["gps_hdop"] >= 0).all(), f"{assertion10}: NOT MET"
    print(f"\t{assertion10} MET")


def load_json_files(directory):
    """
    Load all JSON files in a directory and return their contents as a list of dictionaries.
    """
    data_list = []
    # Get a list of files in the directory
    files = [f for f in os.listdir(directory) if f.endswith('.json')]

    # Load each file
    for file in files:
        file_path = os.path.join(directory, file)
        with open(file_path, 'r') as f:
            data = json.load(f)
            data_list.append(data)

    return data_list

def process_all_json_files(directory):
    """
    Process all JSON files in a specified directory.
    """
    # Load all JSON files
    data_list = load_json_files(directory)

    # Process each JSON data
    for data in data_list:
        # Convert the data to a pandas DataFrame
        df = pd.DataFrame(data)

        # Perform initial data validation
        data_validate1(df)

        # Transform the data
        transformed_df = data_transform(df)

        # Perform post-transformation data validation
        data_validate2(transformed_df)

def main():
    # Define the directory where the JSON files are stored
    json_directory = '/home/jithendrabojedla9999/new_vehicle_data_2024-05-09'

    # Process all JSON files in the directory
    process_all_json_files(json_directory)

    # Open the file in append mode, or create it if it doesn't exist
    if not os.path.exists(file_path):
        with open(file_path, 'w'):
            pass

    # Listen for messages on the subscription
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    print(f"Listening for messages on {subscription_path}...")

    # Keep the main thread alive, or the subscriber will stop listening
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()

if __name__ == "__main__":
    main()

