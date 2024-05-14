from io import StringIO
import psycopg2
from concurrent.futures import TimeoutError
from google.cloud import pubsub_v1
from datetime import datetime, timedelta
import os
import json
import pandas as pd

project_id = "scientific-pad-420219"
subscription_id = "project_topic-sub"

DB_name = "postgres"
DB_user = "postgres"
DB_pwd = "165833"

current_date = datetime.now().strftime("%m%d%Y")
current_folder = 'project-topic-sub'

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    json_list.append(json_message)
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=process_message)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=60.0)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()

df = pd.DataFrame(json_list)

# Get the length of the DataFrame
length = len(df)

if length > 0:
    # Check if 'METERS' column contains only positive integers
    if df['METERS'].dtype != 'int64':
        print("METERS column should contain integers")
    elif not (df['METERS'] >= 0).all():
        print("METERS column should be non-negative")
    else:
        print("METERS column are integers and non-negative.")

    print("Ran Assertion #1 successfully")

    # Check for missing values in any column
    if df.isnull().values.any():
        print("There are missing values in the dataset.")
    else:
        print("No missing values found in any column.")

    print("Ran Assertion #2 successfully")

    # Check if GPS_LONGITUDE values are within the range [-180, 180]
    if not df['GPS_LONGITUDE'].between(-180, 180).all():
        print("Some GPS_LONGITUDE values are not within the range [-180, 180].")
    else:
        print("All GPS_LONGITUDE values are within the range [-180, 180].")

    print("Ran Assertion #3 successfully")

    # Check if GPS_LATITUDE values are within the range [-90, 90]
    if not df['GPS_LATITUDE'].between(-90, 90).all():
        print("Some GPS_LATITUDE values are not within the range [-90, 90].")
    else:
        print("All GPS_LATITUDE values are within the range [-90, 90].")

    print("Ran Assertion #4 successfully")

    # Check for duplicate rows
    duplicate_rows = df[df.duplicated()]
    if duplicate_rows.empty:
        print("There are no duplicate rows in the dataset.")
    else:
        print("Duplicate rows found in the dataset:")
        print(duplicate_rows)

    print("Ran Assertion #5 successfully")

    # Check for missing values in GPS_LONGITUDE and GPS_LATITUDE columns
    missing_longitude = df['GPS_LONGITUDE'].isnull().any()
    missing_latitude = df['GPS_LATITUDE'].isnull().any()

    if missing_longitude or missing_latitude:
        print("Null GPS_LATITUDE or GPS_LATITUDE values found.")
    else:
        print("No Null GPS_LONGITUDE or GPS_LATITUDE values found.")

    print("Ran Assertion #6 successfully")

    # Group the DataFrame by VEHICLE_ID and check if both Event_No_Trip and Event_No_Stop exist for each group
    grouped = df.groupby('VEHICLE_ID')
    for vehicle_id, group in grouped:
        has_event_no_trip = 'EVENT_NO_TRIP' in group.columns
        has_event_no_stop = 'EVENT_NO_STOP' in group.columns
        if not (has_event_no_trip and has_event_no_stop):
            print(f"Missing Event_No_Trip or Event_No_Stop data for Vehicle ID {vehicle_id}.")

    print("Ran Assertion #7 successfully")

    # Group the DataFrame by 'VEHICLE_ID' and 'OPD_DATE'
    grouped = df.groupby(['VEHICLE_ID', 'OPD_DATE'])

    # Check if each group has only one unique date
    for (vehicle_id, opd_date), group in grouped:
        if len(group['OPD_DATE'].unique()) != 1:
            print(f"Vehicle ID {vehicle_id} on {opd_date.date()} has data with different dates.")

    print("Ran Assertion #8 successfully")

    grouped = df.groupby('VEHICLE_ID')
    # Check if any row violates the condition
    violation_found = False
    for index, row in df.iterrows():
        if row['GPS_HDOP'] > 10:
            violation_found = True
            break  # Exit the loop once a violation is found

    # Print assertion result based on violation flag
    if violation_found:
        print("Assertion failed: There are rows where GPS_HDOP values are greater than 10.")
    else:
        print("Assertion passed: All rows have GPS_HDOP values less than 10.")

    print("Ran Assertion #9 successfully")

    # Convert OPD_DATE to datetime
    df['NEW_OPD_DATE'] = pd.to_datetime(
        df['OPD_DATE'], format='%d%b%Y:%H:%M:%S')

    # Extract day of the week
    df['DAY_OF_WEEK'] = df['NEW_OPD_DATE'].dt.dayofweek

    # Map day of the week to name
    day_names = {0: 'Weekday', 1: 'Weekday', 2: 'Weekday',
                 3: 'Weekday', 4: 'Weekday', 5: 'Saturday', 6: 'Sunday'}
    df['DAY_NAME'] = df['DAY_OF_WEEK'].map(day_names)

    update_df = df.drop_duplicates(subset=['EVENT_NO_TRIP'], keep='first')

    # Define function to create timestamp
    def create_timestamp(row):
        opd_date = datetime.strptime(row['OPD_DATE'], '%d%b%Y:%H:%M:%S')
        act_time = timedelta(seconds=row['ACT_TIME'])
        timestamp = opd_date + act_time
        return pd.Timestamp(timestamp)

    # Apply the function to create the TIMESTAMP column
    df['TIMESTAMP'] = df.apply(create_timestamp, axis=1)

    df.sort_values(by=['EVENT_NO_TRIP', 'TIMESTAMP',
                       'VEHICLE_ID'], inplace=True)

    df['SPEED'] = df.groupby('EVENT_NO_TRIP')['METERS'].diff(
    ) / df.groupby('EVENT_NO_TRIP')['ACT_TIME'].diff()

    # Backfill to handle the first record of each trip
    df['SPEED'] = df['SPEED'].fillna(method='bfill')

    df['SPEED'] = df['SPEED'].clip(lower=0)  # No negative speeds

    df['GPS_LATITUDE'] = df['GPS_LATITUDE'].fillna(0.0)

    df['GPS_LONGITUDE'] = df['GPS_LONGITUDE'].fillna(0.0)

    # Add dummy columns with default value
    update_df['ROUTE_ID'] = 0
    


    # Select only required columns and rename them
    df_trip = update_df[['EVENT_NO_TRIP', 'ROUTE_ID', 'VEHICLE_ID', 'DAY_NAME']].rename(
        columns={'EVENT_NO_TRIP': 'trip_id', 'ROUTE_ID': 'route_id', 'VEHICLE_ID': 'vehicle_id', 'DAY_NAME': 'service_key'})
    # Select only required columns and rename them
    df_breadcrumb = df[['TIMESTAMP', 'GPS_LATITUDE', 'GPS_LONGITUDE', 'SPEED', 'EVENT_NO_TRIP']].rename(
        columns={'TIMESTAMP': 'tstamp', 'GPS_LATITUDE': 'latitude', 'GPS_LONGITUDE': 'longitude', 'SPEED': 'speed', 'EVENT_NO_TRIP': 'trip_id'})

    # Create a new column for the day of the week
    df['DAY_OF_WEEK'] = df['TIMESTAMP'].dt.dayofweek

    # Map day of the week to 'Weekday' or 'Weekend'
    df['DAY_TYPE'] = df['DAY_OF_WEEK'].map({0: 'Weekend', 1: 'Weekday', 2: 'Weekday', 3: 'Weekday', 4: 'Weekday', 5: 'Weekday', 6: 'Weekend'})

    # Calculate average speed for each day of the week
    avg_speed_per_day = df.groupby(['DAY_TYPE', 'DAY_OF_WEEK'])['SPEED'].mean().reset_index()

    # Display average speed for each day of the week
    print(f"The average spped of the day for summary assertion: {avg_speed_per_day}")
    print("Ran Assertion #10 successfully")

    # Establish a connection to the database
    conn = psycopg2.connect(
        host="localhost",
        database=DB_name,
        user=DB_user,
        password=DB_pwd
    )
 
    def copy_to_trip_table(conn, trip_data):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        cur = conn.cursor()
        buffer = StringIO()
        trip_data_unique = trip_data.drop_duplicates(subset=['trip_id'])
        trip_data[['trip_id', 'vehicle_id']].to_csv(buffer, sep=',', header=False, index=False)
        buffer.seek(0)
        cur.copy_from(buffer, 'trip', sep=',', columns=('trip_id', 'vehicle_id'))
        conn.commit()
        print("Trip data copied successfully!")

    def copy_to_breadcrumb_table(conn, breadcrumb_data):
        """
        Here we are going save the dataframe in memory
        and use copy_from() to copy it to the table
        """
        buffer = StringIO()
        breadcrumb_data.to_csv(buffer, index=False, header=False)
        buffer.seek(0)

        cursor = conn.cursor()
        try:
            cursor.copy_from(buffer, 'breadcrumb', sep=",")
            conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error: %s" % error)
            conn.rollback()
            cursor.close()
            return 1
        print("Loading of breadcrumb table completed")
        cursor.close()

    copy_to_trip_table(conn, df_trip)
    copy_to_breadcrumb_table(conn, df_breadcrumb)

