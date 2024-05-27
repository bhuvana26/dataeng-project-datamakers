from io import StringIO
import psycopg2
from google.cloud import pubsub_v1
import json
import pandas as pd
import time

# Project and subscription details
project_id = "dataeng-project-420102"
subscription_id = "stop-topic-sub"

# Database credentials
DB_name = "postgres"
DB_user = "postgres"
DB_pwd = "Bhuvana@26"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    json_list.append(json_message)
    message.ack()

streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_message)

print(f"Listening for messages on {subscription_path}..\n")

# Loop to keep listening for messages
while True:
    try:
        with subscriber:
            streaming_pull_future.result(timeout=60.0)
    except TimeoutError:
        if json_list:
            break
        print("No messages received in the last 60 seconds, retrying...")
        # Re-subscribe to continue listening for messages
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=process_message)

# Check if messages were received
if not json_list:
    print("No messages received. Exiting.")
    exit()

df = pd.DataFrame(json_list)

# Data validations
if len(df) > 0:
    # Check for necessary columns
    expected_columns = {'vehicle_num', 'pdx_trip', 'date', 'leave_time', 'train', 'route_number', 'direction', 'service_key', 'trip_number', 'stop_time', 'arrive_time', 'dwell', 'location_id', 'door', 'lift', 'ons', 'offs', 'estimated_load', 'maximum_speed', 'train_mileage', 'pattern_distance', 'location_distance', 'x_coordinate', 'y_coordinate', 'data_source', 'schedule_status'}
    if not expected_columns.issubset(df.columns):
        print("Error: Missing expected columns in the dataset.")
        exit()

    # Check for missing values
    if df.isnull().values.any():
        print("Missing values found in the dataset. Handling missing values...")
        df = df.dropna()

        # Check if there are still missing values
        if df.isnull().values.any():
            print("Error: Missing values found in the dataset after handling them.")
            exit()

    # Check for duplicate rows
    duplicate_rows = df[df.duplicated()]
    if duplicate_rows.empty:
        print("There are no duplicate rows in the dataset.")
    else:
        print("Duplicate rows found in the dataset:")
        print(duplicate_rows)

    print("Ran Assertion successfully")

    # Group the DataFrame by vehicle_num and check if both trip_number and stop_time exist for each group
    grouped = df.groupby('vehicle_num')
    for vehicle_num, group in grouped:
        has_event_no_trip = 'trip_number' in group.columns
        has_event_no_stop = 'stop_time' in group.columns
        if not (has_event_no_trip and has_event_no_stop):
            print(f"Missing trip_number or stop_time data for Vehicle Number {vehicle_num}.")

    print("Ran Assertion successfully")

    # Check the speed of a TriMet bus should not exceed 100 miles per hour
    excessive_speed_rows = df[df['maximum_speed'] > 100]

    if excessive_speed_rows.empty:
        print("All bus speeds are within the allowed limit of 100 miles per hour.")
    else:
        print("Warning: Some buses are exceeding the maximum allowed speed of 100 miles per hour:")
        print(excessive_speed_rows)

    print("Ran Assertion successfully")

# Database connection
conn = psycopg2.connect(
    host="localhost",
    database=DB_name,
    user=DB_user,
    password=DB_pwd
)

# Define function to copy data to stopevent table
def copy_to_stopevent_table(conn, stopevent_data):
    buffer = StringIO()
    stopevent_data.to_csv(buffer, index=False, header=False, sep=",")
    buffer.seek(0)

    cursor = conn.cursor()
    try:
        cursor.copy_from(buffer, 'stopevent', sep=",")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("Loading of stopevent table completed")
    cursor.close()

# Copy data to stopevent table
copy_to_stopevent_table(conn, df)

# Close the database connection
conn.close()
