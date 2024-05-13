import json
import pandas as pd
import psycopg2
import io
from datetime import timedelta
from google.cloud import pubsub_v1
import threading
import time

# Database connection details
db_params = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "165833",
    "host": "127.0.0.1",
    "port": "5432"
}

# Google Cloud Pub/Sub setup
project_id = "scientific-pad-420219"
subscription_id = "project_sub"
subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Data buffering
buffered_data = []
last_message_time = None
buffer_lock = threading.Lock()
process_timer = None

def validate_initial_data(df):
    try:
        assertion1 = "Every vehicle ID should be a positive number and exactly 4 digits"
        assert (df["VEHICLE_ID"] > 0).all() and df['VEHICLE_ID'].apply(lambda x: len(str(x)) == 4).all(), assertion1

        assertion2 = "Latitude must be between -90 and 90, and Longitude must be between -180 and 180"
        assert df["GPS_LATITUDE"].between(-90, 90).all() and df["GPS_LONGITUDE"].between(-180, 180).all(), assertion2

        assertion3 = "If a latitude coordinate is present for every trip, then a corresponding longitude coordinate should also be present"
        assert (df["GPS_LATITUDE"].notnull() == df["GPS_LONGITUDE"].notnull()).all(), assertion3

        assertion4 = "EVENT_NO_TRIP should be a positive nine-digit number"
        assert df["EVENT_NO_TRIP"].between(100000000, 999999999).all(), assertion4

        assertion5 = "EVENT_NO_STOP should be a positive nine-digit number"
        assert df["EVENT_NO_STOP"].between(100000000, 999999999).all(), assertion5

        assertion6 = "ACT_TIME should be within valid range (0 to total seconds in a day)"
        total_seconds = (24 * 60 * 60) + (60 * 60 * 4)
        assert df["ACT_TIME"].between(0, total_seconds).all(), assertion6

        assertion7 = "OPD_DATE format is either DD-MMM-YY or DDMMMYY:00:00:00"
        pattern1 = r"\d{2}-[A-Za-z]{3}-\d{2}"
        pattern2 = r"\d{2}[A-Za-z]{3}\d{4}:\d{2}:\d{2}:\d{2}"
        assert (df["OPD_DATE"].str.contains(pattern1).any() or df["OPD_DATE"].str.contains(pattern2).any()), assertion7

        assertion8 = "gps_satellites exists and is a positive number"
        assert "gps_satellites" in df.columns and (df["gps_satellites"] >= 0).all(), assertion8

        assertion9 = "gps_hdop exists and is a positive number"
        assert "gps_hdop" in df.columns and (df["gps_hdop"] >= 0).all(), assertion9

        return True
    except AssertionError as e:
        return False

def validate_transformed_data(df):
    try:
        assertion10 = "All speed values should be Positive"
        assert (df["speed"] >= 0).all(), assertion10
        
        assertion11 = "Speed should not exceed 200 mph"
        speed_mph = df["speed"] * 2.23694
        assert not (speed_mph > 200).any(), assertion11

        return True
    except AssertionError as e:
        return False

def transform_vehicle_data(df):
    df1 = df.copy()
    df1.rename(
        columns={
            "EVENT_NO_TRIP": "trip_id",
            "OPD_DATE": "tstamp",
            "GPS_LONGITUDE": "longitude",
            "GPS_LATITUDE": "latitude",
        },
        inplace=True,
    )
    df1.columns = df1.columns.str.lower()
    df1["tstamp"] = df1["tstamp"].apply(
        lambda value: pd.to_datetime(value, format="%d-%b-%y", errors="coerce")
        if len(value) <= 11
        else pd.to_datetime(value, format="%d%b%Y:%H:%M:%S", errors="coerce")
    )
    df1["act_time"] = pd.to_numeric(df1["act_time"], errors="coerce")
    df1["tstamp"] = df1.apply(
        lambda row: row["tstamp"] + timedelta(seconds=row["act_time"])
        if pd.notnull(row["tstamp"])
        else pd.NaT,
        axis=1
    )
    df1 = df1.sort_values(["trip_id", "tstamp"])
    df1["dmeters"] = df1.groupby(["trip_id"])["meters"].diff()
    df1["dtimestamp"] = df1.groupby(["trip_id"])["tstamp"].diff()
    df1["speed"] = df1.apply(
        lambda row: round(row["dmeters"] / row["dtimestamp"].total_seconds(), 2)
        if pd.notnull(row["dtimestamp"]) and row["dtimestamp"].total_seconds() > 0
        else 0,
        axis=1
    )


    def replace_first_speed(group):
        if group.iloc[0]['speed'] == 0 and len(group) > 1:
            group.iloc[0, group.columns.get_loc('speed')] = group.iloc[1]['speed']
        return group

    df1 = df1.groupby('trip_id').apply(replace_first_speed)

    df1["service_key"] = df1["tstamp"].dt.dayofweek.apply(
        lambda day: "Weekday" if day < 5 else ("Saturday" if day == 5 else "Sunday")
    )
    return df1
    ''' filtered_df = filtered_df.sort_values(["trip_id", "tstamp"])
    filtered_df["dmeters"] = filtered_df.groupby(["trip_id"])["meters"].diff()
    filtered_df["dtimestamp"] = filtered_df.groupby(["trip_id"])["tstamp"].diff()
    filtered_df["speed"] = filtered_df.apply(
        lambda row: round(row["dmeters"] / row["dtimestamp"].total_seconds(), 2)
        if pd.notnull(row["dtimestamp"]) and row["dmeters"] is not None
        else 0,
        axis=1
    )

    # Forward fill speed zeros with the next row's value (but only once)
    for i in filtered_df.index:
        if filtered_df.at[i, 'speed'] == 0:
            next_speed = filtered_df.loc[i+1:i+1, 'speed'] if i+1 in filtered_df.index else 0
            filtered_df.at[i, 'speed'] = next_speed.values[0] if not next_speed.empty else 0

    filtered_df["service_key"] = filtered_df["tstamp"].dt.dayofweek.apply(
        lambda day: "Weekday" if day < 5 else ("Saturday" if day == 5 else "Sunday")
    )
    return filtered_df'''

def upload_data_to_database(df):
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()
    temp_table_name = "temp_trip"
    cur.execute(f"CREATE TEMPORARY TABLE {temp_table_name} (LIKE trip);")
    cur.execute(
        f"ALTER TABLE {temp_table_name} ADD CONSTRAINT temp_trip_pkey PRIMARY KEY (trip_id, route_id, vehicle_id, service_key, direction);"
    )
    output = io.StringIO()
    df[["trip_id", "route_id", "vehicle_id", "service_key", "direction"]].drop_duplicates().to_csv(output, header=False, index=False)
    output.seek(0)
    cur.copy_from(output, temp_table_name, sep=",", null="")
    cur.execute(f"INSERT INTO trip SELECT * FROM {temp_table_name} ON CONFLICT DO NOTHING;")
    output = io.StringIO()
    df[["tstamp", "latitude", "longitude", "speed", "trip_id"]].to_csv(output, header=False, index=False)
    output.seek(0)
    cur.copy_from(output, "breadcrumb", sep=",", null="")
    conn.commit()
    cur.close()
    conn.close()
    print("Loading data to PostgreSQL is complete!")

def process_data():
    global last_message_time
    with buffer_lock:
        if buffered_data:
            df = pd.DataFrame(buffered_data)
            if validate_initial_data(df):
               transformed_df = transform_vehicle_data(df)
               transform_df["direction"] = "Out"
               transform_df["route_id"] = -1
               if validate_transformed_data(transformed_df):
                  upload_data_to_database(transformed_df)
            else:
               print(f"Skipping loading due to failed validation 2 for file: {file_path}")
        else:
            print(f"Skipping transformation and loading due to failed validation 1 for file: {file_path}")

            buffered_data.clear()
    last_message_time = None

def on_timer():
    global last_message_time
    if last_message_time is not None and (time.time() - last_message_time >= 30):
        process_data()

def reset_timer():
    global process_timer, last_message_time
    if process_timer:
        process_timer.cancel()
    if last_message_time is not None:
        process_timer = threading.Timer(30, on_timer)
        process_timer.start()

def callback(message):
    global last_message_time
    data = json.loads(message.data)
    with buffer_lock:
        buffered_data.append(data)
    message.ack()
    last_message_time = time.time()
    reset_timer()

def main():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    with subscriber:
        try:
            last_message_time = time.time()
            reset_timer()
            streaming_pull_future.result()
        except KeyboardInterrupt:
            streaming_pull_future.cancel()
            if process_timer:
                process_timer.cancel()

if __name__ == "__main__":
    main()

