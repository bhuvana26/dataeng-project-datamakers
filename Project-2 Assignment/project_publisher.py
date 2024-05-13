import urllib.request
import json
from google.cloud import pubsub_v1

# GCP Configuration
project_id = 'scientific-pad-420219'
topic_id = 'busBreadCrumbData'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

vehicle_ids = [
        3951, 3235, 3010, 3042, 2919, 4048, 3548, 3750, 3056, 4062,
        3228, 3320, 4017, 4020, 3023, 3722, 3012, 4041, 3577, 3252,
        3608, 3241, 3954, 4068, 3749, 3904, 4014, 3950, 4061, 3617,
        3632, 4203, 3963, 3735, 3229, 4228, 3953, 3316, 3623, 3301,
        3234, 3007, 3207, 3403, 3538, 4221, 3014, 3232, 3029, 3933,
        3113, 4050, 3751, 3918, 4021, 3219, 3128, 3732, 3707, 3004,
        4519, 3558, 3047, 2908, 3609, 3201, 3929, 4508, 3906, 3650,
        3570, 3118, 4001, 3145, 3258, 3624, 3621, 3054, 3634, 4045,
        3519, 3757, 3716, 3641, 3739, 3556, 3602, 2904, 3545, 3614,
        3931, 4037, 3329, 3405, 3744, 3955, 4018, 3743, 3223, 3034
]

def fetch_and_publish_data(vehicle_id):
    """Fetch JSON data for a vehicle and publish each record to GCP Pub/Sub."""
    url = f"https://busdata.cs.pdx.edu/api/getBreadCrumbs?vehicle_id={vehicle_id}"
    try:
        with urllib.request.urlopen(url) as response:
            data = json.loads(response.read().decode())
            # Assuming the JSON structure contains a list of records
            for record in data:
                publish_to_pubsub(json.dumps(record),vehicle_id)
            print(f"Processed vehicle ID {vehicle_id}")
    except urllib.error.URLError as e:
        print(f"Failed to fetch data for vehicle ID {vehicle_id}: {e}")

def publish_to_pubsub(message,vehicle_id):
    """Publish a message to the configured GCP Pub/Sub topic."""
    try:
        message_bytes = message.encode('utf-8')
        future = publisher.publish(topic_path, message_bytes)
        #print(f"Message published. ID: {future.result()}")
    except Exception as e:
        print(f"An error occurred while publishing: {e} {vehicle_id}")

def main():
    for vehicle_id in vehicle_ids:
        fetch_and_publish_data(vehicle_id)

if __name__ == "__main__":
    main()

