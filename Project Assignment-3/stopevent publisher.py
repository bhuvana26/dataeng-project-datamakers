import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
from google.cloud import pubsub_v1

# GCP Configuration
project_id = 'dataengineering-420402'
topic_id = 'stop-topic'

# Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_id)

vehicle_nums = [
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

def publish_to_pubsub(messages, vehicle_num):
    """Publish messages to the configured GCP Pub/Sub topic."""
    try:
        for message in messages:
            message_bytes = message.encode('utf-8')
            future = publisher.publish(topic_path, message_bytes)
        print(f"Published messages for vehicle {vehicle_num}")
    except Exception as e:
        print(f"An error occurred while publishing: {e}")

def main():
    for vehicle_num in vehicle_nums:
        # Construct the URL for the current vehicle_num
        url = f'https://busdata.cs.pdx.edu/api/getStopEvents?vehicle_num={vehicle_num}'

        # Send a GET request to fetch the raw HTML content
        response = requests.get(url)
        if response.status_code == 200:
            # Parse the HTML content
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find all tables with stop event data
            tables = soup.find_all('table')

            messages = []
            for table in tables:
                # Extract rows
                pdx_trip = table.find_previous_sibling('h2').text.split()[-1]
                for row in table.find_all('tr')[1:]:  # Skip the header row
                    cells = row.find_all('td')
                    row_data = {
                        'pdx_trip': pdx_trip,
                        #'date': datetime.now().strftime("%Y-%m-%d"),
                        'vehicle_num': cells[0].text,  # Assuming the first cell contains the vehicle number
                        'leave_time': cells[1].text,
                        'train': cells[2].text,
                        'route_number': cells[3].text,
                        'direction': cells[4].text,
                        'service_key': cells[5].text,
                        'trip_number': cells[6].text,
                        'stop_time': cells[7].text,
                        'arrive_time': cells[8].text,
                        'dwell': cells[9].text,
                        'location_id': cells[10].text,
                        'door': cells[11].text,
                        'lift': cells[12].text,
                        'ons': cells[13].text,
                        'offs': cells[14].text,
                        'estimated_load': cells[15].text,
                        'maximum_speed': cells[16].text,
                        'train_mileage': cells[17].text,
                        'pattern_distance': cells[18].text,
                        'location_distance': cells[19].text,
                        'x_coordinate': cells[20].text,
                        'y_coordinate': cells[21].text,
                        'data_source': cells[22].text,
                        'schedule_status': cells[23].text
                    }
                    messages.append(json.dumps(row_data))
                    
            publish_to_pubsub(messages, vehicle_num)
        else:
            print(f"Failed to retrieve data for vehicle {vehicle_num}")

if __name__ == "__main__":
    main()
