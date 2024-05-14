from google.cloud import pubsub_v1
import json

project_id = "scientific-pad-420219"
subscription_id = "project-topic"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_id)
json_list = []

def callback(message: pubsub_v1.subscriber.message.Message) -> None:
    json_message = json.loads(message.data.decode('utf-8'))
    json_list.append(json_message)
    message.ack()

streaming_pull_future = subscriber.subscribe(
    subscription_path, callback=callback)

print(f"Listening for messages on {subscription_path}..\n")

with subscriber:
    try:
        streaming_pull_future.result(timeout=60.0)
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
