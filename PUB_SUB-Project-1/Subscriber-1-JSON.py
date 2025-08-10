from google.cloud import pubsub_v1
import json
import logging

subscriber = pubsub_v1.SubscriberClient()

PROJECT_ID = "northern-cooler-464505-t9"
SUBSCRIPTION_NAME = "e_com_topic-sub"
SUBSCRIPTION_PATH = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_NAME)



def pull_message():
    while True:
        response = subscriber.pull(
            request={
                "subscription": SUBSCRIPTION_PATH,
                "max_messages": 10
            }
        )
        ack_ids = []

        for response_msg in response.received_messages:
            json_data_str = response_msg.message.data.decode("utf-8").strip()

            if not json_data_str:
                print("⚠ Empty message received, skipping...")
                ack_ids.append(response_msg.ack_id)
                continue

            try:
                deserialized_data = json.loads(json_data_str)
                print(deserialized_data)
            except json.JSONDecodeError:
                print(f"⚠ Invalid JSON message: {json_data_str}")
                # If you want to skip without crashing:
                ack_ids.append(response_msg.ack_id)
                continue

            ack_ids.append(response_msg.ack_id)

        if ack_ids:
            subscriber.acknowledge(
                request={"subscription": SUBSCRIPTION_PATH, "ack_ids": ack_ids}
            )

if __name__ == "__main__":
    try:
        pull_message()
    except KeyboardInterrupt:
        pass
