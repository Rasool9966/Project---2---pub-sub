from datetime import datetime, timezone
import json
import logging
import sys
import time
from faker import Faker
import random
from pythonjsonlogger import jsonlogger
from google.cloud import pubsub_v1

# Create Faker instance
fake = Faker()

# Setting Logging info
logger = logging.getLogger("Publisher-1")
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

# Initialize the Pub/Sub publisher client
publisher = pubsub_v1.PublisherClient()

# Project info
PROJECT_NAME = "northern-cooler-464505-t9"
TOPIC_NAME = "e_com_topic"
TOPIC_PATH = publisher.topic_path(PROJECT_NAME, TOPIC_NAME)

# Creating the mock data and publishing to the topic
def mocking_order():
    return {
        "order_id": f"ORD-{random.randint(1000, 9999)}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "order_date": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_amount": round(random.uniform(20, 500), 2),
        "currency": "USD",
        "status": random.choice(["NEW", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"]),
        "shipping_address": fake.address().replace("\n", ", "),
        "priority": random.choice([True, False])
    }

# Callback mechanism
def callback(future):
    try:
        message_id = future.result()
        logger.info(f"Published message with id: {message_id}")
    except Exception as e:
        logger.error(f"Error publishing message: {e}")

# Endless loop for publishing
while True:
    data = mocking_order()
    json_data = json.dumps(data).encode("utf-8")

    try:
        future = publisher.publish(TOPIC_PATH, data=json_data)
        future.add_done_callback(callback)
    except Exception as e:
        logger.error(f"Exception encountered: {e}")

    time.sleep(2)
