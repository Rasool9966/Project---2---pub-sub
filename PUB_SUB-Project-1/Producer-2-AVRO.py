from google.cloud import pubsub_v1
import avro.schema
import avro.io
import io
import random
import time
import uuid
from datetime import datetime

# Config
PROJECT_ID = "northern-cooler-464505-t9"
TOPIC_ID = "e_com_topic"
SCHEMA_ID = "e_comm"

# Pub/Sub clients
publisher = pubsub_v1.PublisherClient()
schema_client = pubsub_v1.SchemaServiceClient()

# Get schema from Pub/Sub
schema_path = schema_client.schema_path(PROJECT_ID, SCHEMA_ID)
schema_obj = schema_client.get_schema(request={"name": schema_path})

print("Fetched schema type:", schema_obj.type_)
print("Fetched schema definition:\n", schema_obj.definition)

# Parse Avro schema
avro_schema = avro.schema.parse(schema_obj.definition)

# Mock data generators
payment_methods = ["Credit Card", "PayPal", "Bank Transfer", "Cash on Delivery"]
shipping_methods = ["Standard", "Express", "Overnight"]
currencies = ["USD", "EUR", "GBP", "AUD"]
statuses = ["Pending", "Processing", "Shipped", "Delivered", "Cancelled"]

def generate_order_event():
    items = []
    subtotal = 0
    for _ in range(random.randint(1, 3)):
        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(5, 50), 2)
        total_price = round(quantity * unit_price, 2)
        items.append({
            "product_name": f"Product_{random.randint(1, 100)}",
            "quantity": quantity,
            "unit_price": unit_price,
            "total_price": total_price
        })
        subtotal += total_price

    discount = round(random.uniform(0, 5), 2)
    tax = round(subtotal * 0.1, 2)
    total_amount = round(subtotal - discount + tax, 2)

    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": str(uuid.uuid4()),
        "order_date": datetime.utcnow().isoformat(),
        "currency": random.choice(currencies),
        "status": random.choice(statuses),
        "payment_method": random.choice(payment_methods),  # never None
        "shipping_method": random.choice(shipping_methods),
        "shipping_address": f"{random.randint(100,999)} Main Street",
        "priority": random.choice([True, False]),
        "items": items,
        "subtotal": subtotal,
        "discount": discount,
        "tax": tax,
        "total_amount": total_amount,
        "discount_code": random.choice([None, "DISC10", "FREESHIP"])
    }

# Continuous publish
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

while True:
    order_event = generate_order_event()

    # Serialize to Avro
    writer = avro.io.DatumWriter(avro_schema)
    bytes_writer = io.BytesIO()
    encoder = avro.io.BinaryEncoder(bytes_writer)
    writer.write(order_event, encoder)
    data = bytes_writer.getvalue()

    # Publish
    future = publisher.publish(topic_path, data)
    print(f"Published order: {order_event['order_id']}")
    
    time.sleep(1)  # send 1 event/sec
