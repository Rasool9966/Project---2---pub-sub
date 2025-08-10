from google.cloud import pubsub_v1
import avro.schema
import avro.io
import io
import time

PROJECT_ID = "northern-cooler-464505-t9"
SUBSCRIPTION_ID = "e_com_topic-sub"  # Avro subscription
SCHEMA_ID = "e_comm"


subscriber = pubsub_v1.SubscriberClient()
schema_client = pubsub_v1.SchemaServiceClient()


# Get Avro schema from Pub/Sub
schema_path = schema_client.schema_path(PROJECT_ID, SCHEMA_ID)
schema_obj = schema_client.get_schema(request={"name": schema_path})
avro_schema = avro.schema.parse(schema_obj.definition)


def callback(message):
    try:
        bytes_reader = io.BytesIO(message.data)
        decoder = avro.io.BinaryDecoder(bytes_reader)
        reader = avro.io.DatumReader(avro_schema)
        record = reader.read(decoder)

        print("📦 Received Avro order:")
        print(record)
        message.ack()

    except Exception as e:
        print("⚠️ Skipped non-Avro message:", e)
        # Let JSON subscriber handle it — no ack/nack here.

subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
print(f"Listening for Avro messages on {subscription_path}...\n")

# Use context manager so shutdown works reliably
with subscriber:
    future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n🛑 Stopping subscriber...")
        future.cancel()
        future.result()  # Wait until shutdown
