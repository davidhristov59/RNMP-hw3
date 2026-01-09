from kafka import KafkaConsumer
import json

topic='health_data_predicted'

consumer = KafkaConsumer(
    topic,
    bootstrap_servers = "localhost:9092",
    auto_offset_reset = 'earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print(f"Listening for predictions on topic: {topic}")

for message in consumer:
    record = message.value
    print(f"Received prediction: {record}")