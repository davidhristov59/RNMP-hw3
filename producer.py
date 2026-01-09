# simulates real-time streaming data by reading records from a dataset (json format) and sends it to a Kafka topic one-by-one (row at a time)

import time
from kafka import KafkaProducer
import pandas as pd
import json

df = pd.read_csv('data/offline.csv')

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

for _, row in df.iterrows():
    predict_column = row.drop("Diabetes_012").to_dict() # sends data without the target column
    producer.send('health_data', value=predict_column)
    time.sleep(1)