import pandas as pd
from kafka import KafkaProducer
import json

def ingest_passenger_data(file_path, broker='localhost:9092'):
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    data = pd.read_csv(file_path)
    for _, row in data.iterrows():
        producer.send('passenger_data', value=row.to_dict())
