from kafka import KafkaProducer
import os
import json
from utils import validate_schema

KAFKA_BROKER = 'localhost:9092'

def ingest_json(file,  topic, file_type, keys, key_types, broker=KAFKA_BROKER):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if not os.path.exists(file):
        raise FileNotFoundError(f"{file_type} file not found: {file}")

    try:
        with open(file, 'r') as f:
            data = json.load(f)
        
        for record in data:
            if validate_schema(record, keys, key_types):
                producer.send(topic, value=record)
                print(f"Sent to Kafka ({file_type}): {record}")
            else:
                print(f"{record} is incomplete or malformed.")

        print(f"{file_type} data ingested successfully.")
    except Exception as e:
        raise RuntimeError(f"Error reading or ingesting {file_type} data: {e}")