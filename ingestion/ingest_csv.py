from kafka import KafkaProducer
import os
import json
from utils import validate_schema
import pandas as pd

KAFKA_BROKER = "localhost:9092"

def ingest_csv(file, file_type, topic, keys, key_types, broker=KAFKA_BROKER):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if not os.path.exists(file):
        print(f"{file_type} file not found: {file}")
        return

    try:
        # Read the CSV file using pandas
        df = pd.read_csv(file)
        # Get the column names from the first line
        keys = df.columns.tolist()

        for _, row in df.iterrows():
            # Convert the row to a dictionary using the column names as keys
            record = {key: row[key] for key in keys}
            if validate_schema(record, keys, key_types):
                producer.send(topic, value=record)
                print(f"Sent to Kafka: {record}")
            else:
                print(f"{record} is malformed or incomplete.")

        print("All passenger data ingested successfully.")
    except Exception as e:
        print(f"Error reading or ingesting data: {e}")