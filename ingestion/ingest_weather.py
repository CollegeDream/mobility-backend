from kafka import KafkaProducer
import json
import os

def ingest_weather_file(file_path, broker='localhost:9092', topic='weather_update'):
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        with open(file_path, 'r') as file:
            weather_data = json.load(file)
        for record in weather_data:
            producer.send(topic, value=record)
            print(f"Sent to Kafka: {record}")
        print("All weather data ingested successfully.")
    except Exception as e:
        print(f"Error reading or ingesting data: {e}")
