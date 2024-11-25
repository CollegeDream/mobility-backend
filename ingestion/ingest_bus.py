from kafka import KafkaProducer
import json
import os

def ingest_bus_file(file, broker='localhost:9092', topic='bus_location'):
    # Initialize Kafka producer
    producer = KafkaProducer(
        bootstrap_servers=broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    # Check if the file exists
    if not os.path.exists(file):
        print(f"File not found: {file}")
        return

    # Read the JSON file
    print("ingesting bus")
    try:
        with open(file, 'r') as file:
            bus_data = json.load(file)
        
        # Send each record to Kafka
        for record in bus_data:
            producer.send(topic, value=record)
            print(f"Sent to Kafka: {record}")
        
        print("All bus data ingested successfully.")
    except Exception as e:
        print(f"Error reading or ingesting data: {e}")

