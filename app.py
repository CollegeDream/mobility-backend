import sys

from processing.process_van_dispatch import correlate_van_services

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from flask import Flask, jsonify, request, Response
from flask_cors import CORS, cross_origin
from kafka import KafkaProducer, KafkaConsumer
import json
import pandas as pd

# from ingestion.ingest_bus import ingest_bus_file
# from ingestion.ingest_van import ingest_van_file
# from ingestion.ingest_weather import ingest_weather_file
# from ingestion.ingest_passengers import ingest_passenger_data_from_csv

app = Flask(__name__)
CORS(app)

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = Response()
        res.headers['X-Content-Type-Options'] = '*'
        return res

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

from flask import Flask, jsonify, request
from kafka import KafkaProducer
import json
import os
from utils import validate_schema
from processing.process_delay import correlate_bus_weather
from processing.process_van_dispatch import correlate_van_services

app = Flask(__name__)

# Generalized function to ingest data from file to kafka producer
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
    

@app.route('/ingest', methods=['POST'])
@cross_origin()
def ingest_data():
    files = request.json
    if not files:
        return jsonify({"error": "File paths for ingestion are required"}), 400

    responses = {}
    try:
        bus_file = files.get("bus_file")
        bus_keys =  ["bus_id", "lat", "lon", "timestamp"]
        bus_key_types = {"bus_id": str, "lat": float, "lon": float, "timestamp": str}
        if bus_file:
            ingest_json(bus_file, topic='bus_location', file_type="Bus", keys=bus_keys, key_types=bus_key_types)
            responses["bus_file"] = "Ingestion successful"
        else:
            responses["bus_file"] = "No file path provided for {file_type} data"

        van_file = files.get("van_file")
        van_keys =  ["van_id", "lat", "lon", "timestamp"]
        van_key_types = {"van_id": str, "lat": float, "lon": float, "timestamp": str}
        if van_file:
            ingest_json(van_file,  topic='van_location', file_type="Van", keys=van_keys, key_types=van_key_types)
            responses["van_file"] = "Ingestion successful"
        else:
            responses["van_file"] = "No file path provided for {file_type} data"

        weather_file = files.get("weather_file")
        weather_keys =  ["lat", "lon", "temp", "precipitation", "timestamp"]
        weather_key_types = {"lat": float, "lon": float, "temp": int, "precipitation": str, "timestamp": str}
        if weather_file:
            ingest_json(weather_file, topic='weather_update', file_type="Weather", keys=weather_keys, key_types=weather_key_types)
            responses["weather_file"] = "Ingestion successful"
        else:
            responses["weather_file"] = "No file path provided for {file_type} data"

        passenger_file = files.get("passenger_file")
        passenger_keys = ["location", "lat", "lon", "timestamp", "waiting", "avg_wait"]
        passenger_keys_types = {"location": str, "lat": float, "lon": float, "timestamp": str, "waiting": int, "avg_wait": int}
        if passenger_file:
            ingest_csv(passenger_file, file_type="Passenger",  topic='passenger_data', keys=passenger_keys, key_types=passenger_keys_types)
            responses["passenger_file"] = "Ingestion successful"
        else:
            responses["passenger_file"] = "No file path provided"

    except FileNotFoundError as e:
        print(e)
        return jsonify({"error": str(e)}), 404
    except RuntimeError as e:
        print(e)
        return jsonify({"error": str(e)}), 500

    return jsonify(responses), 200

@app.route('/process', methods=['POST'])
@cross_origin()
def process_data():
    try:
        # First correlate bus delays with weather
        correlate_bus_weather("bus_location", "weather_update", "bus_with_delay")

        # Then correlate van requirements using updated bus data
        correlate_van_services("bus_with_delay", "passenger_data", "van_output")

        return jsonify({"message": "Processing completed successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500



@app.route('/dashboard', methods=['GET'])
def dashboard():
    
    return jsonify({
        "delays": [
            {"bus_id": "B001", "delay_reason": "Snow", "delay_time": "5 mins"}
        ],
        "van_requirements": [
            {"location": "Stop A", "reason": "High passenger count"}
        ]
    })

if __name__ == '__main__':
    app.run(debug=False)

