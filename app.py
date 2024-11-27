import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from flask import Flask, jsonify, request, Response
from flask_cors import CORS, cross_origin
from kafka import KafkaProducer, KafkaConsumer
import json
from processing.process_delay import correlate_bus_weather
from processing.process_van_dispatch import correlate_van_services
from ingestion.ingest_csv import ingest_csv
from ingestion.ingest_json import ingest_json

app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}}, supports_credentials=True)

@app.before_request
def handle_preflight():
    if request.method == "OPTIONS":
        res = Response()
        res.headers['X-Content-Type-Options'] = '*'
        return res

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

@app.route('/ingest', methods=['POST'])
@cross_origin()
def ingest_data():
    files = request.json
    if not files:
        return jsonify({"error": "File paths for ingestion are required"}), 400

    # Get files from data using addresses sent from the frontend
    # Keys and key types are specified for validation
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

@app.route('/process', methods=['GET', 'OPTIONS'])
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
@cross_origin()
def dashboard():
    consumer_delays = KafkaConsumer("bus_with_delay", bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    consumer_vans = KafkaConsumer("van_output", bootstrap_servers=KAFKA_BROKER, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    delays = []
    vans = []

    # Collect all bus delay messages
    for delay_msg in consumer_delays:
        delays.append(delay_msg.value)
        print(f"Delay Data: {delay_msg.value}")

    # Collect all van dispatch messages
    for van_msg in consumer_vans:
        vans.append(van_msg.value)
        print(f"Van Data: {van_msg.value}")

    return jsonify({
        "delays": delays,
        "van_requirements": vans
    })

if __name__ == '__main__':
    app.run(debug=False)

