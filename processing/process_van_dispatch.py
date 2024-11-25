import json
from kafka import KafkaConsumer, KafkaProducer
from utils import validate_time

KAFKA_BROKER = "localhost:9092"

def correlate_van_services(bus_topic, passenger_topic, van_output_topic, broker=KAFKA_BROKER):
    """
    Correlate van services with bus delays (including weather delays) and passenger data.
    """
    consumer_bus = KafkaConsumer(bus_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    consumer_passenger = KafkaConsumer(passenger_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    for passenger_message in consumer_passenger:
        passenger_data = passenger_message.value
        for bus_message in consumer_bus:
            bus_data = bus_message.value

            if validate_time(bus_data) and validate_time(passenger_data):
                # Correlate van requirements
                if bus_data.get('delay', 0) > 5 and passenger_data['waiting_passengers'] > 0.5 * passenger_data['average_waiting']:
                    dispatch_record = {
                        "location": passenger_data['location'],
                        "reason": "High passenger count and delayed bus",
                        "timestamp": passenger_data['timestamp'],
                        "bus_id": bus_data['bus_id'],
                        "delay": bus_data.get('delay', 0),
                        "delay_reason": bus_data.get('delay_reason', "Unknown")
                    }
                    producer.send(van_output_topic, value=dispatch_record)
                    print(f"Published (Van Service): {dispatch_record}")

