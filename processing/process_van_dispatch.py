import json
from kafka import KafkaConsumer, KafkaProducer
from utils import validate_time, calculate_distance

KAFKA_BROKER = "localhost:9092"

def correlate_van_services(bus_topic, passenger_topic, van_topic, van_output_topic, broker=KAFKA_BROKER):
    """
    Correlate van services with bus delays (including weather delays) and passenger data.
    """
    consumer_bus_delay = KafkaConsumer(bus_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest')
    consumer_passenger = KafkaConsumer(passenger_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest')
    consumer_van = KafkaConsumer(van_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest')
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    van_data = []
    # Cache van data for quick lookup
    for van_message in consumer_van:
        van_data.append(van_message.value)

    for passenger_message in consumer_passenger:
        passenger_data = passenger_message.value

        for bus_message in consumer_bus_delay:
            bus_data = bus_message.value
            
            # Match coordinates within 0.001 degrees 
            if abs(bus_data['lat'] - passenger_data['lat']) <= 0.001 and abs(bus_data['lon'] - passenger_data['lon']) <= 0.001:
                # Correlate van requirements
                if bus_data.get('delay', 0) > 5 and passenger_data['waiting'] > 0.5 * passenger_data['avg_wait']:
                    # Find the closest van
                    closest_van = None
                    min_distance = float('inf')
                    for van in van_data:
                        distance = calculate_distance(passenger_data['lat'], passenger_data['lon'], van['lat'], van['lon'])
                        if distance < min_distance:
                            min_distance = distance
                            closest_van = van
                            
                    dispatch_record = {
                            "location": passenger_data['location'],
                            "reason": "High passenger count and delayed bus",
                            "timestamp": passenger_data['timestamp'],
                            "delay_reason": bus_data.get('delay_reason', "Unknown"),
                            "van_id": closest_van['van_id'],
                        }
                    # Publish van dispatch record
                    producer.send(van_output_topic, value=dispatch_record)
                    print(f"Published (Van Service): {dispatch_record}")
                    break

