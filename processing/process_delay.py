from kafka import KafkaConsumer, KafkaProducer
import json
from utils import validate_time

KAFKA_BROKER = "localhost:9092"

# Correlate bus delays with weather conditions and publish updated bus data.
def correlate_bus_weather(bus_topic, weather_topic, output_topic, broker=KAFKA_BROKER):

    # Load messages from kafka topics
    consumer_bus = KafkaConsumer(bus_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest')
    consumer_weather = KafkaConsumer(weather_topic, bootstrap_servers=broker, value_deserializer=lambda x: json.loads(x.decode('utf-8')), auto_offset_reset='earliest')
    producer = KafkaProducer(bootstrap_servers=broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    
    for bus_message in consumer_bus:
        bus_data = bus_message.value
        
        for weather_message in consumer_weather:
            weather_data = weather_message.value
            # Validate and correlate weather conditions with bus data

            if bus_data['lat'] == weather_data['lat'] and bus_data['lon'] == weather_data['lon']:
                # Add a delay due to weather
                if weather_data['precipitation'] in ['rain', 'snow']:
                    bus_data['delay'] = bus_data.get('delay', 0) + 5  # Add 5 minutes delay
                else:
                    bus_data['delay'] = 0
                # Publish the updated bus data with delay
                producer.send(output_topic, value=bus_data)
                print(f"Published: {bus_data}")
                break



