from kafka import KafkaProducer
from faker import Faker
import json
import time
import random

fake = Faker()
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    api_version=(3, 8, 0),
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
)

def generate_sensor_data():
    return {
        "sensor_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 90.0), 2),
        "location": fake.city()
    }

if __name__ == '__main__':
    TOPIC = "iot-sensor-data"
    while True:
        data = generate_sensor_data()
        producer.send(TOPIC,key=data["sensor_id"], value=data)
        print(f"Sent: {data}")
        time.sleep(1)
