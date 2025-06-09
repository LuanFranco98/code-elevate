from kafka import KafkaConsumer
import json
# import psycopg2

# Kafka Consumer
consumer = KafkaConsumer(
    'iot-sensor-data',
    api_version=(3, 8, 0), # passei uma fome por cauas dessa versao aqui
    bootstrap_servers='kafka:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='iot-group'
)

# # PostgreSQL connection
# conn = psycopg2.connect(
#     dbname='iotdb',
#     user='iotuser',
#     password='iotpass',
#     host='localhost',
#     port='5432'
# )
# cursor = conn.cursor()

# # Criar tabela (apenas uma vez)
# cursor.execute("""
# CREATE TABLE IF NOT EXISTS sensor_data (
#     sensor_id TEXT,
#     timestamp TIMESTAMP,
#     temperature FLOAT,
#     humidity FLOAT,
#     location TEXT
# )
# """)
# conn.commit()


if __name__ == '__main__':
    for msg in consumer:
        data = msg.value
        # cursor.execute("""
        # INSERT INTO sensor_data (sensor_id, timestamp, temperature, humidity, location)
        # VALUES (%s, %s, %s, %s, %s)
        # """, (data['sensor_id'], data['timestamp'], data['temperature'], data['humidity'], data['location']))
        # conn.commit()
        print(f"Stored: {data}")
