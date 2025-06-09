#!bin/bash
set -e

echo "Esperando o Kafka ficar disponível em kafka:9092..."

# Espera o Kafka responder
while ! nc -z kafka 9092; do
  sleep 1
done

echo "Kafka está disponível. Iniciando o producer..."
python /app/sensor_producer.py