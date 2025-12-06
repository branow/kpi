#!/bin/bash

echo "Creating topic: npp-raw-telemetry..."
docker exec -it kafka /usr/bin/kafka-topics \
  --create \
  --if-not-exists \
  --topic npp-raw-telemetry \
  --bootstrap-server localhost:9092 \
  --partitions 4 \
  --replication-factor 1


echo "Current Kafka Topics:"
docker exec -it kafka /usr/bin/kafka-topics --list --bootstrap-server localhost:9092
