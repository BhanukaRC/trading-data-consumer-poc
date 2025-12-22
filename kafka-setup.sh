#!/bin/bash
docker exec trading-data-consumer-poc-kafka /opt/kafka/bin/kafka-topics.sh --create --topic market --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec trading-data-consumer-poc-kafka /opt/kafka/bin/kafka-topics.sh --create --topic trades --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3
docker exec trading-data-consumer-poc-kafka /opt/kafka/bin/kafka-topics.sh --create --topic reconciliation --bootstrap-server localhost:9092 --replication-factor 1 --partitions 3