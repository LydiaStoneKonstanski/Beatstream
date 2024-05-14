#!/bin/bash
/opt/homebrew/opt/kafka/bin/kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 10 --topic listen-events
