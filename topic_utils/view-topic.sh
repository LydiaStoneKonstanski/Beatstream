#!/bin/bash
/opt/homebrew/opt/kafka/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic user-activity --from-beginning