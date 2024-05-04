# inorder for this to work use must nano /opt/homebrew/etc/kafka/server.properties
# and add delete.topic.enable=true to the file
/opt/homebrew/opt/kafka/bin/kafka-topics --bootstrap-server localhost:9092 --delete --topic listen-events