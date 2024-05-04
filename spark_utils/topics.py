import os
from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from kafka.common import TopicAlreadyExistsError


class Topic:
    def __init__(self, topic=None, port=None, file=None):
        self.topic = topic
        self.port = port
        self.file = file

    def j_produce(self):
        producer = KafkaProducer(bootstrap_servers=f'localhost:{self.port}',
                                 value_serializer=lambda x: dumps(x).encode('utf-8'),
                                 linger_ms=10)
        with (open(self.file, 'r') as file):
            for line in file:
                j_data = loads(line)
                producer.send(topic=self.topic, value=j_data)
                # .add_callback('success') \ research these and find out how to use them properly
                # .add_errback()
                #add more informative responses to err and call bak functions, (metadata, offset, and so on)
           # producer.flush()  # ensures all messages are set before the close
            producer.close()

    def create(self, topic, port):
        try:
            os.system(f"/opt/homebrew/opt/kafka/bin/kafka-topics --create --bootstrap-server localhost:{port} --replication-factor 1 --partitions 1 --topic {topic}")
            self.topic = topic
            self.port = port
        except TopicAlreadyExistsError as e:
            print(e)
            self.topic = topic
            self.port = port

    def list(self):
        os.system(f"/opt/homebrew/opt/kafka/bin/kafka-topics --list --bootstrap-server localhost:{self.port}")

    def view(self):
        os.system(f"/opt/homebrew/opt/kafka/bin/kafka-console-consumer --bootstrap-server localhost:{self.port} --topic {self.topic} --from-beginning")

    def switch_topic(self, topic):
        self.topic = topic

    def switch_port(self, port):
        self.port = port
