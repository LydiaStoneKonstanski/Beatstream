import os
from time import sleep
import logging
import csv
from json import dumps, loads
from kafka import KafkaProducer
from kafka.common import TopicAlreadyExistsError




class Topic:
    def __init__(self, topic=None, port=None, file=None):
        self.topic = topic
        self.port = port
        self.file = file
        self.success = 0
        self.failure = 0

    def on_send_error(self, excp):
        self.failure += 1
        logging.error('failed to send', exc_info=excp)
        print(self.failure)
        #todo send the logs to a database

    def on_send_success(self, metadata):
        self.success += 1
        print(f'Topic: {metadata.topic}')
        print(f'Partition: {metadata.partition}')
        print(f'Offset: {metadata.offset}')
        print(f'messages(rows): {self.success}')
        #todo create logs for successes
    def j_produce(self):
        producer = KafkaProducer(bootstrap_servers=f'localhost:{self.port}',
                                 value_serializer=lambda x: dumps(x).encode('utf-8'),
                                 linger_ms=10)
        with (open(self.file, 'r') as file):
            for line in file:
                j_data = loads(line)
                producer.send(topic=self.topic, value=j_data) \
                    .add_callback(self.on_send_success) \
                    .add_errback(self.on_send_error)
                # add more informative responses to err and call bak functions, (metadata, offset, and so on)
            producer.flush()  # ensures all messages are sent before the close
            producer.close()

    def c_produce(self):
        producer = KafkaProducer(bootstrap_servers=f'localhost:{self.port}',
                                 value_serializer=lambda x: dumps(x).encode('utf-8'),
                                 linger_ms=10)

        with open(self.file, 'r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                c_data = dict(row)
                producer.send(topic=self.topic, value=c_data) \
                    .add_callback(self.on_send_success) \
                    .add_errback(self.on_send_error)

        producer.flush()
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
