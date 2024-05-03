from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: dumps(m).encode('ascii'))

producer_2 = KafkaProducer(bootstrap_servers='localhost:9092',
                           value_serializer=lambda x: dumps(x).encode('utf-8'))



for i in range(1000):
    for userID in range(11):
        data = {'userID': userID, 'songID': i*2}
        producer.send('user_current_song', value=data)
        print(data)
    sleep(2)





def produce(file_path):
    with open(file_path, 'r') as file:
        for line in file:
            j_data = loads(line)
            producer_2.send(topic='listen-activity', value=j_data)
            sleep(1)
        producer_2.close()

