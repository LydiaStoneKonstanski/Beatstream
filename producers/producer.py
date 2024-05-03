from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
        value_serializer=lambda m: dumps(m).encode('ascii'))

for i in range(1000):
    for userID in range(11):
        data = {'userID' : userID, 'songID' : i*2}
        producer.send('user_current_song', value=data)
        print(data)
    sleep(2)