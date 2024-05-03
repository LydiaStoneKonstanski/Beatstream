from time import sleep
from json import dumps
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                                            value_serialization=lambda m: dumps(m).encode('ascii'))


for i in range(1000):
    data = {'userID' : i, 'songID' : i*2}
    print(data)
    producer.send('user_current_song', value=data)
    sleep(2)