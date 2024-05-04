from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from connections.million_connection import MillionConnection, Track
import random

class CurrentSongProducer:

    def __init__(self):
        self.million_connection = MillionConnection(local=True)
        self.million_session = self.million_connection.session
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda m: dumps(m).encode('ascii'))

    def makeMessages(self):
        for i in range(1000):
            for userID in range(11):
                data = {'userID': userID, 'songID': i * 2}
                self.producer.send('user_current_song', value=data)
                print(data)
            sleep(2)

if __name__ == "__main__":
    c = CurrentSongProducer()
    c.makeMessages()










