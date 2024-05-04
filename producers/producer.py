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
            for userID in range(1,11):
                track_id = self.get_random_song()
                data = {'userID': userID, 'trackID': track_id}
                self.producer.send('user_current_song', value=data)
                print(data)
            sleep(2)

    def get_random_song(self):
        index=random.randint(1,1000000)
        # query on table Track, filtering down to matching index, and because index is unique there
        # should only be one result so we can take the first result of the query.
        track = self.million_session.query(Track).filter(Track.index == index).first()
        return track.track_id



if __name__ == "__main__":
    c = CurrentSongProducer()
    c.makeMessages()










